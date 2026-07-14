package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/securetransport"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

const (
	userKey          = "authenticated_user_id"
	registrationTTL  = 90 * time.Second
	registrationName = protocol.HeaderDeviceRegistration
)

type assetManifest struct {
	Assets map[string]struct {
		Integrity string `json:"integrity"`
	} `json:"assets"`
}

type registrationGrant struct {
	principal string
	sessionID string
	expiresAt time.Time
}

type grantStore struct {
	mu     sync.Mutex
	grants map[[32]byte]registrationGrant
}

func newGrantStore() *grantStore {
	return &grantStore{grants: make(map[[32]byte]registrationGrant)}
}

func (s *grantStore) issue(principal, sessionID string) (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	now := time.Now()
	s.mu.Lock()
	for key, grant := range s.grants {
		if !grant.expiresAt.After(now) {
			delete(s.grants, key)
		}
	}
	if len(s.grants) >= 10_000 {
		s.mu.Unlock()
		return "", fmt.Errorf("registration grant capacity exhausted")
	}
	s.grants[sha256.Sum256(raw[:])] = registrationGrant{
		principal: principal,
		sessionID: sessionID,
		expiresAt: now.Add(registrationTTL),
	}
	s.mu.Unlock()
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

// consume is deliberately one-shot. Even a failed binding check burns the
// presented token, preventing replay and online token probing.
func (s *grantStore) consume(raw, principal, sessionID string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) != 32 {
		return false
	}
	key := sha256.Sum256(decoded)
	for i := range decoded {
		decoded[i] = 0
	}
	s.mu.Lock()
	grant, ok := s.grants[key]
	delete(s.grants, key)
	s.mu.Unlock()
	return ok && grant.expiresAt.After(time.Now()) && grant.principal == principal && grant.sessionID == sessionID
}

func main() {
	generateKey := flag.Bool("generate-key", false, "print a base64url X25519 server private key and exit")
	flag.Parse()
	if *generateKey {
		key, err := securetransport.GenerateServerPrivateKey()
		if err != nil {
			log.Fatal(err)
		}
		encoded, err := securetransport.EncodeServerPrivateKey(key)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(encoded)
		return
	}

	addr := env("FH_EXAMPLE_ADDR", "127.0.0.1:8080")
	origin := strings.TrimRight(env("FH_EXAMPLE_ORIGIN", "http://"+addr), "/")
	originURL, err := url.Parse(origin)
	if err != nil || originURL.Scheme == "" || originURL.Host == "" || originURL.Path != "" {
		log.Fatal("FH_EXAMPLE_ORIGIN must be an origin such as https://app.example.com")
	}
	productionTLS := originURL.Scheme == "https"

	manifest, err := loadManifest("wasm/dist/asset-manifest.json")
	if err != nil {
		log.Fatalf("load WASM manifest (run `make wasm` first): %v", err)
	}

	serverKey, ephemeral, err := loadServerKey(productionTLS)
	if err != nil {
		log.Fatal(err)
	}
	sessionSecret, err := loadSessionSecret(productionTLS)
	if err != nil {
		log.Fatal(err)
	}

	webSessions := session.NewSessionManager(
		session.NewMemoryStore(time.Minute),
		session.SessionCookieName(cookieName(productionTLS)),
		session.SessionSecret(sessionSecret),
		session.SessionMaxAge(30*time.Minute),
		session.SessionHTTPOnly(true),
		session.SessionSecure(productionTLS),
		session.SessionSameSite(fh.SameSiteStrict),
		session.SessionPath("/"),
	)
	grants := newGrantStore()
	app := fh.NewProduction()
	app.Use(security.New(security.Config{
		ContentSecurityPolicy:     "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'; style-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
		HSTSMaxAge:                hstsAge(productionTLS),
		HSTSIncludeSubDomains:     productionTLS,
		FrameDeny:                 true,
		ContentTypeNosniff:        true,
		XSSProtection:             "0",
		CrossOriginOpenerPolicy:   "same-origin",
		CrossOriginResourcePolicy: "same-origin",
		CrossOriginEmbedderPolicy: "require-corp",
		ReferrerPolicy:            "no-referrer",
		PermissionsPolicy:         "geolocation=(), microphone=(), camera=(), payment=(), usb=()",
	}))
	// Register immutable assets before session/transport middleware so fetching
	// JS/WASM never creates or refreshes an authentication cookie.
	app.Static("/wasm", "wasm/dist", fh.StaticConfig{MaxAge: 31536000, CacheDuration: time.Minute})
	app.Static("/", "examples/secure_wasm/public", fh.StaticConfig{CacheDuration: time.Second})
	app.Use(session.New(webSessions))

	transport, err := securetransport.Install(app, securetransport.Config{
		ServerPrivateKey:        serverKey,
		AllowEphemeralServerKey: ephemeral,
		KeyID:                   "secure-wasm-example-v1",
		RequireSecure:           true,
		Protect: func(c fh.Ctx) bool {
			return strings.HasPrefix(c.Path(), "/api/")
		},
		AllowedOrigins: []string{origin},
		RequireOrigin:  true,
		AuthorizeDeviceRegistration: func(c fh.Ctx, _ protocol.DeviceRegistrationRequest) (string, error) {
			web := session.Get(c)
			principal, _ := web.Get(userKey).(string)
			if principal == "" || !grants.consume(c.Get(registrationName), principal, web.ID) {
				return "", fh.NewHTTPError(fh.StatusForbidden, "DEVICE_REGISTRATION_FORBIDDEN", "device registration grant is missing, expired, or already used")
			}
			return principal, nil
		},
		ValidateSession: func(c fh.Ctx, secure securetransport.SessionInfo) error {
			principal, _ := session.Get(c).Get(userKey).(string)
			if principal == "" || subtle.ConstantTimeCompare([]byte(principal), []byte(secure.Principal)) != 1 {
				return fh.NewHTTPError(fh.StatusUnauthorized, "SESSION_BINDING_FAILED", "web and secure transport sessions do not match")
			}
			return nil
		},
		OnSecurityEvent: func(event securetransport.SecurityEvent) {
			log.Printf("security_event type=%s device=%s session=%s request=%s ip=%s detail=%q", event.Type, event.DeviceID, event.SessionID, event.RequestID, event.IP, event.Detail)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	app.Post("/auth/login", func(c fh.Ctx) error {
		if !sameOriginRequest(c, origin) {
			return fh.NewHTTPError(fh.StatusForbidden, "ORIGIN_REJECTED", "same-origin login required")
		}
		var input struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.Unmarshal(c.BodyCopy(), &input); err != nil {
			return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_LOGIN", "invalid login request")
		}
		if !secretEqual(input.Username, env("FH_EXAMPLE_USER", "demo")) || !secretEqual(input.Password, env("FH_EXAMPLE_PASSWORD", "demo")) {
			return fh.NewHTTPError(fh.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid credentials")
		}
		web := session.Get(c)
		web.Set(userKey, "user-123")
		if err := webSessions.Regenerate(c, web); err != nil {
			return err
		}
		return c.JSON(fh.Map{"authenticated": true})
	})

	app.Post("/auth/logout", func(c fh.Ctx) error {
		if !sameOriginRequest(c, origin) {
			return fh.NewHTTPError(fh.StatusForbidden, "ORIGIN_REJECTED", "same-origin logout required")
		}
		return webSessions.Destroy(c, session.Get(c))
	})

	app.Get("/secure-config.json", func(c fh.Ctx) error {
		web := session.Get(c)
		principal, _ := web.Get(userKey).(string)
		if principal == "" {
			return fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login required")
		}
		grant, err := grants.issue(principal, web.ID)
		if err != nil {
			return fh.NewHTTPError(fh.StatusServiceUnavailable, "GRANT_UNAVAILABLE", "registration grant unavailable")
		}
		c.Set("Cache-Control", "no-store")
		return c.JSON(fh.Map{
			"baseURL":               origin,
			"pinnedServerKey":       transport.PublicKeyBase64(),
			"pinnedServerKeyID":     transport.KeyID(),
			"registrationToken":     grant,
			"wasmURL":               "/wasm/securefetch.wasm",
			"wasmExecURL":           "/wasm/wasm_exec.js",
			"wasmIntegrity":         manifest.Assets["securefetch.wasm"].Integrity,
			"wasmExecIntegrity":     manifest.Assets["wasm_exec.js"].Integrity,
			"requireAssetIntegrity": true,
		})
	})

	app.Get("/api/me", func(c fh.Ctx) error {
		secure, _ := securetransport.SessionFromContext(c)
		return c.JSON(fh.Map{"userID": secure.Principal, "deviceID": protocol.EncodeID(secure.DeviceID)})
	})
	app.Post("/api/transfer", func(c fh.Ctx) error {
		var input struct {
			To     string `json:"to"`
			Amount int64  `json:"amount"`
		}
		if err := json.Unmarshal(c.BodyCopy(), &input); err != nil || input.To == "" || input.Amount < 1 || input.Amount > 10_000 {
			return fh.NewHTTPError(fh.StatusUnprocessableEntity, "INVALID_TRANSFER", "to and amount (1..10000) are required")
		}
		secure, _ := securetransport.SessionFromContext(c)
		requestID, _ := securetransport.RequestIDFromContext(c)
		// A real service performs authorization, balance checks, idempotency, and
		// persistence here. It never accepts a client-reported success state.
		return c.Status(fh.StatusCreated).JSON(fh.Map{
			"accepted":  true,
			"principal": secure.Principal,
			"to":        input.To,
			"amount":    input.Amount,
			"requestID": protocol.EncodeID(requestID),
		})
	})

	log.Printf("secure WASM example listening on %s (origin %s)", addr, origin)
	log.Fatal(app.Listen(addr))
}

func loadManifest(path string) (assetManifest, error) {
	var out assetManifest
	raw, err := os.ReadFile(path)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	if out.Assets["securefetch.wasm"].Integrity == "" || out.Assets["wasm_exec.js"].Integrity == "" {
		return out, fmt.Errorf("manifest is missing required integrity values")
	}
	return out, nil
}

func loadServerKey(production bool) ([]byte, bool, error) {
	value := strings.TrimSpace(os.Getenv("FH_SECURE_SERVER_KEY"))
	if value == "" {
		if production {
			return nil, false, fmt.Errorf("FH_SECURE_SERVER_KEY is required for a production HTTPS origin")
		}
		log.Print("WARNING: using an ephemeral secure-transport key for loopback development")
		return nil, true, nil
	}
	key, err := securetransport.DecodeServerPrivateKey(value)
	return key, false, err
}

func loadSessionSecret(production bool) ([]byte, error) {
	value := strings.TrimSpace(os.Getenv("FH_EXAMPLE_SESSION_SECRET"))
	if value != "" {
		secret, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil || len(secret) < 32 {
			return nil, fmt.Errorf("FH_EXAMPLE_SESSION_SECRET must be at least 32 random base64url bytes")
		}
		return secret, nil
	}
	if production {
		return nil, fmt.Errorf("FH_EXAMPLE_SESSION_SECRET is required for a production HTTPS origin")
	}
	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	return secret, err
}

func secretEqual(actual, expected string) bool {
	a := sha256.Sum256([]byte(actual))
	b := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

func sameOriginRequest(c fh.Ctx, expected string) bool {
	return strings.TrimRight(c.Get(fh.HeaderOriginStr), "/") == expected && strings.ToLower(c.Get("Sec-Fetch-Site")) != "cross-site"
}

func hstsAge(enabled bool) int {
	if enabled {
		return 31536000
	}
	return 0
}

func cookieName(secure bool) string {
	if secure {
		return "__Host-fh_example"
	}
	return "fh_example"
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
