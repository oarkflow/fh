package main

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/oarkflow/fh"
	secure "github.com/oarkflow/fh/mw/securetransport"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

const (
	sessionUserIDKey   = "user_id"
	sessionUserNameKey = "user_name"
	sessionUserRoleKey = "user_role"
	sessionLoginKey    = "logged_in_at"
	exampleSessionName = "fh_auth"

	demoUsername = "demo"
	demoPassword = "demo"
	demoUserID   = "demo-user"
	demoUserName = "Demo User"
	demoUserRole = "admin"
)

type appUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Role string `json:"role"`
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type wasmAssetManifest struct {
	Assets map[string]struct {
		Integrity string `json:"integrity"`
	} `json:"assets"`
}

func loadWASMAssetManifest(path string) (wasmAssetManifest, error) {
	var manifest wasmAssetManifest
	data, err := os.ReadFile(path)
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, err
	}
	for _, name := range []string{"securefetch.wasm", "wasm_exec.js"} {
		asset, ok := manifest.Assets[name]
		if !ok || !strings.HasPrefix(asset.Integrity, "sha256-") {
			return manifest, fmt.Errorf("manifest is missing a valid integrity pin for %s", name)
		}
	}
	return manifest, nil
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	generateKey := flag.Bool("generate-key", false, "generate an FH secure transport server key")
	flag.Parse()

	if *generateKey {
		key, err := secure.GenerateServerPrivateKey()
		if err != nil {
			log.Fatal(err)
		}
		encoded, _ := secure.EncodeServerPrivateKey(key)
		fmt.Println(encoded)
		return
	}

	serverKey, err := secure.DecodeServerPrivateKey(os.Getenv("FH_SECURE_SERVER_KEY"))
	if err != nil {
		log.Fatalf("FH_SECURE_SERVER_KEY is required. Generate one with: go run ./examples/secure_wasm -generate-key")
	}
	sessionSecret, err := decodeSecret("FH_SESSION_SECRET", 32)
	if err != nil {
		log.Fatal(err)
	}
	origins := parseOrigins(os.Getenv("FH_APP_ORIGIN"))
	if len(origins) == 0 {
		// Accept both loopback hostnames in dev: the browser treats
		// localhost and 127.0.0.1 as distinct origins, so whichever one
		// is used to load the page must also be the one WASM fetches
		// bind to, or CSP's "connect-src 'self'" rejects the request.
		origins = []string{"http://localhost:8080", "http://127.0.0.1:8080"}
	}
	origin := origins[0]
	secureCookie := strings.HasPrefix(origin, "https://")
	assetManifest, err := loadWASMAssetManifest("wasm/dist/asset-manifest.json")
	if err != nil {
		log.Fatalf("build WASM assets first with `make wasm`: %v", err)
	}

	sessionManager := session.NewSessionManager(
		session.NewMemoryStore(time.Minute),
		session.SessionCookieName(exampleSessionName),
		session.SessionSecret(sessionSecret),
		session.SessionMaxAge(30*time.Minute),
		session.SessionSecure(secureCookie),
		session.SessionSameSite(fh.SameSiteStrict),
	)

	app := fh.NewProduction()
	app.Use(security.New(security.Config{
		ContentSecurityPolicy:     "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; worker-src 'self'",
		HSTSMaxAge:                31536000,
		HSTSIncludeSubDomains:     true,
		FrameDeny:                 true,
		ContentTypeNosniff:        true,
		XSSProtection:             "0",
		CrossOriginOpenerPolicy:   "same-origin",
		CrossOriginResourcePolicy: "same-origin",
		CrossOriginEmbedderPolicy: "require-corp",
		ReferrerPolicy:            "no-referrer",
		PermissionsPolicy:         "geolocation=(), microphone=(), camera=(), payment=(), usb=()",
	}))
	app.Use(session.New(sessionManager))

	transport, err := secure.Install(app, secure.Config{
		ServerPrivateKey: serverKey,
		KeyID:            "example-server-v1",
		RequireSecure:    true,
		Protect: func(c fh.Ctx) bool {
			return strings.HasPrefix(c.Path(), "/api/")
		},
		AllowedOrigins: origins,
		RequireOrigin:  true,
		AuthorizeDeviceRegistration: func(c fh.Ctx, _ protocol.DeviceRegistrationRequest) (string, error) {
			user, ok := currentUser(c)
			if !ok {
				return "", fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login is required before device registration")
			}
			return user.ID, nil
		},
		ValidateSession: func(c fh.Ctx, info secure.SessionInfo) error {
			user, ok := currentUser(c)
			if !ok {
				return fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login session is required")
			}
			if info.Principal != user.ID {
				return fh.NewHTTPError(fh.StatusUnauthorized, "SESSION_PRINCIPAL_MISMATCH", "secure session does not match the logged-in user")
			}
			return nil
		},
		OnSecurityEvent: func(event secure.SecurityEvent) {
			log.Printf("security type=%s device=%s session=%s request=%s detail=%s", event.Type, event.DeviceID, event.SessionID, event.RequestID, event.Detail)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	app.Get("/", func(c fh.Ctx) error {
		return c.SendFile("examples/secure_wasm/public/index.html")
	})
	app.Get("/auth/me", func(c fh.Ctx) error {
		user, ok := currentUser(c)
		if !ok {
			return c.JSON(fh.Map{"authenticated": false})
		}
		return c.JSON(fh.Map{"authenticated": true, "user": user, "session_cookie": exampleSessionName})
	})
	app.Post("/auth/login", func(c fh.Ctx) error {
		var request loginRequest
		if err := c.BodyParser(&request); err != nil {
			return fh.NewHTTPError(fh.StatusBadRequest, "LOGIN_REQUEST_INVALID", "login request is invalid")
		}
		if subtle.ConstantTimeCompare([]byte(request.Username), []byte(demoUsername)) != 1 || subtle.ConstantTimeCompare([]byte(request.Password), []byte(demoPassword)) != 1 {
			return fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_INVALID", "invalid username or password")
		}
		s := session.Get(c)
		s.Set(sessionUserIDKey, demoUserID)
		s.Set(sessionUserNameKey, demoUserName)
		s.Set(sessionUserRoleKey, demoUserRole)
		s.Set(sessionLoginKey, time.Now().UTC().Format(time.RFC3339))
		if err := sessionManager.Regenerate(c, s); err != nil {
			return err
		}
		return c.JSON(fh.Map{"authenticated": true, "user": appUser{ID: demoUserID, Name: demoUserName, Role: demoUserRole}})
	})
	app.Post("/auth/logout", func(c fh.Ctx) error {
		if err := sessionManager.Destroy(c, session.Get(c)); err != nil {
			return err
		}
		return c.SendStatus(fh.StatusNoContent)
	})
	app.Get("/secure-config.json", func(c fh.Ctx) error {
		if _, ok := currentUser(c); !ok {
			return fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login is required before loading secure transport config")
		}
		c.Set("Cache-Control", "no-store")
		return c.JSON(fh.Map{
			"baseURL":               matchOrigin(origins, origin, c.Get("Host")),
			"pinnedServerKey":       transport.PublicKeyBase64(),
			"keyID":                 transport.KeyID(),
			"wasmIntegrity":         assetManifest.Assets["securefetch.wasm"].Integrity,
			"wasmExecIntegrity":     assetManifest.Assets["wasm_exec.js"].Integrity,
			"requireAssetIntegrity": true,
		})
	})
	app.Static("/assets", "examples/secure_wasm/public", fh.StaticConfig{MaxAge: 300})
	app.Static("/wasm", "wasm/dist", fh.StaticConfig{MaxAge: 300})

	app.Get("/api/profile", func(c fh.Ctx) error {
		user, transportSession, err := authenticatedSecureContext(c)
		if err != nil {
			return err
		}
		return c.JSON(fh.Map{
			"user":                        user,
			"app_session_cookie":          exampleSessionName,
			"secure_transport_principal":  transportSession.Principal,
			"secure_transport_device_id":  protocol.EncodeID(transportSession.DeviceID),
			"secure_transport_session_id": protocol.EncodeID(transportSession.ID),
			"secure_transport_expires_at": transportSession.ExpiresAt.UTC().Format(time.RFC3339),
		})
	})
	app.Post("/api/echo", func(c fh.Ctx) error {
		user, transportSession, err := authenticatedSecureContext(c)
		if err != nil {
			return err
		}
		c.Set("X-Application-Metadata", "this header is encrypted by the secure transport")
		body := c.BodyCopy()
		response := fh.Map{
			"secured":                     true,
			"user":                        user,
			"secure_transport_principal":  transportSession.Principal,
			"secure_transport_session_id": protocol.EncodeID(transportSession.ID),
			"content_type":                c.Get(fh.HeaderContentTypeStr),
			"bytes":                       len(body),
		}
		if json.Valid(body) {
			var value any
			if err := json.Unmarshal(body, &value); err == nil {
				response["json"] = value
			}
		}
		if utf8.Valid(body) {
			response["text"] = string(body)
		} else {
			response["base64"] = base64.RawURLEncoding.EncodeToString(body)
		}
		return c.JSON(response)
	})

	log.Printf("FH secure WASM auth example listening on %s (origin %s)", *addr, origin)
	log.Fatal(app.ListenWithGracefulShutdown(*addr))
}

func currentUser(c fh.Ctx) (appUser, bool) {
	s := session.Get(c)
	id, _ := s.Get(sessionUserIDKey).(string)
	name, _ := s.Get(sessionUserNameKey).(string)
	role, _ := s.Get(sessionUserRoleKey).(string)
	if id == "" {
		return appUser{}, false
	}
	return appUser{ID: id, Name: name, Role: role}, true
}

func authenticatedSecureContext(c fh.Ctx) (appUser, secure.SessionInfo, error) {
	user, ok := currentUser(c)
	if !ok {
		return appUser{}, secure.SessionInfo{}, fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login session is required")
	}
	transportSession, ok := secure.SessionFromContext(c)
	if !ok {
		return appUser{}, secure.SessionInfo{}, fh.NewHTTPError(fh.StatusUnauthorized, "SECURE_SESSION_REQUIRED", "secure transport session is required")
	}
	if transportSession.Principal != user.ID {
		return appUser{}, secure.SessionInfo{}, fh.NewHTTPError(fh.StatusUnauthorized, "SESSION_PRINCIPAL_MISMATCH", "secure session does not match the logged-in user")
	}
	return user, transportSession, nil
}

// parseOrigins splits a comma-separated FH_APP_ORIGIN value into a
// normalized, order-preserving list of unique origins.
func parseOrigins(value string) []string {
	var origins []string
	seen := make(map[string]struct{})
	for _, part := range strings.Split(value, ",") {
		o := strings.TrimRight(strings.TrimSpace(part), "/")
		if o == "" {
			continue
		}
		if _, ok := seen[o]; ok {
			continue
		}
		seen[o] = struct{}{}
		origins = append(origins, o)
	}
	return origins
}

// matchOrigin returns whichever allowed origin has the same host:port as the
// incoming request's Host header, falling back to fallback otherwise. This
// keeps the baseURL handed to the WASM client aligned with the origin the
// browser actually loaded the page from, which is required for the
// "connect-src 'self'" CSP directive to permit its fetches.
func matchOrigin(origins []string, fallback, host string) string {
	if host == "" {
		return fallback
	}
	for _, o := range origins {
		if strings.HasSuffix(o, "://"+host) {
			return o
		}
	}
	return fallback
}

func decodeSecret(name string, minimum int) ([]byte, error) {
	value := os.Getenv(name)
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) < minimum {
		return nil, fmt.Errorf("%s must be base64url and at least %d bytes", name, minimum)
	}
	// Reject an all-zero secret without early exit.
	zero := make([]byte, len(decoded))
	if subtle.ConstantTimeCompare(decoded, zero) == 1 {
		return nil, fmt.Errorf("%s must not be all zero", name)
	}
	return decoded, nil
}
