package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"flag"
	"github.com/oarkflow/fh/delegation"
	"github.com/oarkflow/fh/gateway"
	"github.com/oarkflow/fh/internal/certconfig"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	bind := flag.String("listen", "127.0.0.1:9443", "HTTPS listen address")
	cert := flag.String("cert", "", "HTTPS certificate")
	key := flag.String("key", "", "HTTPS private key")
	upstreamTCP := flag.String("upstream-tcp", "127.0.0.1:7448", "backend mTLS bootstrap TCP address")
	upstreamUDP := flag.String("upstream-udp", "127.0.0.1:7447", "backend MIRA UDP address")
	clientCert := flag.String("client-cert", "", "backend mTLS client certificate")
	clientKey := flag.String("client-key", "", "backend mTLS client key")
	ca := flag.String("ca", "", "backend server CA")
	upstreamName := flag.String("upstream-name", "server.local", "backend certificate DNS name")
	browserUser := flag.String("user", "gateway-user", "single allowed gateway principal, demo profile")
	allowedOrigin := flag.String("origin", "", "explicit allowed HTTPS browser origin (required for browser cross-origin access)")
	jwtKey := flag.String("jwt-public-key", "", "pinned PEM signing key for multi-principal JWT access")
	jwksURL := flag.String("jwt-jwks-url", "", "HTTPS-pinned JWKS endpoint with fail-closed rotation")
	oidcDiscovery := flag.Bool("oidc-discovery", false, "discover same-origin JWKS endpoint from pinned -jwt-issuer")
	jwtKid := flag.String("jwt-kid", "", "JWT signing key ID")
	jwtAlg := flag.String("jwt-alg", "RS256", "JWT algorithm: RS256 or EdDSA")
	delegationKey := flag.String("delegation-key", "", "legacy 0600 Ed25519 PKCS8 key (no rotation)")
	delegationRing := flag.String("delegation-ring", "", "JSON keyring for SIGHUP reloadable gateway assertion signing keys")
	delegationAudience := flag.String("delegation-audience", "mira-upstream", "pinned backend principal delegation audience")
	jwtIssuer := flag.String("jwt-issuer", "", "exact JWT issuer")
	jwtAudience := flag.String("jwt-audience", "", "exact JWT audience")
	revocationsPath := flag.String("jwt-revocations", "", "locally provisioned JWT revocation JSON (SIGHUP reloaded, fail-closed expiry)")
	flag.Parse()
	// Explicit mode selection: the legacy single-user token is demo-only.
	var authenticate gateway.Authenticate
	if *jwtKey != "" && (*jwksURL != "" || *oidcDiscovery) {
		log.Fatal("choose exactly one JWT signing-key source")
	}
	if *jwksURL != "" || *oidcDiscovery {
		endpoint := *jwksURL
		if *oidcDiscovery {
			if endpoint != "" {
				log.Fatal("choose JWKS URL or OIDC discovery, not both")
			}
			dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var err error
			endpoint, err = gateway.DiscoverOIDCJWKS(dctx, *jwtIssuer, nil)
			cancel()
			if err != nil {
				log.Fatal(err)
			}
		}
		rotate, err := gateway.NewRotatingJWKS(gateway.JWKSOptions{Issuer: *jwtIssuer, Audience: *jwtAudience, URL: endpoint, ClockSkew: 15 * time.Second})
		if err != nil {
			log.Fatal(err)
		}
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = rotate.Refresh(cctx)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		lifetime, stop := context.WithCancel(context.Background())
		defer stop()
		go rotate.Run(lifetime)
		authenticate = rotate.Authenticate
	} else if *jwtKey != "" {
		pemData, err := os.ReadFile(*jwtKey)
		if err != nil {
			log.Fatal(err)
		}
		pub, err := gateway.ParseJWTPublicKeyPEM(pemData, *jwtAlg)
		if err != nil {
			log.Fatal(err)
		}
		verifier := &gateway.JWTVerifier{Issuer: *jwtIssuer, Audience: *jwtAudience, Keys: map[string]gateway.JWTKey{*jwtKid: {KeyID: *jwtKid, Algorithm: *jwtAlg, PublicKey: pub}}, ClockSkew: 15 * time.Second}
		if err := verifier.Validate(); err != nil {
			log.Fatal(err)
		}
		authenticate = verifier.Authenticate
	} else {
		token := os.Getenv("MIRA_GATEWAY_TOKEN")
		if len(token) < 32 {
			log.Fatal("JWT mode requires -jwt-public-key; demo mode requires MIRA_GATEWAY_TOKEN with 32+ random characters")
		}
		wanted := sha256.Sum256([]byte(token))
		token = ""
		authenticate = func(r *http.Request) (string, error) {
			v := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			actual := sha256.Sum256([]byte(v))
			if v == "" || subtle.ConstantTimeCompare(actual[:], wanted[:]) != 1 {
				return "", os.ErrPermission
			}
			return *browserUser, nil
		}
	}
	jwtEnabled := *jwtKey != "" || *jwksURL != "" || *oidcDiscovery
	var revocations *gateway.RevocationStore
	if *revocationsPath != "" {
		if !jwtEnabled {
			log.Fatal("-jwt-revocations requires JWT authentication")
		}
		revocations = &gateway.RevocationStore{MaxAge: 24 * time.Hour}
		if err := revocations.Reload(*revocationsPath); err != nil {
			log.Fatal(err)
		}
		authenticate = revocations.Wrap(authenticate)
	}
	var bindPrincipal func(context.Context, *mira.Session, protocol.Channel, string) error
	if *delegationRing != "" && *delegationKey != "" {
		log.Fatal("choose delegation-ring or delegation-key")
	}
	if *delegationRing != "" {
		signer := &delegation.RotatingSigner{}
		if err := signer.Reload(*delegationRing); err != nil {
			log.Fatal(err)
		}
		bindPrincipal = func(ctx context.Context, session *mira.Session, ch protocol.Channel, subject string) error {
			return signer.Prove(ctx, ch, session.ID(), *delegationAudience, subject)
		}
		reloadKeys := make(chan os.Signal, 1)
		signal.Notify(reloadKeys, syscall.SIGHUP)
		defer signal.Stop(reloadKeys)
		lifetime, stopReload := context.WithCancel(context.Background())
		defer stopReload()
		go func() {
			for {
				select {
				case <-lifetime.Done():
					return
				case <-reloadKeys:
					if err := signer.Reload(*delegationRing); err != nil {
						log.Printf("delegation key reload refused: %v", err)
					} else {
						log.Print("delegation signing key reloaded")
					}
				}
			}
		}()
	}
	if *delegationKey != "" {
		private, e := delegation.LoadPrivateKey(*delegationKey)
		if e != nil {
			log.Fatal(e)
		}
		bindPrincipal = func(ctx context.Context, session *mira.Session, ch protocol.Channel, subject string) error {
			return delegation.Prove(ctx, ch, session.ID(), private, *delegationAudience, subject)
		}
	}
	if jwtEnabled && bindPrincipal == nil {
		log.Fatal("multi-user JWT gateway requires -delegation-key and matching backend identity verification")
	}
	config, err := certconfig.Client(*clientCert, *clientKey, *ca, *upstreamName)
	if err != nil {
		log.Fatal(err)
	}
	g, err := gateway.New(gateway.Config{
		Authenticate:            authenticate,
		RequirePrincipalBinding: jwtEnabled, BindPrincipal: bindPrincipal,
		DialBackend: func(ctx context.Context, principal string) (*mira.Session, error) {
			return mira.DialTLS(ctx, *upstreamTCP, *upstreamUDP, config, mira.Options{})
		},
		MaxSessions: 256, MaxSessionsPerPrincipal: 4, IdleTimeout: 10 * time.Minute,
		AllowedOrigins: func() []string {
			if *allowedOrigin == "" {
				return nil
			}
			return []string{*allowedOrigin}
		}(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer g.Close()
	if revocations != nil {
		reload := make(chan os.Signal, 1)
		signal.Notify(reload, syscall.SIGHUP)
		defer signal.Stop(reload)
		lifetime, stop := context.WithCancel(context.Background())
		defer stop()
		go func() {
			for {
				select {
				case <-lifetime.Done():
					return
				case <-reload:
					if err := revocations.Reload(*revocationsPath); err != nil {
						log.Printf("JWT revocation reload refused: %v", err)
					} else {
						g.InvalidateAllSessions()
						log.Print("JWT revocations reloaded; existing gateway sessions disconnected")
					}
				}
			}
		}()
	}
	server := &http.Server{Addr: *bind, Handler: g.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("MIRA browser fetch gateway on https://%s", *bind)
	if err := server.ListenAndServeTLS(*cert, *key); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
