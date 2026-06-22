package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/apiversion"
	"github.com/oarkflow/fh/mw/basicauth"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/cache"
	"github.com/oarkflow/fh/mw/circuitbreaker"
	"github.com/oarkflow/fh/mw/compress"
	"github.com/oarkflow/fh/mw/contract"
	"github.com/oarkflow/fh/mw/correlationid"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/fh/mw/earlydata"
	"github.com/oarkflow/fh/mw/ipwhitelist"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/metrics"
	"github.com/oarkflow/fh/mw/policy"
	"github.com/oarkflow/fh/mw/proxy"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/replay"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/rewrite"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/mw/signature"
	"github.com/oarkflow/fh/mw/skip"
	staticmw "github.com/oarkflow/fh/mw/static"
	"github.com/oarkflow/fh/mw/timeout"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	upstream := flag.String("upstream", "http://127.0.0.1:4000", "gateway upstream")
	flag.Parse()

	app := fh.New()
	requests := metrics.New()
	sessions := session.NewMemoryStore(10 * time.Minute)
	defer sessions.StopGC()
	sessionManager := session.NewSessionManager(sessions,
		session.SessionCookieName("fh_session"),
		session.SessionSecret([]byte(env("SESSION_SECRET", "dev-only-change-this-session-secret"))),
		session.SessionMaxAge(8*time.Hour),
		session.SessionHTTPOnly(true),
		session.SessionSameSite(fh.SameSiteLax),
	)

	// The outer stack establishes identity, safety, observability, and resource bounds.
	app.Use(recover.New())
	app.Use(requestid.New(requestid.Config{TrustIncoming: true, Generator: requestid.NewAtomicGeneratorWithPrefix("edge")}))
	app.Use(correlationid.New(correlationid.Config{TrustIncoming: true}))
	app.Use(requests.Middleware())
	app.Use(security.New())
	app.Use(earlydata.New(earlydata.Config{AllowWithIdempotencyKey: true}))
	app.Use(bodylimit.New(1 << 20))
	app.Use(timeout.New(3 * time.Second))
	app.Use(logger.New(logger.Config{FormatName: "json", SkipPaths: []string{"/health"}}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"GET", "POST", "OPTIONS"},
		AllowHeaders: []string{"Content-Type", "X-API-Key", "X-Request-ID", "X-CSRF-Token", "X-Nonce"},
	}))
	app.Use(skip.New(ratelimiter.New(ratelimiter.Config{Max: 120, Window: time.Minute}), skip.Any(skip.Health(), skip.Static())))
	app.Use(skip.New(cache.New(cache.Config{TTL: 20 * time.Second, MaxEntries: 256, VaryHeaders: []string{"Accept-Version"}}), skip.Prefixes("/admin", "/account", "/webhooks", "/gateway")))
	app.Use(compress.New(compress.Config{MinSize: 256}))
	app.Use(rewrite.New(rewrite.Rule{From: "/legacy/catalog", To: "/api/catalog", Methods: []string{"GET"}}))
	app.Use(session.New(sessionManager))

	app.Get("/health", func(c *fh.Ctx) error { return c.JSON(fh.Map{"status": "ok"}) })
	publicDir := filepath.Join(sourceDir(), "public")
	app.Get("/assets/*", staticmw.New(publicDir, staticmw.Config{
		Root: publicDir, Prefix: "/assets/", ETag: true, LastModified: true, MaxAge: time.Hour,
	}))

	// A versioned public catalog carries explicit data-handling metadata.
	app.Get("/api/catalog",
		policy.New(policy.Config{
			Data:    fh.DataPolicy{Sensitivity: "public", JournalMode: "metadata-only"},
			Version: apiversion.Config{Default: "2026-01", Supported: []string{"2025-10", "2026-01"}, Deprecated: map[string]string{"2025-10": "Wed, 30 Sep 2026 23:59:59 GMT"}},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{"items": []string{"notebook", "pen", "backpack"}, "version": c.Locals("api_version")})
		},
	)

	// Machine clients get a separate API-key and contract boundary.
	partner := app.Group("/partner",
		apikey.New(apikey.Config{Keys: []string{env("PARTNER_API_KEY", "partner-demo-key")}}),
		apiversion.New(apiversion.Config{Default: "v2", Supported: []string{"v2"}}),
	)
	partner.Post("/shipments",
		contract.New(contract.Config{Methods: []string{"POST"}, ContentTypes: []string{"application/json"}, RequireHeaders: []string{"X-Partner-Request-ID"}, MaxBodyBytes: 64 << 10}),
		func(c *fh.Ctx) error {
			return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "queued", "partner_request_id": c.Get("X-Partner-Request-ID")})
		},
	)

	// Browser state changes use a signed session plus double-submit CSRF protection.
	accountCSRF := csrf.New(csrf.Config{TrustedOrigins: []string{"http://localhost:3000"}})
	app.Get("/account", accountCSRF, func(c *fh.Ctx) error {
		s := session.Get(c)
		return c.JSON(fh.Map{"user": s.Get("user"), "csrf_token": c.Locals("csrf_token")})
	})
	app.Post("/account/login", accountCSRF, func(c *fh.Ctx) error {
		s := session.Get(c)
		s.Set("user", "demo-user")
		if err := sessionManager.Regenerate(c, s); err != nil {
			return err
		}
		return c.JSON(fh.Map{"status": "signed_in"})
	})

	// Webhooks require both a recent HMAC signature and a one-time nonce.
	app.Post("/webhooks/payments",
		signature.New(signature.Config{Secret: []byte(env("WEBHOOK_SECRET", "webhook-demo-secret")), Tolerance: 5 * time.Minute}),
		replay.New(replay.Config{Header: "X-Nonce", TTL: 10 * time.Minute}),
		func(c *fh.Ctx) error { return c.Status(fh.StatusAccepted).JSON(fh.Map{"status": "accepted"}) },
	)

	// Operations endpoints illustrate two different human/network trust boundaries.
	app.Get("/admin", basicauth.New(env("ADMIN_USER", "admin"), env("ADMIN_PASSWORD", "change-me")), func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"service": "operations", "request_id": c.Locals("requestID")})
	})
	app.Get("/_internal/metrics", ipwhitelist.New("127.0.0.1/32", "::1/128"), requests.Handler())

	// Gateway failures trip a circuit before the unhealthy upstream is called again.
	breaker := circuitbreaker.New(circuitbreaker.Config{FailureThreshold: 3, ResetAfter: 15 * time.Second})
	app.All("/gateway/*", breaker.Handler(), proxy.New(proxy.Config{
		Target: *upstream, StripPrefix: "/gateway", Timeout: 2 * time.Second,
		ErrorHandler: func(c *fh.Ctx, err error) error {
			return fh.NewHTTPError(fh.StatusBadGateway, "UPSTREAM_UNAVAILABLE", "catalog upstream is unavailable")
		},
	}))

	log.Printf("secure middleware example listening on %s", *addr)
	log.Fatal(app.Listen(*addr))
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func sourceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file)
}
