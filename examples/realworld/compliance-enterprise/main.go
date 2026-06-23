package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/fh"
	middleware "github.com/oarkflow/fh/contrib/mw/authz"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/compliance"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/logger"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/tenant"
	"github.com/oarkflow/fh/mw/timeout"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()

	// ── 1. AuthZ Engine ─────────────────────────────────────────────────────
	engine, err := middleware.LoadEngineFromAuthzFile("config.authz")
	if err != nil {
		log.Fatalf("failed to load authz config: %v", err)
	}
	log.Print("authz engine loaded from config.authz")

	// ── 2. App with Enterprise Compliance ───────────────────────────────────
	app := fh.New(
		fh.WithCompliance(fh.ComplianceConfig{
			Enabled:         true,
			Profile:         fh.ComplianceEnterprise,
			ExposeEndpoints: true,
			EndpointPrefix:  "/_fh",
			FailOnCritical:  true,
		}),
		fh.WithDebug(false),
	)

	// Enable compliance evidence endpoints
	app.EnableComplianceEndpoints("/_fh")

	// ── 3. Global Middleware Stack ──────────────────────────────────────────
	app.Use(recover.New())
	app.Use(requestid.New(requestid.Config{TrustIncoming: true}))
	app.Use(security.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"https://console.acme.com"},
		AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders: []string{"Content-Type", "Authorization", "X-Tenant-ID", "X-Request-ID", "X-Subject-ID", "X-Roles", "X-Clearance"},
	}))
	app.Use(logger.New(logger.Config{FormatName: "json", SkipPaths: []string{"/health", "/_fh/compliance", "/_fh/config/safe"}}))
	app.Use(bodylimit.New(1 << 20))
	app.Use(timeout.New(10 * time.Second))

	// ── 4. Tenant Resolution ──────────────────────────────────────────────
	// Resolve tenant from X-Tenant-ID header, falling back to Principal.TenantID
	app.Use(tenant.New(tenant.Config{
		Header:   "X-Tenant-ID",
		Required: true,
	}))

	// ── 5. Principal Extraction ───────────────────────────────────────────
	// Extract identity from headers (simulating a verified JWT/session)
	app.Use(fh.UsePrincipal(
		fh.PrincipalExtractor(fh.PrincipalExtractors{
			ID:         fh.HeaderString("X-Subject-ID"),
			Type:       fh.StaticString("user"),
			TenantID:   fh.LocalString("tenant_id"),
			Subject:    fh.HeaderString("X-Subject-ID"),
			Roles:      fh.HeaderCSV("X-Roles"),
			Scopes:     fh.HeaderCSV("X-Scopes"),
			AuthMethod: fh.StaticString("header"),
		}),
		true,
	))

	// ── 6. AuthZ from contrib — bridge Principal → authz.Subject ─────────
	// Uses SubjectFromPrincipal() which maps fh.Principal to authz.Subject
	app.Use(middleware.FHWithConfig(middleware.FHConfig{
		Engine: engine,
		Context: func(c *fh.Ctx) context.Context {
			if c.Context() != nil {
				return c.Context()
			}
			return context.Background()
		},
		Subject: middleware.SubjectFromPrincipal(),
		Action:  middleware.ActionFromMethod(),
		Resource: func(c *fh.Ctx) (*authz.Resource, bool, error) {
			tenantID := c.Locals("tenant_id")
			tenantStr, _ := tenantID.(string)
			res := &authz.Resource{
				ID:       c.Method() + ":" + c.Path(),
				Type:     "route",
				TenantID: tenantStr,
				Attrs: map[string]any{
					"path":   c.Path(),
					"method": c.Method(),
				},
			}
			parts := strings.Split(strings.Trim(c.Path(), "/"), "/")
			if len(parts) >= 3 && parts[0] == "api" && parts[1] == "users" {
				res.OwnerID = parts[len(parts)-1]
			}
			return res, true, nil
		},
		Environment: func(c *fh.Ctx) (*authz.Environment, bool, error) {
			tenantID := c.Locals("tenant_id")
			tenantStr, _ := tenantID.(string)
			return &authz.Environment{
				Time:     time.Now(),
				TenantID: tenantStr,
				Extra: map[string]any{
					"ip":  c.IP(),
					"ua":  c.Get("User-Agent"),
					"sid": c.Get("X-Subject-ID"),
				},
			}, true, nil
		},
		OnDenied: func(c *fh.Ctx, decision *authz.Decision) error {
			return c.Status(http.StatusForbidden).JSON(fh.Map{
				"error":   "access_denied",
				"message": "insufficient permissions",
				"reason":  decision.Reason,
			})
		},
		OnError: func(c *fh.Ctx, err error) error {
			status := http.StatusInternalServerError
			if errors.Is(err, middleware.ErrMissingEngine) {
				status = http.StatusServiceUnavailable
			}
			return c.Status(status).JSON(fh.Map{
				"error":   "authorization_error",
				"message": err.Error(),
			})
		},
		SkipPaths: []string{
			"/health",
			"/_fh/compliance",
			"/_fh/compliance/controls",
			"/_fh/compliance/findings",
			"/_fh/config/safe",
		},
	}))

	// ── 7. Routes ──────────────────────────────────────────────────────────
	app.Get("/health", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"status": "ok", "time": time.Now().UTC()})
	})

	// Compliance evidence is available at /_fh/compliance (skip-listed above)

	// Tenant-scoped document API
	docs := app.Group("/api/documents")
	docs.Get("", func(c *fh.Ctx) error {
		tenantID := c.Locals("tenant_id")
		return c.JSON(fh.Map{
			"tenant":   tenantID,
			"document": "list",
			"decision": middleware.FHDecision(c),
		})
	})
	docs.Get("/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{
			"document": c.Param("id"),
			"decision": middleware.FHDecision(c),
		})
	})
	docs.Post("/sensitive",
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{
				AuthRequired:        true,
				IdempotencyRequired: true,
			},
			Data: fh.DataPolicy{Sensitivity: "confidential", RedactLogs: true},
		}),
		func(c *fh.Ctx) error {
			return c.Status(http.StatusCreated).JSON(fh.Map{
				"status": "sensitive document created",
				"decision": middleware.FHDecision(c),
			})
		},
	)

	// Owner-scoped user profiles
	app.Get("/api/users/:id",
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{AuthRequired: true},
			Data:     fh.DataPolicy{Sensitivity: "private", RedactLogs: true},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"user":     c.Param("id"),
				"tenant":   c.Locals("tenant_id"),
				"decision": middleware.FHDecision(c),
			})
		},
	)

	// Admin-only route
	app.Get("/api/admin/settings",
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{AuthRequired: true, Roles: []string{"admin", "superadmin"}},
			Data:     fh.DataPolicy{Sensitivity: "confidential", RedactLogs: true},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"settings": "admin configuration",
				"decision": middleware.FHDecision(c),
			})
		},
	)

	// Compliance dashboard
	app.Get("/api/compliance/report",
		compliance.New(compliance.Config{
			Security: fh.RouteSecurityConfig{AuthRequired: true, Roles: []string{"compliance-officer", "auditor", "admin"}},
			Data:     fh.DataPolicy{Sensitivity: "confidential", RedactLogs: true},
		}),
		func(c *fh.Ctx) error {
			return c.JSON(fh.Map{
				"report": app.ComplianceReport(),
				"decision": middleware.FHDecision(c),
			})
		},
	)

	// ── 8. Start ──────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	log.Printf("compliance-enterprise listening on %s", *addr)
	if err := app.Listen(*addr); err != nil {
		log.Fatal(err)
	}
}
