package main

import (
	"context"
	"errors"
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
)

func main() {
	engine, err := middleware.LoadEngineFromAuthzFile("config.authz")
	if err != nil {
		log.Fatal(err)
	}

	app := fh.New(fh.Config{
		ReadTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		IdleTimeout:        60 * time.Second,
		MaxRequestBodySize: 4 * 1024 * 1024,
	})

	// Public health route should not go through AuthZ.
	app.Get("/health", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"ok":   true,
			"time": time.Now().Format(time.RFC3339),
		})
	})

	// Apply authorization globally.
	//
	// Important:
	// Do NOT skip /public/info here.
	// The point is to prove that /public/info is allowed by config.authz ACL:
	//
	// acl acl-route-public {
	//   resource route:GET:/public/info
	//   subject guest
	//   actions [GET]
	//   effect allow
	// }
	app.Use(middleware.FHWithConfig(middleware.FHConfig{
		Engine: engine,

		Context: func(c *fh.Ctx) context.Context {
			if c.Context() != nil {
				return c.Context()
			}
			return context.Background()
		},

		Subject: func(c *fh.Ctx) *authz.Subject {
			subjectID := strings.TrimSpace(c.Get("X-Subject-ID"))
			tenantID := strings.TrimSpace(c.Get("X-Tenant-ID"))

			return &authz.Subject{
				ID:       subjectID,
				TenantID: tenantID,
				Type:     "user",
				Roles:    splitTrim(c.Get("X-Roles"), ","),
				Attrs: map[string]any{
					"clearance": c.Get("X-Clearance"),
					"ip":        c.IP(),
					"userAgent": c.Get("User-Agent"),
				},
			}
		},

		Resource: func(c *fh.Ctx) *authz.Resource {
			tenantID := strings.TrimSpace(c.Get("X-Tenant-ID"))

			res := &authz.Resource{
				ID:       c.Method() + ":" + c.Path(),
				Type:     "route",
				TenantID: tenantID,
				Attrs: map[string]any{
					"path":   c.Path(),
					"method": c.Method(),
				},
			}

			// Owner route example:
			//
			// /users/alice -> owner_id = alice
			//
			// This matches config.authz:
			//
			// policy route-owner-profile {
			//   resources [route:GET:/users/*]
			//   when {
			//     resource.owner_id == subject.id
			//   }
			// }
			parts := strings.Split(strings.Trim(c.Path(), "/"), "/")
			if len(parts) >= 2 && parts[0] == "users" {
				res.OwnerID = parts[len(parts)-1]
			}

			return res
		},

		Environment: func(c *fh.Ctx) *authz.Environment {
			return &authz.Environment{
				Time:     time.Now(),
				TenantID: strings.TrimSpace(c.Get("X-Tenant-ID")),
				Extra: map[string]any{
					"ip":        c.IP(),
					"userAgent": c.Get("User-Agent"),
				},
			}
		},

		OnDenied: func(c *fh.Ctx, decision *authz.Decision) error {
			return c.Status(http.StatusForbidden).JSON(map[string]any{
				"error":   "forbidden",
				"message": "access denied",
				"allowed": false,
				"decision": map[string]any{
					"allowed": decision.Allowed,
					"reason":  decision.Reason,
				},
			})
		},

		OnError: func(c *fh.Ctx, err error) error {
			status := http.StatusInternalServerError
			if errors.Is(err, middleware.ErrMissingEngine) {
				status = http.StatusServiceUnavailable
			}

			return c.Status(status).JSON(map[string]any{
				"error":   "authorization_error",
				"message": err.Error(),
			})
		},

		SkipPaths: []string{
			"/health",
		},
	}))

	// ──────────────────────────────────────────────────────────────────────
	// Protected by route-admin role from config.authz
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/admin/dashboard", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"ok":      true,
			"message": "admin dashboard",
			"decision": middleware.FHDecision(c),
		})
	})

	// ──────────────────────────────────────────────────────────────────────
	// Protected by owner policy from config.authz
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/users/:id", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"ok":      true,
			"message": "user profile",
			"user":    c.Param("id"),
			"decision": middleware.FHDecision(c),
		})
	})

	// ──────────────────────────────────────────────────────────────────────
	// Allowed by ACL from config.authz
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/public/info", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"ok":      true,
			"message": "public info",
			"decision": middleware.FHDecision(c),
		})
	})

	// ──────────────────────────────────────────────────────────────────────
	// Generic route that should be denied unless config.authz allows it
	// ──────────────────────────────────────────────────────────────────────
	app.Get("/documents/:id", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"ok":       true,
			"document": c.Param("id"),
			"decision": middleware.FHDecision(c),
		})
	})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("shutting down...")
		if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	addr := ":8081"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	log.Printf("server listening on %s", addr)
	if err := app.Listen(addr); err != nil {
		log.Fatal(err)
	}
}

func splitTrim(s string, sep string) []string {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}

	return out
}
