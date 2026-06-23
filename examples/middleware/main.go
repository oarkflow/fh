// Command middleware demonstrates body limits, rewrites, conditional skips,
// CSRF, response caching, Early-Data protection, and CORS preflights.
package main

import (
	"log"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
	cachemw "github.com/oarkflow/fh/mw/cache"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/fh/mw/earlydata"
	"github.com/oarkflow/fh/mw/rewrite"
	"github.com/oarkflow/fh/mw/skip"
)

func main() {
	app := fh.New(
		fh.WithMaxRequestBodySize(2 << 20), // absolute server ceiling: 2 MiB
	)

	// Reject unsafe Early-Data requests unless protected by Idempotency-Key.
	app.Use(earlydata.New(earlydata.Config{
		AllowWithIdempotencyKey: true,
	}))

	// CORS should usually run before CSRF and application middleware.
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"GET", "POST", "PUT", "OPTIONS"},
		AllowHeaders: []string{
			"Content-Type",
			"X-CSRF-Token",
			"Idempotency-Key",
		},
		MaxAge: 600,
	}))

	// Rewrite legacy paths before matching routes.
	app.Use(rewrite.New(
		rewrite.Rule{From: "/legacy", To: "/v2/home"},
		rewrite.Rule{From: "/old-users/:id", To: "/users/:id"},
		rewrite.Rule{From: "/old-docs/*path", To: "/docs/*path"},
	))

	// Global body limit, skipped for health/static/CORS preflight.
	app.Use(skip.New(
		bodylimit.New(1<<20), // 1 MiB middleware ceiling
		skip.Any(
			skip.Health(),
			skip.Static(),
			skip.Preflight(),
		),
	))

	// CSRF, skipped for health, cached GET endpoint, safe methods and preflight.
	app.Use(skip.New(
		csrf.New(),
		skip.Any(
			skip.Health(),
			skip.Paths("/cached"),
			skip.Preflight(),
		),
	))

	// Cache only /cached. This middleware already has its own Next predicate.
	app.Use(cachemw.New(cachemw.Config{
		TTL:        30 * time.Second,
		MaxEntries: 256,
		Next: func(c *fh.Ctx) bool {
			return c.Path() != "/cached"
		},
	}))

	app.Get("/health", func(c *fh.Ctx) error {
		return c.SendString("ok")
	})

	app.Get("/csrf-token", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"token": c.Locals("csrf_token"),
		})
	})

	app.Post("/small", bodylimit.New(8<<10), func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"accepted_bytes": len(c.Body()),
		})
	})

	app.Get("/v2/home", func(c *fh.Ctx) error {
		return c.SendString("rewritten static route")
	})

	app.Get("/users/:id", func(c *fh.Ctx) error {
		return c.JSON(map[string]string{
			"id": c.Param("id"),
		})
	})

	app.Get("/docs/*path", func(c *fh.Ctx) error {
		return c.SendString("docs: " + c.Param("path"))
	})

	app.Get("/cached", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"generated_at": time.Now().UTC(),
		})
	})

	// Demonstrates advanced skip composition.
	adminLogger := func(c *fh.Ctx) error {
		log.Printf("admin request method=%s path=%s ip=%s", c.Method(), c.Path(), c.IP())
		return c.Next()
	}

	app.Use(skip.NewWithConfig(adminLogger, skip.Config{
		Logic: skip.LogicAny,
		Predicate: skip.Any(
			// Do not log health/static/preflight.
			skip.Health(),
			skip.Static(),
			skip.Preflight(),

			// Only log admin area. This skips everything outside /admin.
			skip.Not(skip.Prefixes("/admin")),
		),
		RecoverPredicatePanic: true,
		OnPredicatePanic: func(c *fh.Ctx, recovered any) {
			log.Printf("skip predicate panic path=%s err=%v", c.Path(), recovered)
		},
	}))

	app.Get("/admin/dashboard", func(c *fh.Ctx) error {
		return c.SendString("admin dashboard")
	})

	log.Println("middleware example listening on http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}
