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
	app := fh.New(fh.Config{MaxRequestBodySize: 2 << 20}) // absolute server ceiling

	app.Use(earlydata.New(earlydata.Config{AllowWithIdempotencyKey: true}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"GET", "POST", "PUT"},
		AllowHeaders: []string{"Content-Type", "X-CSRF-Token", "Idempotency-Key"},
		MaxAge:       600,
	}))
	app.Use(rewrite.New(
		rewrite.Rule{From: "/legacy", To: "/v2/home"},
		rewrite.Rule{From: "/old-users/:id", To: "/users/:id"},
		rewrite.Rule{From: "/old-docs/*path", To: "/docs/*path"},
	))
	app.Use(skip.New(bodylimit.New(1<<20), skip.Paths("/health")))
	app.Use(skip.New(csrf.New(), skip.Paths("/health", "/cached")))
	app.Use(cachemw.New(cachemw.Config{
		TTL: 30 * time.Second, MaxEntries: 256,
		Next: func(c *fh.Ctx) bool { return c.Path() != "/cached" },
	}))

	app.Get("/health", func(c *fh.Ctx) error { return c.SendString("ok") })
	app.Get("/csrf-token", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{"token": c.Locals("csrf_token")})
	})
	app.Post("/small", bodylimit.New(8<<10), func(c *fh.Ctx) error {
		return c.JSON(map[string]any{"accepted_bytes": len(c.Body())})
	})
	app.Get("/v2/home", func(c *fh.Ctx) error { return c.SendString("rewritten static route") })
	app.Get("/users/:id", func(c *fh.Ctx) error { return c.JSON(map[string]string{"id": c.Param("id")}) })
	app.Get("/docs/*path", func(c *fh.Ctx) error { return c.SendString("docs: " + c.Param("path")) })
	app.Get("/cached", func(c *fh.Ctx) error { return c.JSON(map[string]any{"generated_at": time.Now().UTC()}) })

	log.Println("middleware example listening on http://localhost:3000")
	log.Fatal(app.Listen(":3000"))
}
