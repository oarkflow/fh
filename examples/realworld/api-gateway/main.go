package main

import (
	"flag"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/ratelimiter"
	recovermw "github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/rewrite"
	"github.com/oarkflow/fh/mw/security"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	flag.Parse()
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{Enabled: true, DataDir: ".fh-data", JournalEnabled: true, IdempotencyEnabled: false, QueueEnabled: false}))
	app.Use(recovermw.New())
	app.Use(security.New())
	app.Use(cors.New(cors.Config{AllowOrigins: []string{"http://localhost:5173"}, AllowMethods: []string{"GET", "POST", "PUT", "DELETE"}, AllowHeaders: []string{"Content-Type", "Authorization", "Idempotency-Key"}}))
	app.Use(ratelimiter.New(ratelimiter.Config{Max: 120, Window: time.Minute}))
	app.Use(rewrite.New(rewrite.Rule{From: "/v1/*path", To: "/api/v1/*path"}))
	api := app.Group("/api/v1")
	api.Get("/users/:id", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"service": "users", "user_id": c.Param("id"), "gateway_request_id": c.Locals("request_id")})
	})
	api.Post("/orders", func(c *fh.Ctx) error {
		return c.JSON(fh.Map{"service": "orders", "accepted": true, "gateway_request_id": c.Locals("request_id")})
	})
	app.Get("/health", func(c *fh.Ctx) error { return c.SendString("ok") })
	app.Listen(*addr)
}
