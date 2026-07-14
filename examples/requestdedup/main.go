package main

import (
	"fmt"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/requestdedup"
)

func main() {
	app := fh.New()

	dedup := requestdedup.New(requestdedup.Config{
		Window:  10 * time.Second,
		MaxKeys: 5000,
		OnDuplicate: func(c fh.Ctx, e *requestdedup.Entry) error {
			return c.Status(fh.StatusConflict).JSON(fh.Map{
				"error":     "duplicate_request",
				"key":       e.Key,
				"received":  e.ReceivedAt.Format(time.RFC3339Nano),
				"expires":   e.ExpiresAt.Format(time.RFC3339Nano),
			})
		},
	})

	// POST /payments uses dedup to prevent duplicate charges
	app.Post("/payments", dedup.Handler(), func(c fh.Ctx) error {
		// Simulate payment processing
		time.Sleep(100 * time.Millisecond)
		return c.JSON(fh.Map{
			"status": "success",
			"amount": 99.99,
		})
	})

	// POST /webhooks uses dedup to prevent webhook replay
	app.Post("/webhooks", dedup.Handler(), func(c fh.Ctx) error {
		return c.JSON(fh.Map{"status": "received"})
	})

	fmt.Println("Request dedup example on :3000")
	fmt.Println("  POST /payments   - idempotent within 10s window")
	fmt.Println("  POST /webhooks   - dedup webhook replays")
	app.Listen(":3000")
}
