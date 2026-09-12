package idempotency

import (
	"testing"

	"github.com/oarkflow/fh"
)

func TestNewWithNilKey(t *testing.T) {
	app := fh.New()
	app.Use(New(nil))
	app.Post("/test", func(c fh.Ctx) error {
		return c.SendString("ok")
	})
}

func TestNewSetsIdempotencyKey(t *testing.T) {
	app := fh.New()
	app.Use(New(func(c fh.Ctx) string { return "test-key-123" }))
	app.Post("/test", func(c fh.Ctx) error {
		v := c.RequestHeader().Get(fh.HeaderIdempotencyKey)
		if v != "test-key-123" {
			t.Errorf("expected idempotency key %q, got %q", "test-key-123", v)
		}
		return c.SendString("ok")
	})
}

func TestNewSkipsEmptyKeyValue(t *testing.T) {
	app := fh.New()
	app.Use(New(func(c fh.Ctx) string { return "" }))
	app.Post("/test", func(c fh.Ctx) error {
		v := c.RequestHeader().Get(fh.HeaderIdempotencyKey)
		if v != "" {
			t.Errorf("expected empty idempotency key, got %q", v)
		}
		return c.SendString("ok")
	})
}
