package policy

import (
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apiversion"
)

func TestNewWithDataPolicy(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		Data: fh.DataPolicy{Sensitivity: "pii", RedactLogs: true},
	}))
	app.Get("/test", func(c fh.Ctx) error {
		dp, ok := c.Locals("fh.data_policy").(fh.DataPolicy)
		if !ok {
			t.Fatal("expected DataPolicy in locals")
		}
		if dp.Sensitivity != "pii" {
			t.Errorf("expected sensitivity %q, got %q", "pii", dp.Sensitivity)
		}
		if !dp.RedactLogs {
			t.Error("expected RedactLogs to be true")
		}
		return c.SendString("ok")
	})
}

func TestNewWithVersionPolicy(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		Version: apiversion.Config{
			Supported: []string{"v1", "v2"},
			Default:   "v1",
		},
	}))
	app.Get("/test", func(c fh.Ctx) error {
		v, ok := c.Locals("api_version").(string)
		if !ok || v != "v1" {
			t.Errorf("expected api_version %q, got %v", "v1", v)
		}
		return c.SendString("ok")
	})
}

func TestNewWithEmptySensitivitySkipsLocal(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{}))
	app.Get("/test", func(c fh.Ctx) error {
		dp := c.Locals("fh.data_policy")
		if dp != nil {
			t.Errorf("expected nil DataPolicy for empty sensitivity, got %v", dp)
		}
		return c.SendString("ok")
	})
}
