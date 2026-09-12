package requesthash

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/oarkflow/fh"
)

func TestNewSetsDefaults(t *testing.T) {
	handler := New(Config{})
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewWithCustomConfig(t *testing.T) {
	handler := New(Config{
		Header:    "X-Custom-Hash",
		LocalKey:  "custom_hash",
		SkipEmpty: true,
	})
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestDefaultConstants(t *testing.T) {
	if DefaultHeader != "X-Request-Body-SHA256" {
		t.Errorf("DefaultHeader = %q", DefaultHeader)
	}
	if DefaultLocal != "request_body_sha256" {
		t.Errorf("DefaultLocal = %q", DefaultLocal)
	}
}

func TestHashComputesSHA256(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	expected := sha256.Sum256(body)
	expectedHex := hex.EncodeToString(expected[:])

	app := fh.New()
	app.Use(New(Config{}))
	app.Post("/test", func(c fh.Ctx) error {
		digest := c.Locals(DefaultLocal)
		if digest != expectedHex {
			t.Errorf("expected digest %q, got %v", expectedHex, digest)
		}
		hdr := c.Get(DefaultHeader)
		if hdr != expectedHex {
			t.Errorf("expected header %q, got %q", expectedHex, hdr)
		}
		return c.SendString("ok")
	})
}
