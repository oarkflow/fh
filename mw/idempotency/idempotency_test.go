package idempotency

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

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

// The tests below prove the README's central claim end-to-end: idempotency.New
// alone sets a header and does nothing else — actual deduplication only
// happens once fh.WithReliability's idempotency engine is enabled downstream.
// This exercises the real wiring (a real TCP listener, a real ReliabilityConfig
// with a file-backed store, a real duplicate POST) rather than asserting on
// the header in isolation, since that alone can't tell us the two features
// actually compose correctly.

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

func postJSON(t *testing.T, addr, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/orders", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-Client-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

// TestEndToEndWithReliabilityDeduplicates proves that idempotency.New's
// synthesized key is actually picked up by fh.WithReliability's auto-appended
// middleware: a repeated request with the same derived key and the same body
// replays the original response without re-running the handler.
func TestEndToEndWithReliabilityDeduplicates(t *testing.T) {
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:            true,
		IdempotencyEnabled: true,
		DataDir:            t.TempDir(),
	}))
	// Derive the idempotency key from an arbitrary client-chosen header
	// instead of the literal Idempotency-Key header, which is exactly the
	// case idempotency.New exists for.
	app.Use(New(func(c fh.Ctx) string { return c.Get("X-Client-Key") }))
	var calls int
	app.Post("/orders", func(c fh.Ctx) error {
		calls++
		return c.Status(fh.StatusCreated).JSON(fh.Map{"call": calls})
	})
	addr := testServer(t, app)

	code1, body1 := postJSON(t, addr, "order-42", `{"item":"widget"}`)
	if code1 != fh.StatusCreated {
		t.Fatalf("first request: expected 201, got %d: %s", code1, body1)
	}
	if calls != 1 {
		t.Fatalf("expected handler to run once, ran %d times", calls)
	}

	code2, body2 := postJSON(t, addr, "order-42", `{"item":"widget"}`)
	if code2 != fh.StatusCreated {
		t.Fatalf("replay: expected 201, got %d: %s", code2, body2)
	}
	if body1 != body2 {
		t.Fatalf("expected replay to return the cached response, got %q vs %q", body1, body2)
	}
	if calls != 1 {
		t.Fatalf("expected handler to run exactly once across the duplicate request, ran %d times", calls)
	}
}

// TestEndToEndConflictOnReusedKeyDifferentBody proves the "different
// operation under the same key gets rejected, not silently executed" claim.
func TestEndToEndConflictOnReusedKeyDifferentBody(t *testing.T) {
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:            true,
		IdempotencyEnabled: true,
		DataDir:            t.TempDir(),
	}))
	app.Use(New(func(c fh.Ctx) string { return c.Get("X-Client-Key") }))
	app.Post("/orders", func(c fh.Ctx) error {
		return c.Status(fh.StatusCreated).JSON(fh.Map{"ok": true})
	})
	addr := testServer(t, app)

	if code, body := postJSON(t, addr, "order-99", `{"item":"widget"}`); code != fh.StatusCreated {
		t.Fatalf("first request: expected 201, got %d: %s", code, body)
	}
	code, body := postJSON(t, addr, "order-99", `{"item":"different-item"}`)
	if code != fh.StatusConflict {
		t.Fatalf("expected 409 on reused key with a different body, got %d: %s", code, body)
	}
}

// TestEndToEndWithoutIdempotencyNewStillDedupsOnLiteralHeader proves the
// README's other claim: a client that already sends the literal
// Idempotency-Key header gets deduplicated by fh.WithReliability alone,
// without idempotency.New in the chain at all.
func TestEndToEndWithoutIdempotencyNewStillDedupsOnLiteralHeader(t *testing.T) {
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:            true,
		IdempotencyEnabled: true,
		DataDir:            t.TempDir(),
	}))
	var calls int
	app.Post("/orders", func(c fh.Ctx) error {
		calls++
		return c.Status(fh.StatusCreated).JSON(fh.Map{"call": calls})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	addr := ln.Addr().String()

	req := func() *http.Request {
		r, err := http.NewRequest(http.MethodPost, "http://"+addr+"/orders", bytes.NewBufferString(`{"item":"widget"}`))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(fh.HeaderIdempotencyKey, "literal-key-1")
		return r
	}
	resp1, err := http.DefaultClient.Do(req())
	if err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	resp2, err := http.DefaultClient.Do(req())
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if calls != 1 {
		t.Fatalf("expected handler to run exactly once, ran %d times", calls)
	}
}
