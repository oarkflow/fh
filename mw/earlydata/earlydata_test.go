package earlydata

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

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

func doRequest(t *testing.T, addr, method, path string, headers map[string]string) (statusCode int, body string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\n", method, path)
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	req += "Connection: close\r\n\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	raw := string(resp)
	head, rest, found := strings.Cut(raw, "\r\n\r\n")
	if !found {
		head = raw
	} else {
		body = rest
	}
	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}
	return
}

// TestNonIdempotentMethodWithEarlyDataIsRejected is the core replay-safety
// check: a POST (a non-idempotent, unsafe method) flagged as TLS early data
// must be rejected with 425 Too Early rather than reaching the handler,
// since a replayed early-data POST could duplicate a side effect (e.g. a
// payment).
func TestNonIdempotentMethodWithEarlyDataIsRejected(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New())
	app.Post("/pay", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("paid")
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{"Early-Data": "1"})
	if status != fh.StatusTooEarly {
		t.Fatalf("status = %d, want 425 Too Early", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times for a rejected early-data POST", calls.Load())
	}
}

// TestIdempotentMethodWithEarlyDataIsAllowed proves the default AllowMethods
// set (GET, HEAD, OPTIONS, TRACE — the RFC 7231 "safe" methods) passes
// through even when flagged as early data, since replaying a safe method has
// no side effects.
func TestIdempotentMethodWithEarlyDataIsAllowed(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "GET", "/", map[string]string{"Early-Data": "1"})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 for a GET flagged as early data", status)
	}
}

// TestNoEarlyDataHeaderAlwaysPasses proves the middleware is a no-op for
// ordinary (non-early-data) traffic regardless of method: the protection is
// opt-in, triggered only by the Early-Data signal.
func TestNoEarlyDataHeaderAlwaysPasses(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/pay", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 for a normal POST with no Early-Data header", status)
	}
}

// TestEarlyDataHeaderValueMustBeExactlyOne proves only the RFC 8470 literal
// value "1" is treated as a live early-data signal; any other value is
// treated as absent (the header is not a generic boolean).
func TestEarlyDataHeaderValueMustBeExactlyOne(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	for _, v := range []string{"true", "0", "yes", ""} {
		headers := map[string]string{}
		if v != "" {
			headers["Early-Data"] = v
		}
		status, _ := doRequest(t, addr, "POST", "/pay", headers)
		if status != fh.StatusOK {
			t.Fatalf("Early-Data=%q: status = %d, want 200 (only the literal \"1\" should trigger protection)", v, status)
		}
	}
}

// TestAllowWithIdempotencyKeyBypassesBlockWhenKeyPresent proves the opt-in
// idempotency-key bypass only lets a non-idempotent early-data request
// through when both AllowWithIdempotencyKey is enabled and the caller
// actually supplied the configured header.
func TestAllowWithIdempotencyKeyBypassesBlockWhenKeyPresent(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{AllowWithIdempotencyKey: true}))
	app.Post("/pay", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("paid")
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{
		"Early-Data":      "1",
		"Idempotency-Key": "key-123",
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 when a valid idempotency key accompanies early data", status)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", calls.Load())
	}
}

// TestAllowWithIdempotencyKeyStillBlocksWithoutKey proves enabling the
// bypass option does not itself weaken protection: without the header
// actually present, the request is still rejected.
func TestAllowWithIdempotencyKeyStillBlocksWithoutKey(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{AllowWithIdempotencyKey: true}))
	app.Post("/pay", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("paid")
	})
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{"Early-Data": "1"})
	if status != fh.StatusTooEarly {
		t.Fatalf("status = %d, want 425 when no idempotency key is supplied", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times for a rejected request", calls.Load())
	}
}

// TestDefaultConfigDoesNotAllowIdempotencyKeyBypass proves
// AllowWithIdempotencyKey defaults to false: simply sending the idempotency
// header is not, by itself, enough to bypass protection unless the
// application explicitly opts in.
func TestDefaultConfigDoesNotAllowIdempotencyKeyBypass(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{
		"Early-Data":      "1",
		"Idempotency-Key": "key-123",
	})
	if status != fh.StatusTooEarly {
		t.Fatalf("status = %d, want 425 by default even with an idempotency key present", status)
	}
}

// TestCustomIdempotencyHeaderNameIsHonored proves IdempotencyHeader
// overrides the default header name used for the bypass check.
func TestCustomIdempotencyHeaderNameIsHonored(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{AllowWithIdempotencyKey: true, IdempotencyHeader: "X-Idem-Key"}))
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	// The default header name must no longer work once overridden.
	status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{
		"Early-Data":      "1",
		"Idempotency-Key": "key-123",
	})
	if status != fh.StatusTooEarly {
		t.Fatalf("status = %d, want 425 when the default header name is used but a custom one is configured", status)
	}

	status, _ = doRequest(t, addr, "POST", "/pay", map[string]string{
		"Early-Data": "1",
		"X-Idem-Key": "key-123",
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 when the configured custom header is used", status)
	}
}

// TestCustomAllowMethodsOverridesDefault proves AllowMethods, when set,
// fully replaces the default safe-method set rather than being merged with
// it — configuring a stricter (or looser) allowlist works as documented.
func TestCustomAllowMethodsOverridesDefault(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{AllowMethods: []string{"PUT"}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Put("/", func(c fh.Ctx) error { return c.SendString("put-ok") })
	addr := testServer(t, app)

	// GET is no longer in the allowlist, so it must now be blocked.
	if status, _ := doRequest(t, addr, "GET", "/", map[string]string{"Early-Data": "1"}); status != fh.StatusTooEarly {
		t.Fatalf("GET status = %d, want 425 once the default allowlist is overridden", status)
	}
	// PUT is now explicitly allowed.
	if status, _ := doRequest(t, addr, "PUT", "/", map[string]string{"Early-Data": "1"}); status != fh.StatusOK {
		t.Fatalf("PUT status = %d, want 200 once explicitly allow-listed", status)
	}
}

// TestNextHookBypassesEarlyDataCheckEntirely proves the escape hatch (Next)
// skips the check completely — including for an otherwise-blocked
// non-idempotent early-data request — when the caller opts a request out.
func TestNextHookBypassesEarlyDataCheckEntirely(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Next: func(c fh.Ctx) bool { return c.Path() == "/skip" }}))
	app.Post("/skip", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	if status, _ := doRequest(t, addr, "POST", "/skip", map[string]string{"Early-Data": "1"}); status != fh.StatusOK {
		t.Fatalf("/skip status = %d, want 200 (Next should bypass the check)", status)
	}
	if status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{"Early-Data": "1"}); status != fh.StatusTooEarly {
		t.Fatalf("/pay status = %d, want 425 (Next should not affect other paths)", status)
	}
}

// TestRejectionUsesStandardTooEarlyStatusAndCode proves the error surfaced to
// the client carries the standard 425 status and a stable machine-readable
// code, so clients/proxies can distinguish this from other 4xx errors and
// retry appropriately.
func TestRejectionUsesStandardTooEarlyStatusAndCode(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	status, body := doRequest(t, addr, "POST", "/pay", map[string]string{"Early-Data": "1"})
	if status != 425 {
		t.Fatalf("status = %d, want literal 425", status)
	}
	if !strings.Contains(body, "TOO_EARLY") {
		t.Fatalf("expected response body to contain the TOO_EARLY error code, got %q", body)
	}
}

// TestConcurrentEarlyDataRequestsAreConsistentlyHandled hammers the
// middleware with a concurrent mix of blocked and allowed requests and
// checks every single one gets the status its own method/header pair
// dictates, guarding against any shared-state race in the compiled allowed
// set (it is built once at New() time and must be read-only afterwards).
func TestConcurrentEarlyDataRequestsAreConsistentlyHandled(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Post("/pay", func(c fh.Ctx) error { return c.SendString("paid") })
	addr := testServer(t, app)

	const n = 30
	var wg sync.WaitGroup
	wg.Add(n * 2)
	errs := make(chan string, n*2)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if status, _ := doRequest(t, addr, "GET", "/", map[string]string{"Early-Data": "1"}); status != fh.StatusOK {
				errs <- fmt.Sprintf("GET early-data status = %d, want 200", status)
			}
		}()
		go func() {
			defer wg.Done()
			if status, _ := doRequest(t, addr, "POST", "/pay", map[string]string{"Early-Data": "1"}); status != fh.StatusTooEarly {
				errs <- fmt.Sprintf("POST early-data status = %d, want 425", status)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
