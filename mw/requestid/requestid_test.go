package requestid

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

// doRequest issues a raw HTTP/1.1 request and returns the status code, the
// response headers (lower-cased keys), and the body.
func doRequest(t *testing.T, addr, method, path string, headers map[string]string) (statusCode int, respHeaders map[string]string, body string) {
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

	respHeaders = make(map[string]string)
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		respHeaders[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}

	return
}

// TestDefaultConfigIgnoresClientSuppliedRequestID is the core trust-boundary
// check: TrustIncoming defaults to false, so a client-supplied X-Request-ID
// must never be echoed back or stored in Locals. If this middleware trusted
// client input by default, an attacker could forge a request ID that
// collides with, or spoofs, another request in logs/traces/downstream
// systems.
func TestDefaultConfigIgnoresClientSuppliedRequestID(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals(DefaultLocalKey).(string))
	})
	addr := testServer(t, app)

	const attackerSupplied = "attacker-supplied-id"
	status, headers, body := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Request-Id": attackerSupplied,
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	got := headers["x-request-id"]
	if got == "" {
		t.Fatal("expected X-Request-ID response header to be set")
	}
	if got == attackerSupplied {
		t.Fatalf("middleware trusted client-supplied request id %q by default; TrustIncoming must default to false", attackerSupplied)
	}
	if body != got {
		t.Fatalf("locals value %q does not match response header %q", body, got)
	}
}

// TestTrustIncomingAcceptsValidClientID proves that when a caller explicitly
// opts in via TrustIncoming, a well-formed incoming id is passed through
// unchanged rather than being replaced by a freshly generated one.
func TestTrustIncomingAcceptsValidClientID(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true}))
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals(DefaultLocalKey).(string))
	})
	addr := testServer(t, app)

	const clientID = "trace-abc123.DEF_456:789"
	status, headers, body := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Request-Id": clientID,
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-request-id"] != clientID {
		t.Fatalf("x-request-id header = %q, want %q", headers["x-request-id"], clientID)
	}
	if body != clientID {
		t.Fatalf("locals value = %q, want %q", body, clientID)
	}
}

// TestTrustIncomingRejectsInvalidCharacters proves a malformed/malicious
// incoming id (here, one that attempts a header-injection-style payload) is
// rejected by the default validator and the request never reaches the
// downstream handler.
func TestTrustIncomingRejectsInvalidCharacters(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true}))
	app.Get("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, _, body := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Request-Id": "bad id; rm -rf",
	})
	if status != fh.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%q", status, body)
	}
	if calls.Load() != 0 {
		t.Fatalf("downstream handler was invoked %d times for a rejected request id", calls.Load())
	}
}

// TestTrustIncomingRejectsOversizedID proves an incoming id larger than
// MaxIncomingLength is rejected outright, guarding against unbounded
// attacker-controlled header values being stored/echoed/logged.
func TestTrustIncomingRejectsOversizedID(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true, MaxIncomingLength: 8}))
	app.Get("/", func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Request-Id": "this-id-is-way-too-long-for-the-configured-max",
	})
	if status != fh.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
	if calls.Load() != 0 {
		t.Fatalf("downstream handler was invoked for an oversized request id")
	}
}

// TestTrustIncomingGeneratesWhenHeaderMissing proves that even with
// TrustIncoming enabled, a request with no incoming id still gets a
// generated one rather than an empty value.
func TestTrustIncomingGeneratesWhenHeaderMissing(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-request-id"] == "" {
		t.Fatal("expected a generated request id when none was supplied")
	}
	if !DefaultValidator(headers["x-request-id"]) {
		t.Fatalf("generated request id %q does not pass the default validator", headers["x-request-id"])
	}
}

// TestCustomErrorHandlerReceivesInvalidRequestIDError proves the configured
// ErrorHandler is invoked with ErrInvalidRequestID (not some other error)
// so callers can rely on errors.Is(err, ErrInvalidRequestID) in custom
// handlers.
func TestCustomErrorHandlerReceivesInvalidRequestIDError(t *testing.T) {
	var gotErr error
	app := fh.New()
	app.Use(New(Config{
		TrustIncoming: true,
		ErrorHandler: func(c fh.Ctx, err error) error {
			gotErr = err
			return c.Status(fh.StatusTeapot).SendString("nope")
		},
	}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{"X-Request-Id": "bad id"})
	if status != fh.StatusTeapot {
		t.Fatalf("status = %d, want custom handler's status", status)
	}
	if gotErr != ErrInvalidRequestID {
		t.Fatalf("error handler received %v, want ErrInvalidRequestID", gotErr)
	}
}

// TestDefaultValidatorRejectsControlAndSpecialChars covers the allowlist
// invariant the whole trust-boundary relies on: only safe header-token
// characters pass, everything else (whitespace, CRLF-injection payloads,
// empty string) is rejected.
func TestDefaultValidatorRejectsControlAndSpecialChars(t *testing.T) {
	valid := []string{
		"abc123",
		"trace-id_1.2:3",
		"ABCDEF",
	}
	for _, id := range valid {
		if !DefaultValidator(id) {
			t.Errorf("DefaultValidator(%q) = false, want true", id)
		}
	}

	invalid := []string{
		"",
		"has space",
		"semi;colon",
		"id\r\nX-Injected: 1",
		"quote\"here",
		"slash/here",
		"emoji-😀",
	}
	for _, id := range invalid {
		if DefaultValidator(id) {
			t.Errorf("DefaultValidator(%q) = true, want false", id)
		}
	}
}

// TestAtomicGeneratorProducesUniqueIDsUnderConcurrency is the
// collision-resistance check: a request-id generator that is not safe for
// concurrent/high-throughput use could hand out duplicate ids, which breaks
// log correlation and can be used to make two distinct requests
// indistinguishable in an audit trail.
func TestAtomicGeneratorProducesUniqueIDsUnderConcurrency(t *testing.T) {
	g := NewAtomicGenerator()

	const goroutines = 50
	const perGoroutine = 200
	total := goroutines * perGoroutine

	ids := make(chan string, total)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				ids <- g.Generate(nil)
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, total)
	for id := range ids {
		if id == "" {
			t.Fatal("generator produced an empty id")
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("generator produced a duplicate id: %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("got %d unique ids, want %d", len(seen), total)
	}
}

// TestAtomicGeneratorOutputPassesDefaultValidator ensures generated ids are
// themselves valid according to the format the middleware enforces on
// incoming ids, so a mixed deployment (some nodes trusting incoming ids from
// others) never rejects an id this generator produced.
func TestAtomicGeneratorOutputPassesDefaultValidator(t *testing.T) {
	g := NewAtomicGenerator()
	for i := 0; i < 20; i++ {
		id := g.Generate(nil)
		if !DefaultValidator(id) {
			t.Fatalf("generated id %q failed DefaultValidator", id)
		}
		if len(id) > defaultMaxIncomingLen {
			t.Fatalf("generated id %q exceeds default max incoming length", id)
		}
	}
}

// TestNewAtomicGeneratorWithPrefixSanitizesInput proves an attacker- or
// misconfiguration-supplied prefix containing header-injection-style or
// otherwise unsafe characters can never leak into the generated id's
// prefix segment.
func TestNewAtomicGeneratorWithPrefixSanitizesInput(t *testing.T) {
	g := NewAtomicGeneratorWithPrefix("node\r\n/1: bad")
	id := g.Generate(nil)
	if !DefaultValidator(id) {
		t.Fatalf("id generated from unsanitized prefix failed validation: %q", id)
	}
	if strings.ContainsAny(id, "\r\n/ :") {
		t.Fatalf("id retained unsafe characters from prefix: %q", id)
	}
}

// TestSanitizePrefixFallsBackToNodeOnAllInvalidInput proves sanitizePrefix
// never returns an empty string, which would otherwise produce a malformed
// "-<unixnano>-<counter>" id with a leading separator.
func TestSanitizePrefixFallsBackToNodeOnAllInvalidInput(t *testing.T) {
	cases := map[string]string{
		"":        "node",
		"???":     "node",
		"a/b_c-D": "ab_c-D",
	}
	for in, want := range cases {
		if got := sanitizePrefix(in); got != want {
			t.Errorf("sanitizePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestPartialConfigPreservesDefaults proves that overriding only one field
// (here, LocalKey) does not silently reset unrelated defaults such as
// Header, Generator, or Validator back to zero values.
func TestPartialConfigPreservesDefaults(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{LocalKey: "custom_id_key"}))
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals("custom_id_key").(string))
	})
	addr := testServer(t, app)

	status, headers, body := doRequest(t, addr, "GET", "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-request-id"] == "" {
		t.Fatal("expected default Header (X-Request-Id) to still be used")
	}
	if body == "" {
		t.Fatal("expected custom local key to hold the generated id")
	}
	if headers["x-request-id"] != body {
		t.Fatalf("header %q and local %q should match", headers["x-request-id"], body)
	}
}

// TestConcurrentRequestsGetDistinctIDsEndToEnd exercises the full
// middleware (not just the generator in isolation) under concurrent HTTP
// load and checks every response carries a unique X-Request-ID.
func TestConcurrentRequestsGetDistinctIDsEndToEnd(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	const n = 40
	ids := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, headers, _ := doRequest(t, addr, "GET", "/", nil)
			ids[i] = headers["x-request-id"]
		}(i)
	}
	wg.Wait()

	seen := make(map[string]struct{}, n)
	for _, id := range ids {
		if id == "" {
			t.Fatal("a response was missing its X-Request-ID header")
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate request id observed across concurrent requests: %q", id)
		}
		seen[id] = struct{}{}
	}
}
