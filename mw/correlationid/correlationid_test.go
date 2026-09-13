package correlationid

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/requestid"
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

// TestDefaultTrustsIncomingHeader documents the package's actual trust model:
// unlike mw/requestid (which defaults to distrust), correlationid.New() with
// no Config defaults TrustIncoming to true, so a well-formed client-supplied
// correlation ID is honored rather than replaced. This test pins that
// documented behavior so a future change can't silently flip the default
// without a test failing.
func TestDefaultTrustsIncomingHeader(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals("correlationID").(string))
	})
	addr := testServer(t, app)

	const clientID = "trace-abc123.DEF_456:789"
	status, headers, body := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Correlation-ID": clientID,
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-correlation-id"] != clientID {
		t.Fatalf("x-correlation-id header = %q, want client-supplied %q (default TrustIncoming should be true)", headers["x-correlation-id"], clientID)
	}
	if body != clientID {
		t.Fatalf("locals value = %q, want %q", body, clientID)
	}
}

// TestDefaultGeneratesWhenHeaderMissing proves that even with the
// trust-by-default behavior, a request with no incoming correlation id still
// gets a freshly generated one.
func TestDefaultGeneratesWhenHeaderMissing(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	got := headers["x-correlation-id"]
	if got == "" {
		t.Fatal("expected a generated correlation id when none was supplied")
	}
	if !requestid.DefaultValidator(got) {
		t.Fatalf("generated correlation id %q does not pass the default validator", got)
	}
}

// TestExplicitTrustIncomingFalseDisablesTrust is the regression test for a
// real bug: the original merge logic was
//
//	if o.TrustIncoming { cfg.TrustIncoming = true }
//
// Since the baseline default is already true, this line could only ever
// force TrustIncoming to true and could never honor an explicit
// Config{TrustIncoming: false} — the field was effectively a dead write for
// the "false" case, so a caller who deliberately opted out of trusting
// client-supplied correlation IDs kept trusting them anyway. New() now
// assigns cfg.TrustIncoming = o.TrustIncoming unconditionally so an explicit
// false is honored.
func TestExplicitTrustIncomingFalseDisablesTrust(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{TrustIncoming: false}))
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals("correlationID").(string))
	})
	addr := testServer(t, app)

	const attackerSupplied = "attacker-supplied-correlation-id"
	status, headers, body := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Correlation-ID": attackerSupplied,
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	got := headers["x-correlation-id"]
	if got == attackerSupplied {
		t.Fatalf("Config{TrustIncoming: false} did not disable trust: client-supplied id %q was echoed back", attackerSupplied)
	}
	if got == "" {
		t.Fatal("expected a generated correlation id when trust is disabled")
	}
	if body != got {
		t.Fatalf("locals value %q does not match response header %q", body, got)
	}
}

// TestExplicitTrustIncomingTrueStillWorks proves the fix did not break the
// opt-in path: explicitly setting TrustIncoming: true alongside other fields
// must still honor a valid incoming id.
func TestExplicitTrustIncomingTrueStillWorks(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true, Header: "X-Correlation-ID"}))
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals("correlationID").(string))
	})
	addr := testServer(t, app)

	const clientID = "abc-123"
	status, headers, _ := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Correlation-ID": clientID,
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-correlation-id"] != clientID {
		t.Fatalf("x-correlation-id = %q, want %q", headers["x-correlation-id"], clientID)
	}
}

// TestOversizedIncomingIDRejected proves an incoming id larger than
// MaxIncomingLength is rejected outright rather than silently truncated or
// passed through, guarding against unbounded attacker-controlled header
// values being stored/echoed/logged downstream.
func TestOversizedIncomingIDRejected(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true, MaxIncomingLength: 8}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Correlation-ID": "this-correlation-id-is-way-too-long",
	})
	if status != fh.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}

// TestInvalidIncomingIDRejected proves a header-injection-style incoming
// value is rejected by the default validator rather than being echoed back
// into the response header or logs.
func TestInvalidIncomingIDRejected(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{TrustIncoming: true}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, _, _ := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Correlation-ID": "bad id; rm -rf",
	})
	if status != fh.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}

// TestPartialConfigPreservesHeaderAndLocalKeyDefaults proves overriding one
// field (Header) does not silently reset unrelated defaults (LocalKey,
// Generator, Validator, MaxIncomingLength) to their zero values.
func TestPartialConfigPreservesHeaderAndLocalKeyDefaults(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Header: "X-Trace-ID", TrustIncoming: true}))
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString(c.Locals("correlationID").(string))
	})
	addr := testServer(t, app)

	status, headers, body := doRequest(t, addr, "GET", "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-trace-id"] == "" {
		t.Fatal("expected custom header X-Trace-ID to be used")
	}
	if body != headers["x-trace-id"] {
		t.Fatalf("locals value %q should match response header %q (default LocalKey should still be correlationID)", body, headers["x-trace-id"])
	}
}

// TestCustomGeneratorIsUsedWhenHeaderMissing proves a caller-supplied
// Generator fully replaces the default AtomicGenerator instead of being
// ignored.
func TestCustomGeneratorIsUsedWhenHeaderMissing(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Generator: staticGenerator{id: "fixed-id"}}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	status, headers, _ := doRequest(t, addr, "GET", "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-correlation-id"] != "fixed-id" {
		t.Fatalf("x-correlation-id = %q, want custom generator output %q", headers["x-correlation-id"], "fixed-id")
	}
}

// TestCustomValidatorReplacesDefault proves a caller-supplied Validator is
// consulted instead of requestid.DefaultValidator: a value the default
// validator would reject (contains a space) is accepted here because the
// custom validator permits anything.
func TestCustomValidatorReplacesDefault(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		TrustIncoming: true,
		Validator:     func(string) bool { return true },
	}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	const unusual = "has space"
	if requestid.DefaultValidator(unusual) {
		t.Fatalf("test fixture invalid: %q must be rejected by the default validator for this test to be meaningful", unusual)
	}
	status, headers, _ := doRequest(t, addr, "GET", "/", map[string]string{
		"X-Correlation-ID": unusual,
	})
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if headers["x-correlation-id"] != unusual {
		t.Fatalf("x-correlation-id = %q, want custom-validator-accepted %q", headers["x-correlation-id"], unusual)
	}
}

type staticGenerator struct{ id string }

func (g staticGenerator) Generate(fh.Ctx) string { return g.id }

// TestConcurrentRequestsGetDistinctGeneratedIDs exercises the full middleware
// under concurrent HTTP load with no client-supplied header and checks every
// response carries a unique correlation id — a regression here would signal
// either a data race in the default generator or accidental ID reuse across
// requests.
func TestConcurrentRequestsGetDistinctGeneratedIDs(t *testing.T) {
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
			ids[i] = headers["x-correlation-id"]
		}(i)
	}
	wg.Wait()

	seen := make(map[string]struct{}, n)
	for _, id := range ids {
		if id == "" {
			t.Fatal("a response was missing its X-Correlation-ID header")
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate correlation id observed across concurrent requests: %q", id)
		}
		seen[id] = struct{}{}
	}
}
