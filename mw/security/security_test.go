package security

import (
	"fmt"
	"io"
	"net"
	"strings"
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

func rawGet(t *testing.T, addr string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return string(resp)
}

// TestDefaultHeadersAreSecureByDefault ensures the zero-value Config (as used
// by New() with no arguments) still emits the standard hardening header set,
// so a caller who forgets to configure the middleware does not silently ship
// without clickjacking/MIME-sniffing/referrer protections.
func TestDefaultHeadersAreSecureByDefault(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	resp := rawGet(t, addr)
	for _, want := range []string{
		"X-Frame-Options: DENY",
		"X-Content-Type-Options: nosniff",
		"Referrer-Policy: no-referrer",
		"Cross-Origin-Opener-Policy: same-origin",
		"Cross-Origin-Resource-Policy: same-origin",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("expected response to contain %q, got:\n%s", want, resp)
		}
	}
}

// TestCSPNonceIsUniquePerRequest guards against a nonce implementation that
// reuses or predicts values, which would let an attacker who observes one
// response's nonce bypass the CSP on a different response.
func TestCSPNonceIsUniquePerRequest(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{CSPNonce: true, ContentSecurityPolicy: "default-src 'self'; script-src 'self'"}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	first := extractNonce(t, rawGet(t, addr))
	second := extractNonce(t, rawGet(t, addr))
	if first == "" || second == "" {
		t.Fatal("expected a nonce in both responses")
	}
	if first == second {
		t.Fatalf("expected distinct nonces per request, got %q twice", first)
	}
}

func extractNonce(t *testing.T, resp string) string {
	t.Helper()
	idx := strings.Index(resp, "'nonce-")
	if idx < 0 {
		return ""
	}
	rest := resp[idx+len("'nonce-"):]
	end := strings.IndexByte(rest, '\'')
	if end < 0 {
		return ""
	}
	return rest[:end]
}
