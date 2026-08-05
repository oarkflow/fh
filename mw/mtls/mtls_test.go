package mtls

import (
	"fmt"
	"io"
	"net"
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

func rawStatus(t *testing.T, addr string) int {
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
	var proto, status string
	fmt.Sscan(string(resp), &proto, &status)
	code := 0
	fmt.Sscan(status, &code)
	return code
}

// TestRequiredRejectsPlaintextConnection is the secure-default check: a
// plaintext (non-TLS) connection carries no peer certificates and no TLS
// state at all, so Required must fail closed rather than treating "no TLS
// state present" as "certificate check not applicable".
func TestRequiredRejectsPlaintextConnection(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Required: true}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	if code := rawStatus(t, addr); code != fh.StatusUnauthorized {
		t.Fatalf("expected 401 for a required-mTLS route with no client cert, got %d", code)
	}
}

// TestOptionalAllowsPlaintextConnection confirms Required:false still lets
// unauthenticated (non-mTLS) traffic through, so the middleware is opt-in
// per route rather than globally fail-closed.
func TestOptionalAllowsPlaintextConnection(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Required: false}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	if code := rawStatus(t, addr); code != fh.StatusOK {
		t.Fatalf("expected 200 when mTLS is optional and no cert is presented, got %d", code)
	}
}

func TestAllowedEmptyListPermitsAny(t *testing.T) {
	if !allowed("anything", nil) {
		t.Fatal("an empty allowlist should permit any subject/issuer")
	}
}

func TestAllowedIsCaseInsensitiveAndTrimmed(t *testing.T) {
	if !allowed("Example", []string{" example "}) {
		t.Fatal("expected case-insensitive, whitespace-tolerant match")
	}
	if allowed("evil.example.com", []string{"example.com"}) {
		t.Fatal("expected exact match only, not substring match")
	}
}
