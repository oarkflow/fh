package tenant

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

func doRequest(t *testing.T, addr, method, path string, headers map[string]string) (statusCode int, respBody string) {
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
	req += "\r\n"

	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	if len(parts) < 1 {
		t.Fatal("empty response")
	}
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	fmt.Sscan(status, &statusCode)

	idx := strings.Index(string(resp), "\r\n\r\n")
	if idx >= 0 {
		respBody = string(resp)[idx+4:]
	}
	return
}

func TestTenantResolvedFromHeader(t *testing.T) {
	var got string
	app := fh.New()
	app.Use(New(Config{}))
	app.Get("/*", func(c fh.Ctx) error {
		got = fh.TenantID(c)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/x", map[string]string{"X-Tenant-ID": "acme"})
	if got != "acme" {
		t.Fatalf("expected tenant 'acme', got %q", got)
	}
}

func TestTenantRequiredRejectsMissing(t *testing.T) {
	called := false
	app := fh.New()
	app.Use(New(Config{Required: true}))
	app.Get("/*", func(c fh.Ctx) error {
		called = true
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	code, _ := doRequest(t, addr, "GET", "/x", nil)
	if called {
		t.Fatal("handler should not run without a tenant")
	}
	if code != 400 {
		t.Fatalf("expected 400, got %d", code)
	}
}

func TestTenantValidateRejectsUnknownTenant(t *testing.T) {
	called := false
	app := fh.New()
	app.Use(New(Config{
		Validate: func(c fh.Ctx, tenant string) bool { return tenant == "known" },
	}))
	app.Get("/*", func(c fh.Ctx) error {
		called = true
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	code, _ := doRequest(t, addr, "GET", "/x", map[string]string{"X-Tenant-ID": "spoofed"})
	if called {
		t.Fatal("handler should not run for a tenant rejected by Validate")
	}
	if code != 403 {
		t.Fatalf("expected 403, got %d", code)
	}
}

func TestTenantValidateAllowsKnownTenant(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		Validate: func(c fh.Ctx, tenant string) bool { return tenant == "known" },
	}))
	app.Get("/*", func(c fh.Ctx) error {
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/x", map[string]string{"X-Tenant-ID": "known"})
	if code != 200 {
		t.Fatalf("expected 200, got %d: %s", code, body)
	}
}
