package redirect

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
	time.Sleep(15 * time.Millisecond)
	return ln.Addr().String()
}

func sendRequest(t *testing.T, addr, rawReq string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, rawReq)
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return string(resp)
}

func TestHTTPSRedirect(t *testing.T) {
	app := fh.New()
	app.Use(ToHTTPS(fh.StatusMovedPermanently))
	app.Get("/test", func(c fh.Ctx) error {
		return c.SendString("ok")
	})

	addr := testServer(t, app)

	// 1. Cleartext request -> redirects to https://
	resp := sendRequest(t, addr, "GET /test HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "301 Moved Permanently") {
		t.Fatalf("expected 301 redirect, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Location: https://example.com/test") {
		t.Fatalf("expected Location https://example.com/test, got:\n%s", resp)
	}

	// 2. Proxied HTTPS request (X-Forwarded-Proto: https) -> passes through
	respProto := sendRequest(t, addr, "GET /test HTTP/1.1\r\nHost: example.com\r\nX-Forwarded-Proto: https\r\nConnection: close\r\n\r\n")
	if !strings.Contains(respProto, "200 OK") {
		t.Fatalf("expected 200 OK for X-Forwarded-Proto: https, got:\n%s", respProto)
	}
}

func TestHostRedirect(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		HostRedirect: map[string]string{
			"www.example.com": "example.com",
		},
		Status: fh.StatusPermanentRedirect,
	}))
	app.Get("/dashboard", func(c fh.Ctx) error {
		return c.SendString("dashboard")
	})

	addr := testServer(t, app)

	// Request with www host
	resp := sendRequest(t, addr, "GET /dashboard HTTP/1.1\r\nHost: www.example.com\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "308 Permanent Redirect") {
		t.Fatalf("expected 308 redirect, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Location: http://example.com/dashboard") {
		t.Fatalf("expected Location http://example.com/dashboard, got:\n%s", resp)
	}

	// Request with apex host -> passes through
	respApex := sendRequest(t, addr, "GET /dashboard HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if !strings.Contains(respApex, "200 OK") {
		t.Fatalf("expected 200 OK, got:\n%s", respApex)
	}
}

func TestPathRulesRedirect(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		Rules: map[string]string{
			"/old-pricing": "/pricing",
		},
	}))
	app.Get("/pricing", func(c fh.Ctx) error {
		return c.SendString("pricing")
	})

	addr := testServer(t, app)

	resp := sendRequest(t, addr, "GET /old-pricing HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "301 Moved Permanently") {
		t.Fatalf("expected 301, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Location: /pricing") {
		t.Fatalf("expected Location /pricing, got:\n%s", resp)
	}
}

func TestNextSkip(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{
		HTTPSRedirect: true,
		Next: func(c fh.Ctx) bool {
			return c.Path() == "/health"
		},
	}))
	app.Get("/health", func(c fh.Ctx) error {
		return c.SendString("healthy")
	})

	addr := testServer(t, app)

	resp := sendRequest(t, addr, "GET /health HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "200 OK") {
		t.Fatalf("expected 200 OK for skipped path, got:\n%s", resp)
	}
}
