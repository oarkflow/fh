package trailingslash

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

func TestRemoveSlashRedirect(t *testing.T) {
	app := fh.New()
	app.Use(RemoveSlash(true))
	app.Get("/users", func(c fh.Ctx) error {
		return c.SendString("users")
	})

	addr := testServer(t, app)

	// Trailing slash -> redirect to non-trailing slash with query preserved
	resp := sendRequest(t, addr, "GET /users/?sort=asc HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "301 Moved Permanently") {
		t.Fatalf("expected 301 redirect, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Location: /users?sort=asc") {
		t.Fatalf("expected Location /users?sort=asc, got:\n%s", resp)
	}

	// Exact match -> 200 OK
	respOK := sendRequest(t, addr, "GET /users HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(respOK, "200 OK") {
		t.Fatalf("expected 200 OK, got:\n%s", respOK)
	}
}

func TestRemoveSlashRewrite(t *testing.T) {
	app := fh.New()
	app.Use(RemoveSlash(false)) // internal rewrite
	app.Get("/users", func(c fh.Ctx) error {
		return c.SendString("users")
	})

	addr := testServer(t, app)

	// Trailing slash -> internally rewritten and handled by /users
	resp := sendRequest(t, addr, "GET /users/ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "200 OK") {
		t.Fatalf("expected 200 OK via rewrite, got:\n%s", resp)
	}
	if !strings.Contains(resp, "users") {
		t.Fatalf("expected body 'users', got:\n%s", resp)
	}
}

func TestAddSlashRedirect(t *testing.T) {
	app := fh.New()
	app.Use(AddSlash(true))
	app.Get("/dashboard", func(c fh.Ctx) error {
		return c.SendString("dashboard")
	})

	addr := testServer(t, app)

	// Non-trailing slash -> redirect to trailing slash
	resp := sendRequest(t, addr, "GET /dashboard HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "301 Moved Permanently") {
		t.Fatalf("expected 301 redirect, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Location: /dashboard/") {
		t.Fatalf("expected Location /dashboard/, got:\n%s", resp)
	}
}

func TestRootUntouched(t *testing.T) {
	app := fh.New()
	app.Use(RemoveSlash(true))
	app.Get("/", func(c fh.Ctx) error {
		return c.SendString("root")
	})

	addr := testServer(t, app)

	resp := sendRequest(t, addr, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "200 OK") {
		t.Fatalf("expected 200 OK for root, got:\n%s", resp)
	}
}
