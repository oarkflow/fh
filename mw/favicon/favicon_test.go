package favicon

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

func TestFaviconDefaultNoContent(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/hello", func(c fh.Ctx) error {
		return c.SendString("world")
	})

	addr := testServer(t, app)

	// GET /favicon.ico -> 204 No Content
	resp := sendRequest(t, addr, "GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "204 No Content") {
		t.Fatalf("expected 204 No Content, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Cache-Control: public") {
		t.Fatalf("expected Cache-Control header, got:\n%s", resp)
	}

	// Normal route still works
	respHello := sendRequest(t, addr, "GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(respHello, "200 OK") {
		t.Fatalf("expected 200 OK, got:\n%s", respHello)
	}
}

func TestFaviconWithData(t *testing.T) {
	fakeIcon := []byte{0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x10, 0x10}

	app := fh.New()
	app.Use(New(Config{
		Data: fakeIcon,
	}))

	addr := testServer(t, app)

	// GET /favicon.ico -> 200 OK with ETag and body
	resp := sendRequest(t, addr, "GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "200 OK") {
		t.Fatalf("expected 200 OK, got:\n%s", resp)
	}
	if !strings.Contains(resp, "Content-Type: image/x-icon") {
		t.Fatalf("expected Content-Type image/x-icon, got:\n%s", resp)
	}
	if !strings.Contains(resp, "ETag:") {
		t.Fatalf("expected ETag header, got:\n%s", resp)
	}

	// Extract ETag
	var etag string
	for _, line := range strings.Split(resp, "\r\n") {
		if strings.HasPrefix(line, "ETag: ") {
			etag = strings.TrimPrefix(line, "ETag: ")
			break
		}
	}
	if etag == "" {
		t.Fatal("failed to extract ETag")
	}

	// Conditional GET with If-None-Match -> 304 Not Modified
	reqConditional := fmt.Sprintf("GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\nIf-None-Match: %s\r\nConnection: close\r\n\r\n", etag)
	resp304 := sendRequest(t, addr, reqConditional)
	if !strings.Contains(resp304, "304 Not Modified") {
		t.Fatalf("expected 304 Not Modified, got:\n%s", resp304)
	}

	// HEAD request -> 200 OK without body
	respHEAD := sendRequest(t, addr, "HEAD /favicon.ico HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(respHEAD, "200 OK") {
		t.Fatalf("expected 200 OK for HEAD, got:\n%s", respHEAD)
	}

	// POST request -> 405 Method Not Allowed
	respPOST := sendRequest(t, addr, "POST /favicon.ico HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	if !strings.Contains(respPOST, "405 Method Not Allowed") {
		t.Fatalf("expected 405 Method Not Allowed, got:\n%s", respPOST)
	}
}
