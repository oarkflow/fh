package smartcache

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

// TestAuthenticatedResponsesAreNeverSharedFromCache proves an authenticated
// caller's personalized response is not cached/served to a different,
// unauthenticated caller on the same path.
func TestAuthenticatedResponsesAreNeverSharedFromCache(t *testing.T) {
	var calls int
	app := fh.New()
	app.Use(New())
	app.Get("/me", func(c fh.Ctx) error {
		calls++
		token := c.Get("Authorization")
		return c.SendString("profile-for:" + token)
	})
	addr := testServer(t, app)

	_, bodyA := doRequest(t, addr, "GET", "/me", map[string]string{"Authorization": "Bearer victim-token"})
	if !strings.Contains(bodyA, "victim-token") {
		t.Fatalf("unexpected body: %s", bodyA)
	}

	_, bodyB := doRequest(t, addr, "GET", "/me", map[string]string{"Authorization": "Bearer attacker-token"})
	if strings.Contains(bodyB, "victim-token") {
		t.Fatalf("attacker received victim's cached authenticated response: %s", bodyB)
	}
	if !strings.Contains(bodyB, "attacker-token") {
		t.Fatalf("expected attacker to get their own response, got: %s", bodyB)
	}
	if calls != 2 {
		t.Fatalf("expected handler to run for both distinct callers, ran %d times", calls)
	}
}

// TestQueryStringIsPartOfCacheKey proves two requests to the same path that
// differ only by query string are not treated as the same cache entry.
func TestQueryStringIsPartOfCacheKey(t *testing.T) {
	app := fh.New()
	app.Use(New())
	app.Get("/item", func(c fh.Ctx) error {
		return c.SendString("item:" + c.Query("id"))
	})
	addr := testServer(t, app)

	_, body1 := doRequest(t, addr, "GET", "/item?id=1", nil)
	if body1 != "item:1" {
		t.Fatalf("expected item:1, got %s", body1)
	}
	_, body2 := doRequest(t, addr, "GET", "/item?id=2", nil)
	if body2 != "item:2" {
		t.Fatalf("expected distinct query string to bypass cache and return item:2, got %s", body2)
	}
}

// TestResponseSetCookieIsNeverCached proves a response that sets a cookie
// (e.g. a fresh session) is never stored in the shared cache.
func TestResponseSetCookieIsNeverCached(t *testing.T) {
	var calls int
	app := fh.New()
	app.Use(New())
	app.Get("/login", func(c fh.Ctx) error {
		calls++
		c.SetCookie(&fh.Cookie{Name: "session", Value: "s1"})
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/login", nil)
	doRequest(t, addr, "GET", "/login", nil)
	if calls != 2 {
		t.Fatalf("expected handler to run every time for a Set-Cookie response (never cached), ran %d times", calls)
	}
}

// TestResponsePrivateCacheControlIsNotCached proves a handler marking its
// own response private/no-store is honored, closing the pre-existing bug
// where smartcache parsed the request's Cache-Control instead of the
// response's when deciding whether to store.
func TestResponsePrivateCacheControlIsNotCached(t *testing.T) {
	var calls int
	app := fh.New()
	app.Use(New())
	app.Get("/secret", func(c fh.Ctx) error {
		calls++
		c.Set("Cache-Control", "private")
		return c.SendString("secret")
	})
	addr := testServer(t, app)

	doRequest(t, addr, "GET", "/secret", nil)
	doRequest(t, addr, "GET", "/secret", nil)
	if calls != 2 {
		t.Fatalf("expected handler to run every time for a private response, ran %d times", calls)
	}
}

// TestVaryHeaderPreventsEncodingMismatch proves two requests differing only
// by a configured Vary header don't collide on the same cache entry.
func TestVaryHeaderPreventsEncodingMismatch(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{VaryHeaders: []string{"Accept-Encoding"}}))
	app.Get("/data", func(c fh.Ctx) error {
		return c.SendString("encoded-for:" + c.Get("Accept-Encoding"))
	})
	addr := testServer(t, app)

	_, bodyGzip := doRequest(t, addr, "GET", "/data", map[string]string{"Accept-Encoding": "gzip"})
	if bodyGzip != "encoded-for:gzip" {
		t.Fatalf("expected gzip variant, got %s", bodyGzip)
	}
	_, bodyPlain := doRequest(t, addr, "GET", "/data", nil)
	if bodyPlain != "encoded-for:" {
		t.Fatalf("expected distinct plain variant (not the cached gzip one), got %s", bodyPlain)
	}
}
