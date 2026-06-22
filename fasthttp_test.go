package fh_test

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/compress"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/mw/timeout"
)

// testServer starts the app on a random port and returns the address.
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

func doRequest(t *testing.T, addr, method, path, body string, headers map[string]string) (statusCode int, respBody string) {
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
	if body != "" {
		req += fmt.Sprintf("Content-Length: %d\r\n", len(body))
	}
	req += "\r\n" + body

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
	statusLine := parts[0]
	var proto, status string
	fmt.Sscan(statusLine, &proto, &status)
	fmt.Sscan(status, &statusCode)

	// parse body (after \r\n\r\n)
	idx := strings.Index(string(resp), "\r\n\r\n")
	if idx >= 0 {
		respBody = string(resp)[idx+4:]
	}
	return
}

// ── Tests ──────────────────────────────────────────────────────────────────

func TestGetRoute(t *testing.T) {
	app := fh.New()
	app.Get("/hello", func(ctx *fh.Ctx) error {
		return ctx.SendString("hello world")
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/hello", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "hello world" {
		t.Fatalf("expected 'hello world', got %q", body)
	}
}

func TestNotFound(t *testing.T) {
	app := fh.New()
	addr := testServer(t, app)
	code, _ := doRequest(t, addr, "GET", "/missing", "", nil)
	if code != 404 {
		t.Fatalf("expected 404, got %d", code)
	}
}

func TestRouteParams(t *testing.T) {
	app := fh.New()
	app.Get("/users/:id", func(ctx *fh.Ctx) error {
		return ctx.SendString(ctx.Param("id"))
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/users/42", "", nil)
	if code != 200 || body != "42" {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestNestedParams(t *testing.T) {
	app := fh.New()
	app.Get("/a/:x/b/:y", func(ctx *fh.Ctx) error {
		return ctx.SendString(ctx.Param("x") + "-" + ctx.Param("y"))
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/a/foo/b/bar", "", nil)
	if code != 200 || body != "foo-bar" {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestWildcard(t *testing.T) {
	app := fh.New()
	app.Get("/static/*", func(ctx *fh.Ctx) error {
		return ctx.SendString(ctx.Param("*"))
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/static/js/app.js", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d %q", code, body)
	}
}

func TestPostJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	app := fh.New()
	app.Post("/echo", func(ctx *fh.Ctx) error {
		var p payload
		if err := ctx.BodyParser(&p); err != nil {
			return ctx.Status(400).SendString(err.Error())
		}
		return ctx.JSON(p)
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "POST", "/echo", `{"name":"alice"}`, map[string]string{
		"Content-Type": "application/json",
	})
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if !strings.Contains(body, "alice") {
		t.Fatalf("expected alice in body, got %q", body)
	}
}

func TestQueryParams(t *testing.T) {
	app := fh.New()
	app.Get("/search", func(ctx *fh.Ctx) error {
		return ctx.SendString(ctx.Query("q"))
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/search?q=golang", "", nil)
	if code != 200 || body != "golang" {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestMiddlewareChain(t *testing.T) {
	app := fh.New()
	var order []string
	done := make(chan struct{})

	app.Use(func(ctx *fh.Ctx) error {
		order = append(order, "mw1")
		err := ctx.Next()
		order = append(order, "mw1-after")
		close(done)
		return err
	})

	app.Use(func(ctx *fh.Ctx) error {
		order = append(order, "mw2")
		return ctx.Next()
	})

	app.Get("/", func(ctx *fh.Ctx) error {
		order = append(order, "handler")
		return ctx.SendString("ok")
	})

	addr := testServer(t, app)
	doRequest(t, addr, "GET", "/", "", nil)
	<-done

	expected := []string{"mw1", "mw2", "handler", "mw1-after"}
	for i, v := range expected {
		if i >= len(order) || order[i] != v {
			t.Fatalf("middleware order wrong: got %v expected %v", order, expected)
		}
	}
}

func TestGroupRoutes(t *testing.T) {
	app := fh.New()
	v1 := app.Group("/v1")
	v1.Get("/ping", func(ctx *fh.Ctx) error {
		return ctx.SendString("pong")
	})

	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/v1/ping", "", nil)
	if code != 200 || body != "pong" {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestGroupMiddleware(t *testing.T) {
	app := fh.New()
	var called atomic.Bool

	admin := app.Group("/admin", func(ctx *fh.Ctx) error {
		called.Store(true)
		return ctx.Next()
	})
	admin.Get("/secret", func(ctx *fh.Ctx) error {
		return ctx.SendString("secret")
	})

	addr := testServer(t, app)
	doRequest(t, addr, "GET", "/admin/secret", "", nil)
	if !called.Load() {
		t.Fatal("group middleware not called")
	}
}

func TestKeepAlive(t *testing.T) {
	app := fh.New()
	app.Get("/ka", func(ctx *fh.Ctx) error {
		return ctx.SendString("ok")
	})
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send two requests on the same connection
	for i := 0; i < 2; i++ {
		req := "GET /ka HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n"
		conn.Write([]byte(req))

		conn.SetDeadline(time.Now().Add(2 * time.Second))
		reader := bufio.NewReader(conn)
		line, _ := reader.ReadString('\n')
		if !strings.HasPrefix(line, "HTTP/1.1 200") {
			t.Fatalf("req %d: expected 200, got %q", i+1, line)
		}
	}
}

func TestStatusCodes(t *testing.T) {
	app := fh.New()
	app.Get("/created", func(ctx *fh.Ctx) error {
		return ctx.Status(201).SendString("created")
	})
	app.Get("/redirect", func(ctx *fh.Ctx) error {
		return ctx.Redirect("/other", 301)
	})

	addr := testServer(t, app)

	code, _ := doRequest(t, addr, "GET", "/created", "", nil)
	if code != 201 {
		t.Fatalf("expected 201, got %d", code)
	}

	code, _ = doRequest(t, addr, "GET", "/redirect", "", nil)
	if code != 301 {
		t.Fatalf("expected 301, got %d", code)
	}
}

func TestLocals(t *testing.T) {
	app := fh.New()
	app.Use(func(ctx *fh.Ctx) error {
		ctx.Locals("user", "alice")
		return ctx.Next()
	})
	app.Get("/whoami", func(ctx *fh.Ctx) error {
		return ctx.SendString(ctx.Locals("user").(string))
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/whoami", "", nil)
	if code != 200 || body != "alice" {
		t.Fatalf("got %d %q", code, body)
	}
}

func TestPanicRecovery(t *testing.T) {
	app := fh.New()
	app.Use(recover.New())
	app.Get("/panic", func(ctx *fh.Ctx) error {
		panic("test panic")
	})
	addr := testServer(t, app)
	code, _ := doRequest(t, addr, "GET", "/panic", "", nil)
	if code != 500 {
		t.Fatalf("expected 500, got %d", code)
	}
}

func TestSecurityHeaders(t *testing.T) {
	app := fh.New()
	app.Use(security.New())
	app.Get("/", func(ctx *fh.Ctx) error {
		return ctx.SendString("ok")
	})
	addr := testServer(t, app)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	resp, _ := io.ReadAll(conn)
	s := string(resp)

	checks := []struct {
		name   string
		header string
	}{
		{"X-Content-Type-Options", "X-Content-Type-Options: nosniff"},
		{"X-Frame-Options", "X-Frame-Options: DENY"},
		{"X-XSS-Protection", "X-XSS-Protection: 0"},
		{"Referrer-Policy", "Referrer-Policy: no-referrer"},
		{"Strict-Transport-Security", "Strict-Transport-Security: max-age=31536000; includeSubDomains"},
		{"Permissions-Policy", "Permissions-Policy:"},
	}
	for _, c := range checks {
		if !strings.Contains(s, c.header) {
			t.Errorf("missing response header %q", c.name)
		}
	}
	if strings.Contains(s, "Content-Security-Policy:") {
		t.Error("Content-Security-Policy should not be set by default")
	}
}

func TestSecurityHeadersCustom(t *testing.T) {
	app := fh.New()
	app.Use(security.New(security.Config{
		FrameDeny:          false,
		ContentTypeNosniff: false,
		HSTSMaxAge:         0,
	}))
	app.Get("/", func(ctx *fh.Ctx) error {
		return ctx.SendString("ok")
	})
	addr := testServer(t, app)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	resp, _ := io.ReadAll(conn)
	s := string(resp)

	if strings.Contains(s, "X-Frame-Options") {
		t.Error("expected X-Frame-Options to be disabled")
	}
	if strings.Contains(s, "X-Content-Type-Options") {
		t.Error("expected X-Content-Type-Options to be disabled")
	}
	if strings.Contains(s, "Strict-Transport-Security") {
		t.Error("expected HSTS to be disabled")
	}
}

func TestCORS(t *testing.T) {
	app := fh.New()
	app.Use(cors.New())
	app.Get("/", func(ctx *fh.Ctx) error {
		return ctx.SendString("ok")
	})
	addr := testServer(t, app)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write([]byte("OPTIONS / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://example.com\r\n\r\n"))
	resp, _ := io.ReadAll(conn)
	if !strings.Contains(string(resp), "204") && !strings.Contains(string(resp), "200") {
		t.Fatalf("CORS preflight failed: %s", resp)
	}
}

func TestRequestID(t *testing.T) {
	app := fh.New()
	app.Use(requestid.New())
	app.Get("/", func(ctx *fh.Ctx) error {
		return ctx.SendString("ok")
	})
	addr := testServer(t, app)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	resp, _ := io.ReadAll(conn)
	if !strings.Contains(string(resp), "X-Request-ID") {
		t.Fatalf("missing X-Request-ID header in response: %s", resp)
	}
}

func TestCompressionNegotiationAndStreaming(t *testing.T) {
	app := fh.New()
	app.Use(compress.New())
	payload := strings.Repeat("hello world ", 60) // > 512 bytes to bypass MinSize check
	app.Get("/compressed", func(ctx *fh.Ctx) error {
		return ctx.Stream(func(w *fh.StreamWriter) error {
			_, err := w.Write([]byte(payload))
			return err
		})
	})
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/compressed", "", map[string]string{"Accept-Encoding": "gzip"})
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	zr, err := gzip.NewReader(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(zr)
	zr.Close()
	if err != nil || string(decoded) != payload {
		t.Fatalf("decoded=%q err=%v", decoded, err)
	}

	_, body = doRequest(t, addr, "GET", "/compressed", "", map[string]string{"Accept-Encoding": "gzip;q=0"})
	if !strings.Contains(body, "hello ") || !strings.Contains(body, "world") || strings.HasPrefix(body, "\x1f\x8b") {
		t.Fatalf("q=0 response negotiation failed: %q", body)
	}
}

func TestCooperativeTimeout(t *testing.T) {
	app := fh.New()
	app.Use(timeout.New(5 * time.Millisecond))
	app.Get("/timeout", func(ctx *fh.Ctx) error { <-ctx.Context().Done(); return nil })
	addr := testServer(t, app)
	code, _ := doRequest(t, addr, "GET", "/timeout", "", nil)
	if code != 503 {
		t.Fatalf("expected 503, got %d", code)
	}
}

func TestSessionMiddlewarePersistsBeforeResponseAndDestroys(t *testing.T) {
	store := session.NewMemoryStore(0)
	manager := session.NewSessionManager(store,
		session.SessionCookieName("sid"),
		session.SessionSecrets([]byte("0123456789abcdef0123456789abcdef")),
		session.SessionSecure(false),
		session.SessionMaxAge(time.Hour),
	)
	app := fh.New()
	app.Use(session.New(manager))
	app.Get("/counter", func(ctx *fh.Ctx) error {
		s := session.Get(ctx)
		count, _ := s.Get("count").(float64)
		if count == 0 {
			if n, ok := s.Get("count").(int); ok {
				count = float64(n)
			}
		}
		count++
		s.Set("count", int(count))
		return ctx.SendString(fmt.Sprintf("%d", int(count)))
	})
	app.Get("/logout", func(ctx *fh.Ctx) error {
		if err := manager.Destroy(ctx, session.Get(ctx)); err != nil {
			return err
		}
		return ctx.SendStatus(204)
	})
	app.Get("/session-stream", func(ctx *fh.Ctx) error {
		session.Get(ctx).Set("streamed", true)
		return ctx.Stream(func(w *fh.StreamWriter) error { _, err := w.Write([]byte("streamed")); return err })
	})
	addr := testServer(t, app)

	first := rawHTTP11(t, addr, "GET /counter HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	cookie := responseCookiePair(first, "sid")
	if cookie == "" || !strings.Contains(first, "HttpOnly") || !strings.Contains(first, "SameSite=Lax") || !strings.Contains(first, "Expires=") {
		t.Fatalf("session cookie missing security attributes: %q", first)
	}
	second := rawHTTP11(t, addr, "GET /counter HTTP/1.1\r\nHost: local\r\nCookie: "+cookie+"\r\nConnection: close\r\n\r\n")
	if !strings.HasSuffix(second, "2") {
		t.Fatalf("session did not persist: %q", second)
	}
	logout := rawHTTP11(t, addr, "GET /logout HTTP/1.1\r\nHost: local\r\nCookie: "+cookie+"\r\nConnection: close\r\n\r\n")
	if !strings.Contains(logout, "Max-Age=0") {
		t.Fatalf("logout did not expire cookie: %q", logout)
	}
	again := rawHTTP11(t, addr, "GET /counter HTTP/1.1\r\nHost: local\r\nCookie: "+cookie+"\r\nConnection: close\r\n\r\n")
	if !strings.HasSuffix(again, "1") {
		t.Fatalf("destroyed session was resurrected: %q", again)
	}
	streamed := rawHTTP11(t, addr, "GET /session-stream HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if responseCookiePair(streamed, "sid") == "" {
		t.Fatalf("streamed response omitted session cookie: %q", streamed)
	}
}

func rawHTTP11(t *testing.T, addr, request string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	return string(resp)
}

func responseCookiePair(response, name string) string {
	for _, line := range strings.Split(response, "\r\n") {
		prefix := "Set-Cookie: " + name + "="
		if strings.HasPrefix(line, prefix) {
			value := strings.TrimPrefix(line, "Set-Cookie: ")
			if semi := strings.IndexByte(value, ';'); semi >= 0 {
				value = value[:semi]
			}
			return value
		}
	}
	return ""
}

func TestAllMethods(t *testing.T) {
	app := fh.New()
	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"} {
		method := m // capture
		app.Add(method, "/test", func(ctx *fh.Ctx) error {
			return ctx.SendString(method)
		})
	}
	addr := testServer(t, app)

	for _, m := range []string{"GET", "POST", "PUT", "DELETE", "PATCH"} {
		code, body := doRequest(t, addr, m, "/test", "", nil)
		if code != 200 {
			t.Fatalf("%s: expected 200, got %d", m, code)
		}
		if m != "HEAD" && body != m {
			t.Fatalf("%s: expected %q body, got %q", m, m, body)
		}
	}
}

// ── Benchmark ──────────────────────────────────────────────────────────────

func BenchmarkHelloWorld(b *testing.B) {
	app := fh.New()
	app.Get("/bench", func(ctx *fh.Ctx) error {
		return ctx.SendString("hello")
	})

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go app.Serve(ln)
	defer app.Shutdown()

	addr := ln.Addr().String()
	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	req := []byte("GET /bench HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
	buf := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn.Write(req)
		conn.SetDeadline(time.Now().Add(time.Second))
		n, _ := conn.Read(buf)
		if n == 0 {
			b.Fatal("empty response")
		}
	}
}

func BenchmarkParallelRequests(b *testing.B) {
	app := fh.New()
	app.Get("/bench", func(ctx *fh.Ctx) error {
		return ctx.SendString("hello")
	})

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go app.Serve(ln)
	defer app.Shutdown()

	addr := ln.Addr().String()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		conn, _ := net.Dial("tcp", addr)
		defer conn.Close()
		req := []byte("GET /bench HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
		buf := make([]byte, 4096)
		for pb.Next() {
			conn.Write(req)
			conn.SetDeadline(time.Now().Add(time.Second))
			conn.Read(buf)
		}
	})
}

func BenchmarkRouteWithParams(b *testing.B) {
	app := fh.New()
	app.Get("/users/:id/posts/:post", func(ctx *fh.Ctx) error {
		return ctx.SendString(ctx.Param("id") + ctx.Param("post"))
	})

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go app.Serve(ln)
	defer app.Shutdown()

	addr := ln.Addr().String()
	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	req := []byte("GET /users/42/posts/7 HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
	buf := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		conn.Write(req)
		conn.SetDeadline(time.Now().Add(time.Second))
		conn.Read(buf)
	}
}
