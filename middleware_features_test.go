package fh_test

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
	cachemw "github.com/oarkflow/fh/mw/cache"
	"github.com/oarkflow/fh/mw/cors"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/fh/mw/earlydata"
	"github.com/oarkflow/fh/mw/rewrite"
	"github.com/oarkflow/fh/mw/skip"
)

func request(t *testing.T, addr, method, path, body string, headers map[string]string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b), resp.Header
}

func TestBodyLimitGlobalAndEndpoint(t *testing.T) {
	app := fh.New()
	app.Use(bodylimit.New(8))
	app.Post("/global", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Post("/small", bodylimit.New(3), func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)
	code, _, _ := request(t, addr, "POST", "/global", "123456789", nil)
	if code != fh.StatusPayloadTooLarge {
		t.Fatalf("global status = %d", code)
	}
	code, _, _ = request(t, addr, "POST", "/small", "1234", nil)
	if code != fh.StatusPayloadTooLarge {
		t.Fatalf("endpoint status = %d", code)
	}
	code, _, _ = request(t, addr, "POST", "/global", "1234", nil)
	if code != fh.StatusOK {
		t.Fatalf("allowed status = %d", code)
	}

	boundary := fh.New()
	boundary.Post("/json", bodylimit.New(100), func(c fh.Ctx) error { return c.SendString("ok") })
	boundaryAddr := testServer(t, boundary)
	code, _, _ = request(t, boundaryAddr, "POST", "/json", strings.Repeat("a", 100), map[string]string{"Content-Type": "application/json"})
	if code != fh.StatusOK {
		t.Fatalf("boundary status = %d", code)
	}
	code, _, _ = request(t, boundaryAddr, "POST", "/json", strings.Repeat("a", 101), map[string]string{"Content-Type": "application/json"})
	if code != fh.StatusPayloadTooLarge {
		t.Fatalf("over boundary status = %d", code)
	}
}

func TestRewriteAndLoopProtection(t *testing.T) {
	app := fh.New()
	app.Use(rewrite.New(
		rewrite.Rule{From: "/legacy", To: "/new/static"},
		rewrite.Rule{From: "/member/:id", To: "/new/:id", Methods: []string{"GET"}},
		rewrite.Rule{From: "/old/:name", To: "/new/{name}", Headers: map[string]string{"X-Rewrite": "yes"}},
		rewrite.Rule{From: "/docs/*path", To: "/tree/*path"},
	))
	app.Get("/new/:name", func(c fh.Ctx) error { return c.SendString(c.Param("name") + "?" + c.Query("v")) })
	app.Get("/tree/*", func(c fh.Ctx) error { return c.SendString(c.Param("*")) })
	addr := testServer(t, app)
	code, body, _ := request(t, addr, "GET", "/old/alice?v=2", "", map[string]string{"X-Rewrite": "yes"})
	if code != 200 || body != "alice?2" {
		t.Fatalf("rewrite = %d %q", code, body)
	}
	code, body, _ = request(t, addr, "GET", "/legacy", "", nil)
	if code != 200 || body != "static?" {
		t.Fatalf("static rewrite = %d %q", code, body)
	}
	code, body, _ = request(t, addr, "GET", "/member/42", "", nil)
	if code != 200 || body != "42?" {
		t.Fatalf("dynamic rewrite = %d %q", code, body)
	}
	code, body, _ = request(t, addr, "GET", "/docs/api/v2/auth", "", nil)
	if code != 200 || body != "api/v2/auth" {
		t.Fatalf("wildcard rewrite = %d %q", code, body)
	}

	loop := fh.New()
	loop.Use(rewrite.New(rewrite.Rule{From: "/a", To: "/b"}, rewrite.Rule{From: "/b", To: "/a"}))
	loop.Get("/a", func(c fh.Ctx) error { return c.SendString("unreachable") })
	loopAddr := testServer(t, loop)
	code, body, _ = request(t, loopAddr, "GET", "/a", "", nil)
	if code != fh.StatusLoopDetected || !strings.Contains(body, "REWRITE_LOOP") {
		t.Fatalf("loop = %d %q", code, body)
	}
}

func TestSkipMiddleware(t *testing.T) {
	app := fh.New()
	app.Use(skip.New(bodylimit.New(2), skip.Paths("/health")))
	app.Post("/health", func(c fh.Ctx) error { return c.SendString("healthy") })
	app.Post("/data", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)
	code, _, _ := request(t, addr, "POST", "/health", "large", nil)
	if code != 200 {
		t.Fatalf("skipped status = %d", code)
	}
	code, _, _ = request(t, addr, "POST", "/data", "large", nil)
	if code != 413 {
		t.Fatalf("protected status = %d", code)
	}
}

func TestCSRFMiddleware(t *testing.T) {
	app := fh.New()
	app.Use(csrf.New())
	app.Get("/token", func(c fh.Ctx) error { return c.SendString(c.Locals("csrf_token").(string)) })
	app.Post("/change", func(c fh.Ctx) error { return c.SendString("changed") })
	addr := testServer(t, app)
	code, token, headers := request(t, addr, "GET", "/token", "", nil)
	if code != 200 || token == "" {
		t.Fatalf("token = %d %q", code, token)
	}
	cookie := headers.Get("Set-Cookie")
	code, _, _ = request(t, addr, "POST", "/change", "", map[string]string{"Cookie": cookie, "X-CSRF-Token": "wrong"})
	if code != 403 {
		t.Fatalf("invalid token status = %d", code)
	}
	code, body, _ := request(t, addr, "POST", "/change", "", map[string]string{"Cookie": cookie, "X-CSRF-Token": token})
	if code != 200 || body != "changed" {
		t.Fatalf("valid token = %d %q", code, body)
	}
}

func TestCacheMiddleware(t *testing.T) {
	var calls atomic.Int32
	app := fh.New()
	app.Use(cachemw.New(cachemw.Config{TTL: time.Minute}))
	app.Get("/value", func(c fh.Ctx) error { return c.SendString("call-" + string(rune('0'+calls.Add(1)))) })
	addr := testServer(t, app)
	_, first, h1 := request(t, addr, "GET", "/value", "", nil)
	_, second, h2 := request(t, addr, "GET", "/value", "", nil)
	if first != "call-1" || second != first || h1.Get("X-Cache") != "MISS" || h2.Get("X-Cache") != "HIT" || calls.Load() != 1 {
		t.Fatalf("cache: first=%q second=%q headers=%q/%q calls=%d", first, second, h1.Get("X-Cache"), h2.Get("X-Cache"), calls.Load())
	}
}

func TestEarlyDataAndPreflight(t *testing.T) {
	app := fh.New()
	app.Use(earlydata.New())
	app.Use(cors.New(cors.Config{AllowOrigins: []string{"https://app.example"}, AllowMethods: []string{"GET", "POST"}, AllowHeaders: []string{"Content-Type", "X-CSRF-Token"}}))
	app.Post("/change", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)
	code, _, _ := request(t, addr, "POST", "/change", "", map[string]string{"Early-Data": "1"})
	if code != fh.StatusTooEarly {
		t.Fatalf("early data status = %d", code)
	}
	valid := map[string]string{"Origin": "https://app.example", "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "X-CSRF-Token"}
	code, _, headers := request(t, addr, "OPTIONS", "/change", "", valid)
	if code != 204 || headers.Get("Access-Control-Allow-Origin") != "https://app.example" {
		t.Fatalf("preflight = %d %#v", code, headers)
	}
	valid["Access-Control-Request-Headers"] = "X-Forbidden"
	code, _, _ = request(t, addr, "OPTIONS", "/change", "", valid)
	if code != 403 {
		t.Fatalf("denied preflight = %d", code)
	}
}
