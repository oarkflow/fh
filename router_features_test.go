package fh_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
)

func TestCompiledRoutePatternUsesRouterSemantics(t *testing.T) {
	tests := []struct {
		pattern, path string
		want          map[string]string
		ok            bool
	}{
		{"/static", "/static", nil, true},
		{"/users/:id", "/users/42", map[string]string{"id": "42"}, true},
		{"/users/:id", "/users/42/edit", nil, false},
		{"/assets/*", "/assets/css/app.css", map[string]string{"*": "css/app.css"}, true},
		{"/docs/*path", "/docs/api/v2", map[string]string{"path": "api/v2"}, true},
	}
	for _, tt := range tests {
		pattern := fh.CompileRoutePattern(tt.pattern)
		var params []fh.Param
		if got := pattern.Match(tt.path, &params); got != tt.ok {
			t.Fatalf("%s vs %s = %v", tt.pattern, tt.path, got)
		}
		for _, param := range params {
			if tt.want[param.Key] != param.Value {
				t.Fatalf("%s: %s=%q", tt.pattern, param.Key, param.Value)
			}
		}
	}
}

func TestNamedRouteURLAndRedirectTo(t *testing.T) {
	app := fh.New()
	app.Get("/users/:id", func(c fh.Ctx) error {
		return c.SendString(c.Param("id"))
	}).Name("users.show")
	app.Get("/go", func(c fh.Ctx) error {
		return c.RedirectTo("users.show", map[string]string{"id": "a/b", "tab": "activity"}, http.StatusSeeOther)
	})

	got, err := app.URL("users.show", map[string]string{"id": "a/b", "tab": "activity"})
	if err != nil || got != "/users/a%2Fb?tab=activity" {
		t.Fatalf("URL() = %q, %v", got, err)
	}
	if _, err := app.URL("users.show"); !errors.Is(err, fh.ErrRouteParamMissing) {
		t.Fatalf("expected ErrRouteParamMissing, got %v", err)
	}

	addr := testServer(t, app)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Get("http://" + addr + "/go")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/users/a%2Fb?tab=activity" {
		t.Fatalf("redirect = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestRedirectBackRejectsCrossOriginReferer(t *testing.T) {
	app := fh.New()
	app.Get("/back", func(c fh.Ctx) error { return c.RedirectBack("/safe") })
	addr := testServer(t, app)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	for _, tc := range []struct{ referer, want string }{
		{"http://localhost/profile?tab=1", "/profile?tab=1"},
		{"https://evil.example/phish", "/safe"},
	} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/back", nil)
		req.Host = "localhost"
		req.Header.Set("Referer", tc.referer)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("Location"); got != tc.want {
			t.Errorf("referer %q redirected to %q, want %q", tc.referer, got, tc.want)
		}
	}
}

func TestFlashPersistsForOneRequest(t *testing.T) {
	manager := session.NewSessionManager(session.NewMemoryStore(time.Minute),
		session.SessionSecret([]byte("0123456789abcdef0123456789abcdef")),
		session.SessionSecure(false),
	)
	app := fh.New()
	app.Use(session.New(manager))
	app.Get("/set", func(c fh.Ctx) error {
		c.Flash("notice", "saved")
		return c.Redirect("/read")
	})
	app.Get("/read", func(c fh.Ctx) error {
		value, _ := c.Flash("notice").(string)
		return c.SendString(value)
	})
	addr := testServer(t, app)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar}
	resp, err := client.Get("http://" + addr + "/set")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "saved" {
		t.Fatalf("first flash read = %q", body)
	}
	resp, err = client.Get("http://" + addr + "/read")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "" {
		t.Fatalf("flash was not consumed: %q", body)
	}
}

type typedGetReq struct {
	ID   string `param:"id"`
	Name string `query:"name"`
}
type typedGetRes struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestGetTyped(t *testing.T) {
	app := fh.New()
	app.GetTyped("/users/:id", func(c fh.Ctx, req typedGetReq) (typedGetRes, error) {
		return typedGetRes{ID: req.ID, Name: req.Name}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/users/42?name=john", "", nil)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"id":"42"`) || !strings.Contains(body, `"name":"john"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

type typedHeadRes struct {
	Count int `json:"count"`
}

func TestHeadTyped(t *testing.T) {
	app := fh.New()
	app.HeadTyped("/items", func(c fh.Ctx, req struct{}) (typedHeadRes, error) {
		return typedHeadRes{Count: 10}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "HEAD", "/items", "", nil)
	if code != 200 {
		t.Fatalf("status = %d", code)
	}
	if body != "" {
		t.Fatalf("HEAD should have empty body, got: %q", body)
	}
}

type typedOptsRes struct {
	Methods []string `json:"methods"`
}

func TestOptionsTyped(t *testing.T) {
	app := fh.New()
	app.OptionsTyped("/resource", func(c fh.Ctx, req struct{}) (typedOptsRes, error) {
		return typedOptsRes{Methods: []string{"GET", "POST"}}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "OPTIONS", "/resource", "", nil)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"methods"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

type typedTraceRes struct {
	Method string `json:"method"`
}

func TestTraceTyped(t *testing.T) {
	app := fh.New()
	app.TraceTyped("/debug", func(c fh.Ctx, req struct{}) (typedTraceRes, error) {
		return typedTraceRes{Method: "TRACE"}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "TRACE", "/debug", "", nil)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"method":"TRACE"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

type typedConnectRes struct {
	Status string `json:"status"`
}

func TestConnectTyped(t *testing.T) {
	app := fh.New()
	app.ConnectTyped("/tunnel", func(c fh.Ctx, req struct{}) (typedConnectRes, error) {
		return typedConnectRes{Status: "connected"}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "CONNECT", "/tunnel", "", nil)
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"status":"connected"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

type typedAllReq struct {
	Source string `json:"source"`
}
type typedAllRes struct {
	Method string `json:"method"`
	Source string `json:"source"`
}

func TestAllTyped(t *testing.T) {
	app := fh.New()
	app.AllTyped("/webhook", func(c fh.Ctx, req typedAllReq) (typedAllRes, error) {
		return typedAllRes{Method: c.Method(), Source: req.Source}, nil
	})
	addr := testServer(t, app)

	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"} {
		code, body := doRequest(t, addr, method, "/webhook", `{"source":"test"}`, map[string]string{"Content-Type": "application/json"})
		if code != 200 {
			t.Fatalf("%s: status = %d, body = %s", method, code, body)
		}
		if !strings.Contains(body, `"method":"`+method+`"`) {
			t.Fatalf("%s: missing method in body: %s", method, body)
		}
		if !strings.Contains(body, `"source":"test"`) {
			t.Fatalf("%s: missing source in body: %s", method, body)
		}
	}
	// HEAD has empty body by HTTP spec; verify status only
	code, _ := doRequest(t, addr, "HEAD", "/webhook", `{"source":"test"}`, map[string]string{"Content-Type": "application/json"})
	if code != 200 {
		t.Fatalf("HEAD: status = %d", code)
	}
}

type typedGroupReq struct {
	ID string `param:"id"`
}
type typedGroupRes struct {
	ID     string `json:"id"`
	Prefix string `json:"prefix"`
}

func TestGroupTyped(t *testing.T) {
	app := fh.New()
	grp := app.Group("/api")
	grp.GetTyped("/users/:id", func(c fh.Ctx, req typedGroupReq) (typedGroupRes, error) {
		return typedGroupRes{ID: req.ID, Prefix: "api"}, nil
	})
	grp.PostTyped("/users", func(c fh.Ctx, req struct {
		Name string `json:"name"`
	}) (typedGroupRes, error) {
		return typedGroupRes{ID: "new", Prefix: "api"}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/api/users/99", "", nil)
	if code != 200 {
		t.Fatalf("GET: status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"id":"99"`) || !strings.Contains(body, `"prefix":"api"`) {
		t.Fatalf("GET: unexpected body: %s", body)
	}

	code, body = doRequest(t, addr, "POST", "/api/users", `{"name":"alice"}`, map[string]string{"Content-Type": "application/json"})
	if code != 200 {
		t.Fatalf("POST: status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"id":"new"`) {
		t.Fatalf("POST: unexpected body: %s", body)
	}
}

type typedValidationReq struct {
	Email string `json:"email" validate:"required"`
}

func (r typedValidationReq) Validate() error {
	if r.Email == "" {
		return fh.NewHTTPError(422, "VALIDATION", "email is required")
	}
	return nil
}

type typedValidationRes struct {
	OK bool `json:"ok"`
}

func TestTypedValidation(t *testing.T) {
	app := fh.New()
	app.PostTyped("/validate", func(c fh.Ctx, req typedValidationReq) (typedValidationRes, error) {
		return typedValidationRes{OK: true}, nil
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "POST", "/validate", `{"email":""}`, map[string]string{"Content-Type": "application/json"})
	if code != 422 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"error":"validation_failed"`) {
		t.Fatalf("expected validation error, got: %s", body)
	}
}

type typedMiddlewareReq struct {
	Name string `json:"name"`
}
type typedMiddlewareRes struct {
	Name     string `json:"name"`
	LoggedBy string `json:"logged_by"`
}

func TestTypedWithMiddleware(t *testing.T) {
	app := fh.New()
	mw := func(c fh.Ctx) error {
		c.Locals("logged_by", "middleware")
		return c.Next()
	}
	app.PostTyped("/with-mw", func(c fh.Ctx, req typedMiddlewareReq) (typedMiddlewareRes, error) {
		return typedMiddlewareRes{Name: req.Name, LoggedBy: c.Locals("logged_by").(string)}, nil
	}, mw)
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "POST", "/with-mw", `{"name":"test"}`, map[string]string{"Content-Type": "application/json"})
	if code != 200 {
		t.Fatalf("status = %d, body = %s", code, body)
	}
	if !strings.Contains(body, `"logged_by":"middleware"`) {
		t.Fatalf("expected middleware data, got: %s", body)
	}
}
