package fasthttp_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"

	fh "github.com/oarkflow/fasthttp"
	"github.com/oarkflow/fasthttp/middleware/session"
)

func TestNamedRouteURLAndRedirectTo(t *testing.T) {
	app := fh.New()
	app.Get("/users/:id", func(c *fh.Ctx) error {
		return c.SendString(c.Param("id"))
	}).Name("users.show")
	app.Get("/go", func(c *fh.Ctx) error {
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
	app.Get("/back", func(c *fh.Ctx) error { return c.RedirectBack("/safe") })
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
	app.Get("/set", func(c *fh.Ctx) error {
		c.Flash("notice", "saved")
		return c.Redirect("/read")
	})
	app.Get("/read", func(c *fh.Ctx) error {
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
