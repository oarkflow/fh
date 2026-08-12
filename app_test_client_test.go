package fh

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAppTestClient(t *testing.T) {
	app := New()
	app.Get("/users/:id", func(c Ctx) error {
		return c.SendString("user " + c.Param("id"))
	})

	req := httptest.NewRequest("GET", "http://localhost/users/42", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(body) != "user 42" {
		t.Fatalf("unexpected test response body: %s", string(body))
	}
}

func TestOnRouteHook(t *testing.T) {
	app := New()
	var registered []string

	app.OnRoute(func(r RouteInfo) {
		registered = append(registered, r.Method+" "+r.Path)
	})

	app.Get("/api/v1/health", func(c Ctx) error { return c.SendString("ok") })
	app.Post("/api/v1/users", func(c Ctx) error { return c.SendString("ok") })

	if len(registered) != 2 || registered[0] != "GET /api/v1/health" || registered[1] != "POST /api/v1/users" {
		t.Fatalf("unexpected OnRoute registered routes: %#v", registered)
	}
}
