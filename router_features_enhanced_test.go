package fh

import (
	"testing"
)

func TestRouterParamConstraints(t *testing.T) {
	newConstraintApp := func() *App {
		app := New()
		app.Get("/users/:id<int>", func(c Ctx) error {
			return c.SendString("int id: " + c.Param("id"))
		})
		app.Get("/users/:slug<alpha>", func(c Ctx) error {
			return c.SendString("alpha slug: " + c.Param("slug"))
		})
		app.Get("/orders/:id<uuid>", func(c Ctx) error {
			return c.SendString("uuid order: " + c.Param("id"))
		})
		return app
	}

	t.Run("int constraint match", func(t *testing.T) {
		app := newConstraintApp()
		resp := pipeRequest(t, app, "GET /users/123 HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strContains(resp, "200 OK") || !strContains(resp, "int id: 123") {
			t.Fatalf("expected int constraint match, got: %s", resp)
		}
	})

	t.Run("alpha constraint match", func(t *testing.T) {
		app := newConstraintApp()
		resp := pipeRequest(t, app, "GET /users/alice HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strContains(resp, "200 OK") || !strContains(resp, "alpha slug: alice") {
			t.Fatalf("expected alpha constraint match, got: %s", resp)
		}
	})

	t.Run("uuid constraint match", func(t *testing.T) {
		app := newConstraintApp()
		uuidStr := "123e4567-e89b-12d3-a456-426614174000"
		resp := pipeRequest(t, app, "GET /orders/"+uuidStr+" HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strContains(resp, "200 OK") || !strContains(resp, "uuid order: "+uuidStr) {
			t.Fatalf("expected uuid constraint match, got: %s", resp)
		}
	})
}

func TestRouterOptionalParams(t *testing.T) {
	newOptionalApp := func() *App {
		app := New()
		app.Get("/posts/:category?", func(c Ctx) error {
			cat := c.Param("category")
			if cat == "" {
				return c.SendString("all posts")
			}
			return c.SendString("category: " + cat)
		})
		return app
	}

	t.Run("optional param omitted", func(t *testing.T) {
		app := newOptionalApp()
		resp := pipeRequest(t, app, "GET /posts HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strContains(resp, "200 OK") || !strContains(resp, "all posts") {
			t.Fatalf("expected optional param omitted match, got: %s", resp)
		}
	})

	t.Run("optional param provided", func(t *testing.T) {
		app := newOptionalApp()
		resp := pipeRequest(t, app, "GET /posts/tech HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strContains(resp, "200 OK") || !strContains(resp, "category: tech") {
			t.Fatalf("expected optional param provided match, got: %s", resp)
		}
	})
}

func TestAppMount(t *testing.T) {
	sub := New()
	sub.Get("/dashboard", func(c Ctx) error {
		return c.SendString("admin dashboard")
	}).Name("dashboard").Tag("admin")

	mainApp := New()
	mainApp.Mount("/admin", sub)

	resp := pipeRequest(t, mainApp, "GET /admin/dashboard HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strContains(resp, "200 OK") || !strContains(resp, "admin dashboard") {
		t.Fatalf("expected mounted app route match, got: %s", resp)
	}

	routes := mainApp.Routes()
	found := false
	for _, r := range routes {
		if r.Path == "/admin/dashboard" && len(r.Tags) > 0 && r.Tags[0] == "admin" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("mounted route metadata/tags were not preserved: %#v", routes)
	}
}

func TestURLWithQueryAndMeta(t *testing.T) {
	app := New()
	app.Get("/users/:id/posts/:post", func(c Ctx) error {
		return c.SendString("ok")
	}).Name("user.post").Tag("users", "posts").Meta("owner", "team-a")

	urlStr, err := app.URLWithQuery("user.post", Map{"id": 42, "post": "hello"}, Map{"page": 1, "sort": "desc"})
	if err != nil {
		t.Fatalf("URLWithQuery failed: %v", err)
	}
	if !strContains(urlStr, "/users/42/posts/hello?") || !strContains(urlStr, "page=1") || !strContains(urlStr, "sort=desc") {
		t.Fatalf("unexpected URLWithQuery output: %s", urlStr)
	}

	treeStr := app.RouteTreeString()
	if !strContains(treeStr, "GET") || !strContains(treeStr, "/users/:id/posts/:post") {
		t.Fatalf("unexpected RouteTreeString output: %s", treeStr)
	}
}

func strContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) > 0 && findSubstr(s, substr))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
