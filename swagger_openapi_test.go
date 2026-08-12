package fh

import (
	"strings"
	"testing"
)

func TestOpenAPISpecAndSwaggerUI(t *testing.T) {
	app := New()
	app.Get("/users/:id<int>", func(c Ctx) error {
		return c.SendString("ok")
	}).Name("users.show").Tag("Users").Meta("summary", "Fetch user profile")

	app.EnableSwaggerUI("/docs")

	t.Run("openapi spec endpoint", func(t *testing.T) {
		resp := pipeRequest(t, app, "GET /docs/openapi.json HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "/users/{id}") || !strings.Contains(resp, "Fetch user profile") {
			t.Fatalf("unexpected OpenAPI spec response: %s", resp)
		}
	})

	t.Run("swagger ui html endpoint", func(t *testing.T) {
		subApp := New()
		subApp.EnableSwaggerUI("/docs")
		resp := pipeRequest(t, subApp, "GET /docs HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "text/html") || !strings.Contains(resp, "swagger-ui") {
			t.Fatalf("unexpected Swagger UI response: %s", resp)
		}
	})
}

func TestGroupMount(t *testing.T) {
	sub := New()
	sub.Get("/users", func(c Ctx) error {
		return c.SendString("sub users")
	})

	mainApp := New()
	apiGroup := mainApp.Group("/api")
	apiGroup.Mount("/v1", sub)

	resp := pipeRequest(t, mainApp, "GET /api/v1/users HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "sub users") {
		t.Fatalf("unexpected group mount response: %s", resp)
	}
}
