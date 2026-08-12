package fh

import (
	"strings"
	"testing"
)

type UserDTO struct {
	Name  string `json:"name" query:"name" form:"name"`
	Email string `json:"email" query:"email" form:"email"`
	Role  string `header:"X-User-Role"`
}

func TestBindDTO(t *testing.T) {
	app := New()
	app.Post("/users", func(c Ctx) error {
		var req UserDTO
		if err := c.Bind(&req); err != nil {
			return err
		}
		return c.SendString(req.Name + ":" + req.Email + ":" + req.Role)
	})

	t.Run("bind json and header", func(t *testing.T) {
		req := "POST /users HTTP/1.1\r\nHost: local\r\nContent-Type: application/json\r\nX-User-Role: admin\r\nContent-Length: 37\r\nConnection: close\r\n\r\n{\"name\":\"alice\",\"email\":\"a@test.com\"}"
		resp := pipeRequest(t, app, req)
		if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "alice:a@test.com:admin") {
			t.Fatalf("unexpected DTO bind response: %s", resp)
		}
	})
}

func TestSSEventAndStreamWriter(t *testing.T) {
	t.Run("ssevents", func(t *testing.T) {
		app := New()
		app.Get("/events", func(c Ctx) error {
			return c.SSEvent("user_joined", Map{"id": 42})
		})
		resp := pipeRequest(t, app, "GET /events HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strings.Contains(resp, "text/event-stream") || !strings.Contains(resp, "event: user_joined") || !strings.Contains(resp, "{\"id\":42}") {
			t.Fatalf("unexpected SSE response: %s", resp)
		}
	})

	t.Run("stream writer", func(t *testing.T) {
		app := New()
		app.Get("/stream", func(c Ctx) error {
			return c.Stream(func(w *StreamWriter) error {
				_, _ = w.Write([]byte("chunk1"))
				_, _ = w.Write([]byte("chunk2"))
				return nil
			})
		})
		resp := pipeRequest(t, app, "GET /stream HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strings.Contains(resp, "chunk1") || !strings.Contains(resp, "chunk2") {
			t.Fatalf("unexpected stream writer response: %s", resp)
		}
	})
}

func TestProblemDetails(t *testing.T) {
	app := New()
	app.Get("/error", func(c Ctx) error {
		return c.ProblemDetails(404, "Not Found", "Item 123 does not exist", "/errors/not-found")
	})
	resp := pipeRequest(t, app, "GET /error HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "404 Not Found") || !strings.Contains(resp, "application/problem+json") || !strings.Contains(resp, "Item 123 does not exist") {
		t.Fatalf("unexpected problem details response: %s", resp)
	}
}

func TestHealthCheck(t *testing.T) {
	app := New()
	app.HealthCheck("/healthz", HealthConfig{
		Probes: map[string]HealthProbe{
			"db": func() bool { return true },
		},
	})
	resp := pipeRequest(t, app, "GET /healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "\"status\":\"UP\"") {
		t.Fatalf("unexpected health check response: %s", resp)
	}
}
