package fh

import (
	"strings"
	"testing"
)

func TestServerMetrics(t *testing.T) {
	app := New()
	app.Get("/ok", func(c Ctx) error {
		return c.SendString("ok")
	})
	app.Get("/err", func(c Ctx) error {
		return c.Status(500).SendString("error")
	})

	_ = pipeRequest(t, app, "GET /ok HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")

	app2 := New()
	app2.Get("/err", func(c Ctx) error {
		return c.Status(500).SendString("error")
	})
	_ = pipeRequest(t, app2, "GET /err HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")

	m1 := app.Metrics()
	if m1.TotalRequests < 1 || m1.Status2xx < 1 {
		t.Fatalf("unexpected metrics snapshot: %#v", m1)
	}

	m2 := app2.Metrics()
	if m2.TotalRequests < 1 || m2.Status5xx < 1 || m2.TotalErrors < 1 {
		t.Fatalf("unexpected error metrics snapshot: %#v", m2)
	}
}

func TestAutoETag(t *testing.T) {
	app := New()
	app.Get("/data", func(c Ctx) error {
		c.AutoETag()
		return c.JSON(Map{"hello": "world"})
	})

	t.Run("first request generates etag", func(t *testing.T) {
		resp := pipeRequest(t, app, "GET /data HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if !strings.Contains(resp, "200 OK") || !strings.Contains(resp, "ETag: W/\"") {
			t.Fatalf("expected ETag header in response: %s", resp)
		}
	})

	t.Run("conditional request returns 304", func(t *testing.T) {
		subApp1 := New()
		subApp1.Get("/data", func(c Ctx) error {
			c.AutoETag()
			return c.JSON(Map{"hello": "world"})
		})
		r1 := pipeRequest(t, subApp1, "GET /data HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")

		// Extract ETag header line
		var etagVal string
		for _, line := range strings.Split(r1, "\r\n") {
			if strings.HasPrefix(line, "ETag: ") {
				etagVal = strings.TrimPrefix(line, "ETag: ")
				break
			}
		}

		if etagVal == "" {
			t.Fatalf("failed extracting etag from: %s", r1)
		}

		subApp2 := New()
		subApp2.Get("/data", func(c Ctx) error {
			c.AutoETag()
			return c.JSON(Map{"hello": "world"})
		})
		r2 := pipeRequest(t, subApp2, "GET /data HTTP/1.1\r\nHost: local\r\nIf-None-Match: "+etagVal+"\r\nConnection: close\r\n\r\n")
		if !strings.Contains(r2, "304 Not Modified") {
			t.Fatalf("expected 304 Not Modified, got: %s", r2)
		}
	})
}
