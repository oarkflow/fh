package fetchmetadata

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/oarkflow/fh"
)

func TestFetchMetadataMiddleware(t *testing.T) {
	t.Run("AllowSameOrigin", func(t *testing.T) {
		app := fh.New()
		app.Use(New())
		app.Get("/api/data", func(c fh.Ctx) error {
			return c.SendString("ok")
		})

		req := httptest.NewRequest("GET", "/api/data", nil)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		resp, err := app.Test(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("expected 200 OK, got %d, err %v", resp.StatusCode, err)
		}
	})

	t.Run("AllowTopLevelNavigation", func(t *testing.T) {
		app := fh.New()
		app.Use(New())
		app.Get("/article", func(c fh.Ctx) error {
			return c.SendString("article content")
		})

		req := httptest.NewRequest("GET", "/article", nil)
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.Header.Set("Sec-Fetch-Mode", "navigate")
		req.Header.Set("Sec-Fetch-Dest", "document")
		resp, err := app.Test(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("expected 200 OK for cross-site navigation, got %d, err %v", resp.StatusCode, err)
		}
	})

	t.Run("RejectCrossSiteAPIRequest", func(t *testing.T) {
		app := fh.New()
		app.Use(New())
		app.Post("/api/transfer", func(c fh.Ctx) error {
			return c.SendString("transferred")
		})

		req := httptest.NewRequest("POST", "/api/transfer", nil)
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.Header.Set("Sec-Fetch-Mode", "cors")
		req.Header.Set("Sec-Fetch-Dest", "empty")
		resp, err := app.Test(req)
		if err != nil || resp.StatusCode != 403 {
			t.Fatalf("expected 403 Forbidden for cross-site POST, got %d, err %v", resp.StatusCode, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if len(body) == 0 {
			t.Errorf("expected problem details body")
		}
	})

	t.Run("ExemptPathAllowed", func(t *testing.T) {
		app := fh.New()
		app.Use(New(Config{
			ExemptPaths: []string{"/webhooks/"},
		}))
		app.Post("/webhooks/github", func(c fh.Ctx) error {
			return c.SendString("webhook received")
		})

		req := httptest.NewRequest("POST", "/webhooks/github", nil)
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.Header.Set("Sec-Fetch-Mode", "no-cors")
		resp, err := app.Test(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("expected 200 OK for exempt path, got %d, err %v", resp.StatusCode, err)
		}
	})
}
