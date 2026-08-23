package fh

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func newMockStaticFS() fstest.MapFS {
	var gzBuf bytes.Buffer
	gw := gzip.NewWriter(&gzBuf)
	gw.Write([]byte("Pre-compressed gzip body content"))
	gw.Close()

	return fstest.MapFS{
		"index.htm": &fstest.MapFile{
			Data: []byte("<h1>Alternative Index</h1>"),
		},
		"bundle.js": &fstest.MapFile{
			Data: []byte("console.log('original');"),
		},
		"bundle.js.gz": &fstest.MapFile{
			Data: gzBuf.Bytes(),
		},
		"bundle.js.br": &fstest.MapFile{
			Data: []byte("brotli-raw-bytes-mock"),
		},
	}
}

func newStaticTestApp() *App {
	app := New()
	app.StaticFS("/site", newMockStaticFS(), StaticConfig{
		IndexFiles:    []string{"index.html", "index.htm"},
		CacheControl:  "public, max-age=31536000, immutable",
		PreCompressed: true,
		NotFoundHandler: func(c Ctx) error {
			return c.Status(404).SendString("Custom 404: Not in static")
		},
	})
	return app
}

func TestStaticConfigModernFeatures(t *testing.T) {
	t.Run("IndexFilesAndCacheControl", func(t *testing.T) {
		app := newStaticTestApp()
		req := httptest.NewRequest("GET", "/site/", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "<h1>Alternative Index</h1>" {
			t.Errorf("expected alternative index, got %q", string(body))
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
			t.Errorf("expected custom Cache-Control, got %q", cc)
		}
	})

	t.Run("PreCompressedBrotli", func(t *testing.T) {
		app := newStaticTestApp()
		req := httptest.NewRequest("GET", "/site/bundle.js", nil)
		req.Header.Set("Accept-Encoding", "br, gzip")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()
		if ce := resp.Header.Get("Content-Encoding"); ce != "br" {
			t.Errorf("expected Content-Encoding 'br', got %q", ce)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "brotli-raw-bytes-mock" {
			t.Errorf("expected br mock content, got %q", string(body))
		}
	})

	t.Run("PreCompressedGzip", func(t *testing.T) {
		app := newStaticTestApp()
		req := httptest.NewRequest("GET", "/site/bundle.js", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()
		if ce := resp.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("expected Content-Encoding 'gzip', got %q", ce)
		}
		gzr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatalf("gzip reader error: %v", err)
		}
		decompressed, _ := io.ReadAll(gzr)
		if string(decompressed) != "Pre-compressed gzip body content" {
			t.Errorf("unexpected decompressed content: %q", string(decompressed))
		}
	})

	t.Run("NotFoundHandler", func(t *testing.T) {
		app := newStaticTestApp()
		req := httptest.NewRequest("GET", "/site/nonexistent.png", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Errorf("expected status 404, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "Custom 404: Not in static" {
			t.Errorf("expected custom 404 message, got %q", string(body))
		}
	})
}
