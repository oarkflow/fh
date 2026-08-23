package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oarkflow/fh"
)

func TestCompressGzipAndDeflate(t *testing.T) {
	largeContent := strings.Repeat("Hello modern http server world! ", 20)

	t.Run("GzipEncoding", func(t *testing.T) {
		app := fh.New()
		app.Use(New(Config{MinSize: 10}))
		app.Get("/data", func(c fh.Ctx) error {
			return c.SendString(largeContent)
		})

		req := httptest.NewRequest("GET", "/data", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()

		if ce := resp.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("expected Content-Encoding 'gzip', got %q", ce)
		}
		if vary := resp.Header.Get("Vary"); !strings.Contains(vary, "Accept-Encoding") {
			t.Errorf("expected Vary to contain 'Accept-Encoding', got %q", vary)
		}
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatalf("gzip.NewReader error: %v", err)
		}
		decompressed, _ := io.ReadAll(gzReader)
		if string(decompressed) != largeContent {
			t.Errorf("decompressed content does not match original")
		}
	})

	t.Run("DeflateEncoding", func(t *testing.T) {
		app := fh.New()
		app.Use(New(Config{MinSize: 10}))
		app.Get("/data", func(c fh.Ctx) error {
			return c.SendString(largeContent)
		})

		req := httptest.NewRequest("GET", "/data", nil)
		req.Header.Set("Accept-Encoding", "deflate")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()

		if ce := resp.Header.Get("Content-Encoding"); ce != "deflate" {
			t.Errorf("expected Content-Encoding 'deflate', got %q", ce)
		}
		flateReader := flate.NewReader(resp.Body)
		decompressed, _ := io.ReadAll(flateReader)
		if string(decompressed) != largeContent {
			t.Errorf("deflate decompressed content does not match original")
		}
	})

	t.Run("QualityNegotiation", func(t *testing.T) {
		app := fh.New()
		app.Use(New(Config{MinSize: 10}))
		app.Get("/data", func(c fh.Ctx) error {
			return c.SendString(largeContent)
		})

		req := httptest.NewRequest("GET", "/data", nil)
		req.Header.Set("Accept-Encoding", "gzip;q=0.5, deflate;q=0.9")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()

		if ce := resp.Header.Get("Content-Encoding"); ce != "deflate" {
			t.Errorf("expected Content-Encoding 'deflate' with higher q-value, got %q", ce)
		}
	})
}

func TestDeflateEncoderDirect(t *testing.T) {
	encoder := NewDeflateEncoder(flate.BestSpeed)
	if encoder.Encoding() != EncodingDeflate {
		t.Errorf("unexpected encoding: %s", encoder.Encoding())
	}
	src := []byte("Testing deflate compression directly on bytes buffer")
	var dst bytes.Buffer
	if err := encoder.Encode(&dst, src); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	r := flate.NewReader(&dst)
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(out) != string(src) {
		t.Errorf("expected %q, got %q", string(src), string(out))
	}
}
