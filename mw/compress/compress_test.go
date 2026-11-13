package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"strconv"
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

func TestBreachProtectionSurvivesPartialConfig(t *testing.T) {
	payload := strings.Repeat("private response text ", 40)
	for _, tc := range []struct {
		name       string
		config     *Config
		compressed bool
	}{
		{name: "defaults", config: nil, compressed: false},
		{name: "partial config", config: &Config{MinSize: 10}, compressed: false},
		{name: "explicit opt out", config: &Config{MinSize: 10, DisableBreachProtection: true}, compressed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := fh.New()
			if tc.config == nil {
				app.Use(New())
			} else {
				app.Use(New(*tc.config))
			}
			app.Get("/private", func(c fh.Ctx) error {
				c.Set("Set-Cookie", "session=secret; HttpOnly")
				return c.SendString(payload)
			})
			req := httptest.NewRequest("GET", "/private", nil)
			req.Header.Set("Accept-Encoding", "gzip")
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			gotCompressed := resp.Header.Get("Content-Encoding") == "gzip"
			if gotCompressed != tc.compressed {
				t.Fatalf("Content-Encoding = %q, want compressed=%v", resp.Header.Get("Content-Encoding"), tc.compressed)
			}
		})
	}
}

func TestAcceptEncodingExplicitQualityOverridesWildcard(t *testing.T) {
	for _, tc := range []struct {
		header string
		gzipQ  float64
		defQ   float64
		want   string
	}{
		{header: "*;q=1, gzip;q=0", gzipQ: 0, defQ: 1, want: EncodingDeflate},
		{header: "gzip;q=0, *;q=1", gzipQ: 0, defQ: 1, want: EncodingDeflate},
		{header: "gzip;q=0.5, deflate;q=0.9", gzipQ: 0.5, defQ: 0.9, want: EncodingDeflate},
		{header: "gzip;q=1.001, *;q=1", gzipQ: 0, defQ: 1, want: EncodingDeflate},
		{header: "gzip;q=1, gzip;q=0", gzipQ: 0, defQ: 0, want: ""},
		{header: "gzip;q=.5", gzipQ: 0, defQ: 0, want: ""},
	} {
		t.Run(tc.header, func(t *testing.T) {
			if got := encodingQuality(tc.header, "gzip"); got != tc.gzipQ {
				t.Errorf("gzip quality = %v, want %v", got, tc.gzipQ)
			}
			if got := encodingQuality(tc.header, "deflate"); got != tc.defQ {
				t.Errorf("deflate quality = %v, want %v", got, tc.defQ)
			}
			enc := negotiateEncoder(tc.header, gzip.BestSpeed)
			if tc.want == "" {
				if enc != nil {
					t.Fatalf("encoder = %q, want none", enc.Encoding())
				}
				return
			}
			if enc == nil || enc.Encoding() != tc.want {
				t.Fatalf("encoder = %v, want %q", enc, tc.want)
			}
		})
	}
}

func TestEncodersNormalizeInvalidLevels(t *testing.T) {
	source := []byte(strings.Repeat("bounded compression level ", 20))
	for _, level := range []int{-100, 100} {
		t.Run(strconv.Itoa(level), func(t *testing.T) {
			for _, encoder := range []Encoder{NewGzipEncoder(level), NewDeflateEncoder(level)} {
				var dst bytes.Buffer
				if err := encoder.Encode(&dst, source); err != nil {
					t.Fatalf("%s encode: %v", encoder.Encoding(), err)
				}
				var reader io.ReadCloser
				var err error
				if encoder.Encoding() == EncodingGzip {
					reader, err = gzip.NewReader(&dst)
				} else {
					reader = flate.NewReader(&dst)
				}
				if err != nil {
					t.Fatalf("%s reader: %v", encoder.Encoding(), err)
				}
				decoded, err := io.ReadAll(reader)
				_ = reader.Close()
				if err != nil || !bytes.Equal(decoded, source) {
					t.Fatalf("%s decoded %d bytes, err=%v", encoder.Encoding(), len(decoded), err)
				}
			}
		})
	}
}
