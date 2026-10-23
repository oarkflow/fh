package main

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestHttpModernExample(t *testing.T) {
	app := buildApp()

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer app.ShutdownWithTimeout(time.Second)
	go func() { _ = app.Serve(ln) }()

	baseURL := "http://" + ln.Addr().String()
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // do not auto-follow redirects in tests
		},
	}

	// 1. Test Favicon middleware (/favicon.ico)
	t.Run("favicon", func(t *testing.T) {
		resp, err := client.Get(baseURL + "/favicon.ico")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK for favicon, got %d", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "image/x-icon") {
			t.Fatalf("expected image/x-icon content-type, got %q", ct)
		}
		etag := resp.Header.Get("ETag")
		if etag == "" {
			t.Fatal("expected non-empty ETag header")
		}
		if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "public") {
			t.Fatalf("expected Cache-Control header, got %q", cc)
		}

		// Validate 304 Not Modified conditional request
		req304, _ := http.NewRequest("GET", baseURL+"/favicon.ico", nil)
		req304.Header.Set("If-None-Match", etag)
		resp304, err := client.Do(req304)
		if err != nil {
			t.Fatal(err)
		}
		defer resp304.Body.Close()
		if resp304.StatusCode != http.StatusNotModified {
			t.Fatalf("expected 304 Not Modified, got %d", resp304.StatusCode)
		}
	})

	// 2. Test Trailing Slash middleware (/resources/ -> rewritten to /resources)
	t.Run("trailing_slash_rewrite", func(t *testing.T) {
		resp, err := client.Get(baseURL + "/resources/")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK via trailing slash rewrite, got %d", resp.StatusCode)
		}
	})

	// 3. Test Redirect middleware (/old-priority -> 301 redirect to /priority)
	t.Run("path_redirect", func(t *testing.T) {
		resp, err := client.Get(baseURL + "/old-priority")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMovedPermanently {
			t.Fatalf("expected 301 Moved Permanently for /old-priority, got %d", resp.StatusCode)
		}
		if loc := resp.Header.Get("Location"); loc != "/priority" {
			t.Fatalf("expected Location /priority, got %q", loc)
		}
	})

	// 4. Test normal route (/priority)
	t.Run("priority_route", func(t *testing.T) {
		resp, err := client.Get(baseURL + "/priority")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK for /priority, got %d", resp.StatusCode)
		}
	})
}
