package fh

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAllowedHostsRejectsUnknownHost(t *testing.T) {
	app := New(WithAllowedHosts("api.example.com"))
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	resp, err := app.Test(httptest.NewRequest("GET", "http://evil.example/", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, StatusBadRequest)
	}
}

func TestAllowedHostCaseAndPortMatching(t *testing.T) {
	allowed := normalizeAllowedHosts([]string{" api.example.com ", "ADMIN.example.com:9443"})
	for _, host := range []string{"api.example.com", "API.EXAMPLE.COM:8443", "admin.example.com:9443"} {
		if !allowedHost(host, allowed) {
			t.Errorf("allowedHost(%q) = false", host)
		}
	}
	for _, host := range []string{"evil.example.com", "api.example.com.evil.test", ""} {
		if allowedHost(host, allowed) {
			t.Errorf("allowedHost(%q) = true", host)
		}
	}
}

func TestAllowedHostsNormalizeAtConstructionAndFailClosed(t *testing.T) {
	app := New(WithAllowedHosts(" api.example.com "))
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	resp, err := app.Test(httptest.NewRequest("GET", "http://API.EXAMPLE.COM:8443/", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != StatusOK {
		t.Fatalf("normalized allow-list status = %d, want %d", resp.StatusCode, StatusOK)
	}

	closed := New(WithAllowedHosts(" "))
	closed.Get("/", func(c Ctx) error { return c.SendString("ok") })
	resp, err = closed.Test(httptest.NewRequest("GET", "http://api.example.com/", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != StatusBadRequest {
		t.Fatalf("whitespace-only allow-list status = %d, want %d", resp.StatusCode, StatusBadRequest)
	}
}

func TestConfiguredContentSecurityPolicy(t *testing.T) {
	app := New(WithSecureByDefault(true), WithContentSecurityPolicy("default-src 'none'; frame-ancestors 'none'"))
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	resp, err := app.Test(httptest.NewRequest("GET", "http://api.example.com/", nil))
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Get("Content-Security-Policy"); !strings.Contains(got, "default-src 'none'") {
		t.Fatalf("CSP = %q", got)
	}
}
