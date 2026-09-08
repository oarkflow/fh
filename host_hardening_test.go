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
