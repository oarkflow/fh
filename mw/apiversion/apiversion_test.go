package apiversion_test

import (
	"net/http/httptest"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apiversion"
)

// TestDefaultHeaderNameUsedWhenUnset ensures the documented default header
// ("Accept-Version") is actually wired up when Config.Header is left empty.
func TestDefaultHeaderNameUsedWhenUnset(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{}))

	var got any
	app.Get("/x", func(c fh.Ctx) error {
		got = c.Locals("api_version")
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "v2")
	resp, err := app.Test(req)
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if got != "v2" {
		t.Fatalf("expected api_version=v2, got %#v", got)
	}
}

// TestDefaultVersionUsedWhenHeaderMissing verifies Config.Default is applied
// when the client sends no version header at all.
func TestDefaultVersionUsedWhenHeaderMissing(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{Default: "v1"}))

	var got any
	app.Get("/x", func(c fh.Ctx) error {
		got = c.Locals("api_version")
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if got != "v1" {
		t.Fatalf("expected api_version=v1, got %#v", got)
	}
}

// TestUnsupportedVersionRejected is the fail-closed check: a version not on
// the allow-list must be rejected with 400, not silently passed through.
func TestUnsupportedVersionRejected(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{Supported: []string{"v1", "v2"}}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "v3")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusBadRequest {
		t.Fatalf("expected 400 for an unsupported version, got %d", resp.StatusCode)
	}
}

// TestSupportedVersionMatchIsCaseInsensitive checks contains()'s documented
// EqualFold comparison.
func TestSupportedVersionMatchIsCaseInsensitive(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{Supported: []string{"v1"}}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "V1")
	resp, err := app.Test(req)
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("expected a case-insensitive supported-version match to pass, status=%v err=%v", resp, err)
	}
}

// TestNoSupportedListAllowsAnyVersion confirms Supported is opt-in: leaving
// it empty must not reject any version, including an unrecognized one.
func TestNoSupportedListAllowsAnyVersion(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{}))

	var got any
	app.Get("/x", func(c fh.Ctx) error {
		got = c.Locals("api_version")
		return c.SendString("ok")
	})

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "totally-made-up")
	resp, err := app.Test(req)
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if got != "totally-made-up" {
		t.Fatalf("expected the raw requested version to be recorded, got %#v", got)
	}
}

// TestDeprecatedVersionSetsSunsetHeaders checks the informational
// deprecation signal is emitted for an exact-match deprecated version.
func TestDeprecatedVersionSetsSunsetHeaders(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{Deprecated: map[string]string{"v1": "2026-01-01"}}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "v1")
	resp, err := app.Test(req)
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if got := resp.Header.Get("Sunset"); got != "2026-01-01" {
		t.Fatalf("expected Sunset header, got %q", got)
	}
	if got := resp.Header.Get("Deprecation"); got != "true" {
		t.Fatalf("expected Deprecation: true, got %q", got)
	}
}

// TestNonDeprecatedVersionHasNoSunsetHeader ensures the Sunset/Deprecation
// headers are not stamped on every response regardless of version.
func TestNonDeprecatedVersionHasNoSunsetHeader(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{Deprecated: map[string]string{"v1": "2026-01-01"}}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "v2")
	resp, err := app.Test(req)
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if got := resp.Header.Get("Sunset"); got != "" {
		t.Fatalf("expected no Sunset header for a non-deprecated version, got %q", got)
	}
	if got := resp.Header.Get("Deprecation"); got != "" {
		t.Fatalf("expected no Deprecation header for a non-deprecated version, got %q", got)
	}
}

// TestDeprecatedLookupIsCaseInsensitive is a regression test for a bug
// found while writing these tests: Supported-version matching uses
// strings.EqualFold (case-insensitive), but the Deprecated-map lookup used
// an exact map index. A client sending "V1" against Supported:["v1"] was
// accepted, yet silently never received the Sunset/Deprecation headers that
// "v1" clients got, purely because of header casing. Both lookups must
// agree on case sensitivity.
func TestDeprecatedLookupIsCaseInsensitive(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{
		Supported:  []string{"v1"},
		Deprecated: map[string]string{"v1": "2026-01-01"},
	}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("Accept-Version", "V1")
	resp, err := app.Test(req)
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if got := resp.Header.Get("Sunset"); got != "2026-01-01" {
		t.Fatalf("expected the deprecation Sunset header despite header/map casing mismatch, got %q", got)
	}
	if got := resp.Header.Get("Deprecation"); got != "true" {
		t.Fatalf("expected Deprecation: true despite header/map casing mismatch, got %q", got)
	}
}

// TestEmptyVersionIsRecordedWhenNoDefaultAndNoHeader documents the edge case
// where neither a header nor a Default is present: v is "" and, with no
// Supported restriction, the request still proceeds with an empty
// api_version local.
func TestEmptyVersionIsRecordedWhenNoDefaultAndNoHeader(t *testing.T) {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{}))

	var got any
	var ok bool
	app.Get("/x", func(c fh.Ctx) error {
		got, ok = c.Locals("api_version"), true
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if !ok || got != "" {
		t.Fatalf("expected api_version=\"\" to be recorded, got %#v", got)
	}
}
