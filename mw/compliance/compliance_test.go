package compliance_test

import (
	"net/http/httptest"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/compliance"
)

// TestDataPolicyRecordedWhenSensitivitySet ensures a configured data
// sensitivity/category is available to downstream handlers (and, per fh's
// audit wiring, to the audit log) via Locals.
func TestDataPolicyRecordedWhenSensitivitySet(t *testing.T) {
	app := fh.New()
	app.Use(compliance.New(compliance.Config{
		Data: fh.DataPolicy{Sensitivity: "confidential", Categories: []string{"pii"}},
	}))

	var got fh.DataPolicy
	var ok bool
	app.Get("/x", func(c fh.Ctx) error {
		got, ok = c.Locals("fh.data_policy").(fh.DataPolicy)
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if !ok {
		t.Fatal("expected fh.data_policy to be set in Locals")
	}
	if got.Sensitivity != "confidential" || len(got.Categories) != 1 || got.Categories[0] != "pii" {
		t.Fatalf("unexpected data policy: %#v", got)
	}
}

// TestDataPolicyNotRecordedWhenSensitivityEmpty guards against always
// stamping a (possibly stale/zero-value) data policy on every request
// regardless of configuration.
func TestDataPolicyNotRecordedWhenSensitivityEmpty(t *testing.T) {
	app := fh.New()
	app.Use(compliance.New(compliance.Config{}))

	var val any
	app.Get("/x", func(c fh.Ctx) error {
		val = c.Locals("fh.data_policy")
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if val != nil {
		t.Fatalf("expected no data policy in Locals, got %#v", val)
	}
}

// TestRouteSecurityMetadataAlwaysRecorded checks that the underlying
// fh.RouteSecurity metadata is attached regardless of whether any
// enforcement flags are set, so compliance reporting can see every route.
func TestRouteSecurityMetadataAlwaysRecorded(t *testing.T) {
	cfg := fh.RouteSecurityConfig{Scopes: []string{"orders:read"}, DataClass: "restricted"}
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		fh.SetPrincipal(c, fh.Principal{ID: "u1", Scopes: []string{"orders:read"}})
		return c.Next()
	})
	app.Use(compliance.New(compliance.Config{Security: cfg}))

	var got fh.RouteSecurityConfig
	var ok bool
	app.Get("/x", func(c fh.Ctx) error {
		got, ok = c.Locals("fh.route_security").(fh.RouteSecurityConfig)
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
	if !ok || got.DataClass != "restricted" || len(got.Scopes) != 1 || got.Scopes[0] != "orders:read" {
		t.Fatalf("unexpected route security metadata: ok=%v %#v", ok, got)
	}
}

// TestAuthRequiredRejectsAnonymousRequest is the fail-closed check: a route
// marked AuthRequired must reject a request with no principal, rather than
// silently letting it through because compliance's own logic is thin.
func TestAuthRequiredRejectsAnonymousRequest(t *testing.T) {
	app := fh.New()
	app.Use(compliance.New(compliance.Config{Security: fh.RouteSecurityConfig{AuthRequired: true}}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusUnauthorized {
		t.Fatalf("expected 401 for an unauthenticated required-auth route, got %d", resp.StatusCode)
	}
}

// TestAuthRequiredAllowsAuthenticatedPrincipal confirms the middleware is
// not overly strict: once a principal is attached upstream, the request
// proceeds.
func TestAuthRequiredAllowsAuthenticatedPrincipal(t *testing.T) {
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		fh.SetPrincipal(c, fh.Principal{ID: "u1"})
		return c.Next()
	})
	app.Use(compliance.New(compliance.Config{Security: fh.RouteSecurityConfig{AuthRequired: true}}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
}

// TestScopeDeniedForMissingScope ensures scope enforcement actually blocks
// a principal lacking a required scope (403), not just logs a warning.
func TestScopeDeniedForMissingScope(t *testing.T) {
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		fh.SetPrincipal(c, fh.Principal{ID: "u1", Scopes: []string{"orders:read"}})
		return c.Next()
	})
	app.Use(compliance.New(compliance.Config{
		Security: fh.RouteSecurityConfig{AuthRequired: true, Scopes: []string{"orders:write"}},
	}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusForbidden {
		t.Fatalf("expected 403 for a principal missing the required scope, got %d", resp.StatusCode)
	}
}
