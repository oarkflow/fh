package fh_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/oarkflow/fh"
)

func TestExposedEndpointsUnauthenticatedByDefaultAreFlagged(t *testing.T) {
	dir := t.TempDir()
	app := fh.NewWithConfig(fh.Config{
		Compliance: fh.ComplianceConfig{Enabled: true, ExposeEndpoints: true},
		Reliability: fh.ReliabilityConfig{
			DataDir: filepath.Join(dir, "rel"),
		},
	})
	findings := app.ValidateSecurity()
	var found bool
	for _, f := range findings {
		if f.Code == "COMPLIANCE_ENDPOINTS_UNAUTHENTICATED" {
			found = true
			if !strings.EqualFold(f.Severity, "critical") {
				t.Fatalf("expected critical severity, got %q", f.Severity)
			}
		}
	}
	if !found {
		t.Fatal("expected COMPLIANCE_ENDPOINTS_UNAUTHENTICATED finding when ExposeEndpoints is set with no EndpointAuth")
	}
}

func TestExposedEndpointsWithAuthAreNotFlaggedAndEnforceAuth(t *testing.T) {
	dir := t.TempDir()
	authCalls := 0
	authMW := func(c fh.Ctx) error {
		authCalls++
		if c.Get("X-Admin-Token") != "secret" {
			return c.Status(fh.StatusUnauthorized).SendString("unauthorized")
		}
		return c.Next()
	}
	app := fh.NewWithConfig(fh.Config{
		Compliance: fh.ComplianceConfig{
			Enabled:         true,
			ExposeEndpoints: true,
			EndpointAuth:    []fh.HandlerFunc{authMW},
		},
		Reliability: fh.ReliabilityConfig{
			DataDir: filepath.Join(dir, "rel"),
		},
	})

	for _, f := range app.ValidateSecurity() {
		if f.Code == "COMPLIANCE_ENDPOINTS_UNAUTHENTICATED" {
			t.Fatal("did not expect COMPLIANCE_ENDPOINTS_UNAUTHENTICATED finding when EndpointAuth is set")
		}
	}

	addr := testServer(t, app)

	code, _ := doRequest(t, addr, "GET", "/_fh/routes", "", nil)
	if code != fh.StatusUnauthorized {
		t.Fatalf("expected /_fh/routes to require auth, got %d", code)
	}
	code, _ = doRequest(t, addr, "GET", "/_fh/compliance", "", nil)
	if code != fh.StatusUnauthorized {
		t.Fatalf("expected /_fh/compliance to require auth, got %d", code)
	}
	code, _ = doRequest(t, addr, "GET", "/_fh/runtime", "", nil)
	if code != fh.StatusUnauthorized {
		t.Fatalf("expected /_fh/runtime to require auth, got %d", code)
	}
	code, _ = doRequest(t, addr, "GET", "/_fh/health", "", nil)
	if code != fh.StatusUnauthorized {
		t.Fatalf("expected /_fh/health to require auth, got %d", code)
	}

	code, _ = doRequest(t, addr, "GET", "/_fh/routes", "", map[string]string{"X-Admin-Token": "secret"})
	if code != fh.StatusOK {
		t.Fatalf("expected authenticated /_fh/routes to succeed, got %d", code)
	}
	if authCalls == 0 {
		t.Fatal("expected auth middleware to have been invoked")
	}
}
