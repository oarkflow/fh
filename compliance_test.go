package fh

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestComplianceDefaultsAndReport(t *testing.T) {
	dir := t.TempDir()
	app := NewWithConfig(Config{Compliance: ComplianceConfig{Enabled: true, Profile: ComplianceEnterprise, ExposeEndpoints: true}, Reliability: ReliabilityConfig{DataDir: filepath.Join(dir, "rel")}, Audit: AuditConfig{FilePath: filepath.Join(dir, "audit.jsonl")}})
	app.Post("/orders", func(c Ctx) error { return c.JSON(Map{"ok": true}) }).WithRouteSecurity(RouteSecurityConfig{AuthRequired: true, IdempotencyRequired: true, Scopes: []string{"orders:create"}, DataClass: "confidential"})
	rep := app.ComplianceReport()
	if rep.Profile != ComplianceEnterprise {
		t.Fatalf("profile = %q", rep.Profile)
	}
	if !rep.Config.ReliabilityEnabled || !rep.Config.AuditEnabled || !rep.Config.RedactionEnabled {
		t.Fatalf("expected secure defaults: %#v", rep.Config)
	}
	if len(rep.Controls) == 0 {
		t.Fatal("expected controls")
	}
	var found bool
	for _, r := range rep.Routes {
		if r.Path == "/orders" && r.Security.IdempotencyRequired && r.Data.Sensitivity == "confidential" {
			found = true
		}
	}
	if !found {
		b, _ := json.MarshalIndent(rep.Routes, "", "  ")
		t.Fatalf("route metadata missing: %s", b)
	}
}

func TestAuditSinkAndPrincipal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	app := NewWithConfig(Config{Audit: AuditConfig{Enabled: true, FilePath: path}, Redaction: DefaultRedactionConfig()})
	c := &DefaultCtx{server: app}
	c.reset()
	SetPrincipal(c, Principal{ID: "u1", Type: "user", TenantID: "t1"})
	if err := c.Audit().Record("secret.changed", "user", "u1", Map{"password": "bad", "note": "ok"}); err != nil {
		t.Fatal(err)
	}
	if closer, ok := app.audit.(AuditSinkCloser); ok {
		_ = closer.Close()
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b[:len(b)-1]) {
		t.Fatalf("invalid audit jsonl: %s", b)
	}
	if string(b) == "" || !containsString(string(b), "[REDACTED]") {
		t.Fatalf("audit not redacted: %s", b)
	}
}

func TestComplianceStrictFindings(t *testing.T) {
	app := NewWithConfig(Config{Mode: ModeStrict, Debug: true, Compliance: ComplianceConfig{Enabled: true, Strict: true}, Reliability: ReliabilityConfig{Enabled: false}})
	findings := app.ValidateSecurity()
	if !hasCritical(findings) {
		t.Fatalf("expected critical finding, got %#v", findings)
	}
	_ = time.Now()
}

func containsString(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && (s == sub || containsString(s[1:], sub) || s[:len(sub)] == sub))
}

func findingByCode(findings []SecurityFinding, code string) (SecurityFinding, bool) {
	for _, f := range findings {
		if f.Code == code {
			return f, true
		}
	}
	return SecurityFinding{}, false
}

// TestNewProductionAloneFlagsMissingHostPolicyAndScopeReminder guards the
// "NewProduction()/WithSecureByDefault(true) is unmistakable" fix: neither
// enables a Host allow-list, and ValidateSecurity must say so explicitly
// rather than silently accepting any Host header, plus always carry the
// SECURE_DEFAULTS_SCOPE reminder that protocol hardening isn't auth/CSRF/
// rate-limiting.
func TestNewProductionAloneFlagsMissingHostPolicyAndScopeReminder(t *testing.T) {
	app := NewProduction(WithStartupBannerDisabled(true))
	findings := app.ValidateSecurity()

	if _, ok := findingByCode(findings, "HOST_POLICY_MISSING"); !ok {
		t.Fatalf("expected HOST_POLICY_MISSING finding for NewProduction() with no AllowedHosts, got %#v", findings)
	}
	if _, ok := findingByCode(findings, "SECURE_DEFAULTS_SCOPE"); !ok {
		t.Fatalf("expected SECURE_DEFAULTS_SCOPE reminder finding, got %#v", findings)
	}
}

// TestAllowedHostsClearsHostPolicyFinding proves the check is a real signal,
// not always-on noise.
func TestAllowedHostsClearsHostPolicyFinding(t *testing.T) {
	app := NewProduction(WithAllowedHosts("api.example.com"), WithStartupBannerDisabled(true))
	findings := app.ValidateSecurity()
	if _, ok := findingByCode(findings, "HOST_POLICY_MISSING"); ok {
		t.Fatalf("expected no HOST_POLICY_MISSING finding once AllowedHosts is set, got %#v", findings)
	}
}

// TestSecureByDefaultAloneStillCarriesScopeReminder proves the reminder isn't
// gated only on Mode — SecureByDefault(true) with the default (fast/dev-ish)
// mode still gets it, since it's the specific thing this fix must never let
// slip through: "secure" alone must never read as "auth/CSRF/rate-limiting
// included".
func TestSecureByDefaultAloneStillCarriesScopeReminder(t *testing.T) {
	app := New(WithSecureByDefault(true), WithStartupBannerDisabled(true))
	findings := app.ValidateSecurity()
	if _, ok := findingByCode(findings, "SECURE_DEFAULTS_SCOPE"); !ok {
		t.Fatalf("expected SECURE_DEFAULTS_SCOPE reminder finding for SecureByDefault(true), got %#v", findings)
	}
}

// TestStartupBannerCarriesSecureDefaultsNotice proves the reminder is also
// unmissable at the one place every operator actually looks: process
// startup output, not just a report endpoint nobody queries by default.
func TestStartupBannerCarriesSecureDefaultsNotice(t *testing.T) {
	app := NewProduction()
	data := app.startupBannerData(nil)
	if data.SecureDefaultsNotice == "" {
		t.Fatal("expected non-empty SecureDefaultsNotice for NewProduction()")
	}
	if !containsString(data.SecureDefaultsNotice, "does NOT add authentication") {
		t.Fatalf("expected notice to state the scope explicitly, got %q", data.SecureDefaultsNotice)
	}

	appHidden := NewProduction(WithStartupBanner(StartupBannerConfig{HideSecureDefaultsNotice: true}))
	if got := appHidden.startupBannerData(nil).SecureDefaultsNotice; got != "" {
		t.Fatalf("expected empty notice when HideSecureDefaultsNotice is set, got %q", got)
	}

	appFast := NewFast()
	if got := appFast.startupBannerData(nil).SecureDefaultsNotice; got != "" {
		t.Fatalf("expected no notice for a non-production, non-SecureByDefault app, got %q", got)
	}
}
