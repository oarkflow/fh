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
	c := &Ctx{server: app}
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
