package maintenance

import "testing"

func TestSwitch(t *testing.T) {
	s := NewSwitch()
	if s.Enabled() {
		t.Fatal("expected disabled")
	}
	if s.Message() != "maintenance" {
		t.Fatalf("default message=%q", s.Message())
	}
	s.Enable("x")
	if !s.Enabled() || s.Message() != "x" {
		t.Fatalf("expected enabled with message x, enabled=%v message=%q", s.Enabled(), s.Message())
	}
	s.Disable()
	if s.Enabled() {
		t.Fatal("expected disabled")
	}
}

func TestNormalizeMaintenancePath(t *testing.T) {
	cfg := normalize(Config{Path: "maintenance"})
	if cfg.Path != "/maintenance" {
		t.Fatalf("path=%q", cfg.Path)
	}
	if cfg.RedirectCode == 0 || cfg.StatusCode == 0 || cfg.RetryAfter == 0 {
		t.Fatalf("defaults not set: %#v", cfg)
	}
}

func TestNormalizeMaintenancePathTrimsSlash(t *testing.T) {
	cfg := normalize(Config{Path: " /maintenance/ "})
	if cfg.Path != "/maintenance" {
		t.Fatalf("path=%q", cfg.Path)
	}
}
