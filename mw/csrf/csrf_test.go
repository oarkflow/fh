package csrf

import "testing"

func TestPartialConfigPreservesSecureDefaults(t *testing.T) {
	cfg := DefaultConfig
	merge(&cfg, Config{})
	if !cfg.CookieSecure {
		t.Fatal("partial config disabled Secure cookies")
	}
	if !cfg.RequireOriginHeader {
		t.Fatal("partial config disabled origin validation")
	}
}

func TestInsecureCompatibilityRequiresExplicitOptOut(t *testing.T) {
	cfg := DefaultConfig
	merge(&cfg, Config{AllowInsecureCookie: true, AllowMissingOrigin: true})
	if cfg.CookieSecure || cfg.RequireOriginHeader {
		t.Fatalf("explicit compatibility opt-out was not applied: %#v", cfg)
	}
}
