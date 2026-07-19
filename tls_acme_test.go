package fh

import (
	"crypto/tls"
	"path/filepath"
	"testing"
)

func TestNewACMEManagerValidation(t *testing.T) {
	t.Run("empty domains", func(t *testing.T) {
		_, err := NewACMEManager(ACMEOptions{CacheDir: t.TempDir()})
		if err == nil {
			t.Fatal("expected error for empty Domains")
		}
	})
	t.Run("empty cache dir", func(t *testing.T) {
		_, err := NewACMEManager(ACMEOptions{Domains: []string{"example.com"}})
		if err == nil {
			t.Fatal("expected error for empty CacheDir")
		}
	})
	t.Run("valid", func(t *testing.T) {
		mgr, err := NewACMEManager(ACMEOptions{
			Domains:  []string{"example.com"},
			CacheDir: filepath.Join(t.TempDir(), "acme-cache"),
			Email:    "ops@example.com",
		})
		if err != nil {
			t.Fatal(err)
		}
		if mgr.Cache == nil {
			t.Fatal("expected a Cache to be configured")
		}
		if mgr.HostPolicy == nil {
			t.Fatal("expected a default HostPolicy to be configured")
		}
	})
}

func TestNewACMEManagerHostPolicyRejectsUnmanagedHost(t *testing.T) {
	mgr, err := NewACMEManager(ACMEOptions{
		Domains:  []string{"example.com"},
		CacheDir: filepath.Join(t.TempDir(), "acme-cache"),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mgr.GetCertificate(&tls.ClientHelloInfo{ServerName: "not-managed.example.org"})
	if err == nil {
		t.Fatal("expected GetCertificate to reject a host outside Domains")
	}
}

func TestNewACMEManagerTLSALPN01NoPanicWithoutCachedCert(t *testing.T) {
	mgr, err := NewACMEManager(ACMEOptions{
		Domains:  []string{"example.com"},
		CacheDir: filepath.Join(t.TempDir(), "acme-cache"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// tls-alpn-01 challenge validation reaches GetCertificate with the
	// "acme-tls/1" ALPN identifier. Without a live ACME order in flight this
	// must fail cleanly (not panic), since there is no pending challenge.
	_, err = mgr.GetCertificate(&tls.ClientHelloInfo{
		ServerName:      "example.com",
		SupportedProtos: []string{"acme-tls/1"},
	})
	if err == nil {
		t.Fatal("expected an error with no pending tls-alpn-01 challenge and no cached cert")
	}
}

func TestAutoTLSConfigForcesTLS13AndKeepsAutocertNextProtos(t *testing.T) {
	app := New()
	cfg, err := app.autoTLSConfig([]string{"example.com"}, filepath.Join(t.TempDir(), "acme-cache"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %#x, want TLS 1.3", cfg.MinVersion)
	}
	if cfg.GetCertificate == nil {
		t.Fatal("expected GetCertificate to be wired from the autocert Manager")
	}
	found := map[string]bool{}
	for _, p := range cfg.NextProtos {
		found[p] = true
	}
	for _, want := range []string{"h2", "http/1.1", "acme-tls/1"} {
		if !found[want] {
			t.Fatalf("NextProtos = %v, missing %q", cfg.NextProtos, want)
		}
	}
}

func TestAutoTLSConfigRejectsInvalidOptions(t *testing.T) {
	app := New()
	if _, err := app.autoTLSConfig(nil, t.TempDir()); err == nil {
		t.Fatal("expected error for empty domains")
	}
}
