package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	for _, key := range []string{"APP_NAME", "APP_ADDR", "APP_ENV", "APP_DEBUG", "APP_ALLOWED_HOSTS", "APP_DEV_AUTH", "APP_REQUEST_TIMEOUT", "APP_MAX_BODY_BYTES", "APP_RATE_LIMIT"} {
		t.Setenv(key, "")
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" || c.MaxBodyBytes <= 0 || c.RateLimit <= 0 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestSecretFileLoading(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-secret")
	secret := strings.Repeat("s", 32)
	if err := os.WriteFile(path, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_SESSION_SECRET_FILE", path)
	value, err := Secret("APP_SESSION_SECRET", "APP_SESSION_SECRET_FILE")
	if err != nil || value != secret {
		t.Fatalf("secret file: value=%q err=%v", value, err)
	}
}

func TestProductionFailsClosed(t *testing.T) {
	base := Config{Name: "app", Addr: ":8080", Environment: "production", RequestTimeout: time.Second, MaxBodyBytes: 1, RateLimit: 1, SessionSecret: strings.Repeat("s", 32)}
	if err := base.Validate(); err == nil || !strings.Contains(err.Error(), "explicit authentication adapter") {
		t.Fatalf("expected missing adapter error, got %v", err)
	}
	base.AuthConfigured = true
	if err := base.Validate(); err == nil || !strings.Contains(err.Error(), "JWT_SECRET") {
		t.Fatalf("expected missing JWT secret error, got %v", err)
	}
	base.JWTSecret = strings.Repeat("j", 32)
	if err := base.Validate(); err != nil {
		t.Fatalf("valid production config was rejected: %v", err)
	}
}

func TestInvalidDurationIsRejected(t *testing.T) {
	t.Setenv("APP_REQUEST_TIMEOUT", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestProductionRejectsDevelopmentAuth(t *testing.T) {
	c := Config{Name: "app", Addr: ":8080", Environment: "production", DevelopmentAuth: true, RequestTimeout: 1, MaxBodyBytes: 1, RateLimit: 1}
	if err := c.Validate(); err == nil {
		t.Fatal("expected production development-auth rejection")
	}
}

func TestInvalidIntegerIsRejected(t *testing.T) {
	t.Setenv("APP_MAX_BODY_BYTES", "not-a-number")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid integer error")
	}
}

func TestLoadBCLFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.bcl")
	if err := os.WriteFile(path, []byte("name \"bcl-app\"\naddr \":9090\"\nenvironment \"test\"\nmax_body_bytes 1234\nrate_limit 7\nrequest_timeout 2s\nallowed_hosts \"localhost\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_CONFIG", path)
	for _, key := range []string{"APP_NAME", "APP_ADDR", "APP_ENV", "APP_DEBUG", "APP_ALLOWED_HOSTS", "APP_DEV_AUTH", "APP_AUTH_CONFIGURED", "APP_REQUEST_TIMEOUT", "APP_MAX_BODY_BYTES", "APP_RATE_LIMIT"} {
		t.Setenv(key, "")
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "bcl-app" || c.Addr != ":9090" || c.MaxBodyBytes != 1234 || c.RateLimit != 7 || c.RequestTimeout.String() != "2s" {
		t.Fatalf("BCL config was not loaded: %+v", c)
	}
}
