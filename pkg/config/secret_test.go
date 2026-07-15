package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecretStringPrefersExplicitSingleSource(t *testing.T) {
	t.Setenv("TEST_SECRET", "environment-value")
	got, err := SecretString("TEST_SECRET", "TEST_SECRET_FILE")
	if err != nil || got != "environment-value" {
		t.Fatalf("SecretString() = %q, %v", got, err)
	}

	t.Setenv("TEST_SECRET", "")
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("  mounted-value  \n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SECRET_FILE", path)
	got, err = SecretString("TEST_SECRET", "TEST_SECRET_FILE")
	if err != nil || got != "  mounted-value  " {
		t.Fatalf("SecretString() = %q, %v", got, err)
	}
}

func TestSecretStringRejectsAmbiguousAndOversizedSources(t *testing.T) {
	t.Setenv("TEST_SECRET", "value")
	t.Setenv("TEST_SECRET_FILE", filepath.Join(t.TempDir(), "unused"))
	if _, err := SecretString("TEST_SECRET", "TEST_SECRET_FILE"); err == nil {
		t.Fatal("expected ambiguous sources to fail")
	}

	t.Setenv("TEST_SECRET", "")
	path := filepath.Join(t.TempDir(), "large-secret")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxSecretFileSize+1)), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_SECRET_FILE", path)
	if _, err := SecretString("TEST_SECRET", "TEST_SECRET_FILE"); err == nil {
		t.Fatal("expected oversized secret file to fail")
	}
}

func TestRequireSecretString(t *testing.T) {
	if _, err := RequireSecretString("MISSING_SECRET", "MISSING_SECRET_FILE"); err == nil {
		t.Fatal("expected missing required secret to fail")
	}
}
