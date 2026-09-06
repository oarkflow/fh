package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunGeneratesAndRewritesTemplate(t *testing.T) {
	dir := t.TempDir()
	if err := run("example.com/acme/orders", dir, "orders", false, false, false); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{".gitignore", "go.mod", "cmd/api/main.go", "internal/app/app.go", "README.md"} {
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			t.Fatalf("missing %s: %v", path, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "module example.com/acme/orders") || strings.Contains(string(data), "replace github.com/oarkflow/fh =>") {
		t.Fatalf("module rewrite or standalone output is wrong:\n%s", data)
	}
	readme, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(readme), "{{APP_NAME}}") {
		t.Fatal("template placeholder remains")
	}
	configData, err := os.ReadFile(filepath.Join(dir, "config.bcl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(configData), `ORDERS_NAME`) || strings.Contains(string(configData), `APP_NAME`) {
		t.Fatalf("configuration prefix was not rewritten:\n%s", configData)
	}
}

func TestRunLocalReplaceAndDryRun(t *testing.T) {
	dir := t.TempDir()
	if err := run("example.com/acme/orders", dir, "orders", false, true, true); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("dry run wrote files")
	}
	if err := run("example.com/acme/orders", dir, "orders", false, false, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "replace github.com/oarkflow/fh =>") {
		t.Fatal("local development mode omitted the fh replace")
	}
}

func TestRunRefusesNonEmptyDestination(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "existing"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run("example.com/acme/orders", dir, "orders", false, false, false); err == nil {
		t.Fatal("expected non-empty destination error")
	}
	if err := run("example.com/acme/orders", dir, "orders", true, false, false); err != nil {
		t.Fatalf("force generation failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "existing"))
	if err != nil || string(data) != "keep" {
		t.Fatalf("force generation changed unrelated file: data=%q err=%v", data, err)
	}
}
