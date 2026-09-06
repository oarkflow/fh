package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunGeneratesCompleteApplication(t *testing.T) {
	root := t.TempDir()
	if err := run([]string{"-module", "example.com/acme/orders", "-dir", root}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"go.mod", "cmd/server/main.go", "internal/config/config.go", "internal/auth/auth.go",
		"internal/database/database.go", "internal/httpapi/routes.go", "policy.authz",
		"web/templates/index.html", "web/public/app.js", ".env.example", "Makefile",
	} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("missing generated file %s: %v", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "module example.com/acme/orders") {
		t.Fatal("module path was not applied")
	}
}

func TestRunRejectsNonEmptyDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "existing"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-module", "example.com/app", "-dir", root}); err == nil {
		t.Fatal("expected non-empty directory error")
	}
}
