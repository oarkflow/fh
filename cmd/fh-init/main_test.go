package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
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
		"web/templates/index.html", "web/public/app.js", "web/wasm/securefetch.wasm", "web/wasm/asset-manifest.json", ".env", ".env.example", "Makefile",
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
	mainData, err := os.ReadFile(filepath.Join(root, "cmd/server/main.go"))
	if err != nil {
		t.Fatal(err)
	}
	mainSource := string(mainData)
	for _, required := range []string{"WithSecureByDefault(cfg.Production)", "securetransport.Install", "responsemiddleware.New", "operationAuth"} {
		if !strings.Contains(mainSource, required) {
			t.Fatalf("generated server is missing %q", required)
		}
	}
	routesData, err := os.ReadFile(filepath.Join(root, "internal/httpapi/routes.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(routesData), "renderIndex(c, cfg.Production)") {
		t.Fatal("generated server is missing the secure SPL page renderer")
	}
	templateData, err := os.ReadFile(filepath.Join(root, "web/templates/index.html"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(templateData)), "<script") {
		t.Fatal("secure SPL template contains a forbidden script tag")
	}
	if !strings.Contains(string(templateData), `id="logout"`) || !strings.Contains(string(templateData), `id="account-controls" hidden`) {
		t.Fatal("generated page is missing authenticated logout controls")
	}
	appJS, err := os.ReadFile(filepath.Join(root, "web/public/app.js"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"/auth/session", "showAuthenticated(true)", "secure.revokeSession()", "/auth/logout"} {
		if !strings.Contains(string(appJS), required) {
			t.Fatalf("generated browser client is missing %q", required)
		}
	}
	if strings.Contains(mainSource, "__FH_MODULE__") {
		t.Fatal("module placeholder remains in generated server")
	}
	wasm, err := os.ReadFile(filepath.Join(root, "web/wasm/securefetch.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	manifestData, err := os.ReadFile(filepath.Join(root, "web/wasm/asset-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Assets map[string]struct {
			Integrity string `json:"integrity"`
		} `json:"assets"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(wasm)
	wantIntegrity := "sha256-" + base64.StdEncoding.EncodeToString(sum[:])
	if manifest.Assets["securefetch.wasm"].Integrity != wantIntegrity {
		t.Fatal("generated WASM does not match its integrity manifest")
	}
	environment, err := os.ReadFile(filepath.Join(root, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	environmentExample, err := os.ReadFile(filepath.Join(root, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	if string(environment) != string(environmentExample) {
		t.Fatal(".env was not initialized from .env.example")
	}
	if !strings.Contains(string(environment), "APP_ORIGIN=:8080") {
		t.Fatal("generated development origin does not match the documented browser URL")
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

func TestRunForcePreservesEnvironmentFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("APP_LOGIN_PASSWORD=local-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-module", "example.com/app", "-dir", root, "-force"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "APP_LOGIN_PASSWORD=local-secret\n" {
		t.Fatal("-force overwrote the existing .env")
	}
}
