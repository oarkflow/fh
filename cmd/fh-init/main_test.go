package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain points frontendRepo at a local git fixture repo instead of the
// real git@oarkflow:oarkflow/lithe-boilerplate.git remote, so the test suite
// never needs network/SSH access and stays deterministic. cloneFrontend
// itself is exercised for real (a real `git clone`, just against a local
// path) - only the remote is swapped.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "fh-init-frontend-fixture-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "fh-init test setup:", err)
		os.Exit(1)
	}
	if err := buildFrontendFixtureRepo(dir); err != nil {
		fmt.Fprintln(os.Stderr, "fh-init test setup:", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	frontendRepo = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// buildFrontendFixtureRepo writes a minimal stand-in for the real frontend
// boilerplate - just enough for TestRunGeneratesCompleteApplication's
// assertions - and commits it as a local git repo cloneFrontend can clone
// from by plain filesystem path.
func buildFrontendFixtureRepo(dir string) error {
	files := map[string]string{
		"package.json":              `{"name":"fh-control-center-fixture"}`,
		"src/index.tsx":             `// fixture entry point`,
		"src/app.tsx":               `// fixture composition root`,
		"src/state/session.ts":      `// fixture session state`,
		"src/styles/app.css":        `:root { --fixture: 1; }`,
		".gitignore":                "node_modules/\ndist/\n",
		"src/lib/secure-client.ts": "" +
			"export const authApi = {\n" +
			"  session: () => fetch('/auth/session'),\n" +
			"  login: () => fetch('/auth/login'),\n" +
			"  logout: () => fetch('/auth/logout'),\n" +
			"};\n" +
			"export const secureApi = {\n" +
			"  bootstrap: () => fetch('/secure-config.json'),\n" +
			"  me: () => fetch('/api/me'),\n" +
			"  echo: () => fetch('/api/echo'),\n" +
			"};\n",
		"src/lib/wasm-bridge.ts": "export const bridge = () => import(`/wasm/index.js`);\n",
	}
	for rel, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			return err
		}
	}
	for _, args := range [][]string{
		{"init", "--quiet", "--initial-branch=main"},
		{"add", "-A"},
		{"-c", "user.name=fh-init tests", "-c", "user.email=fh-init-tests@example.com", "commit", "--quiet", "-m", "fixture"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
		}
	}
	return nil
}

func TestRunGeneratesCompleteApplication(t *testing.T) {
	root := t.TempDir()
	if err := run([]string{"-module", "example.com/acme/orders", "-dir", root}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"go.mod", "cmd/server/main.go", "internal/config/config.go", "internal/auth/auth.go",
		"internal/database/database.go", "internal/httpapi/routes.go", "policy.authz",
		"web/templates/index.html",
		"web/frontend/package.json", "web/frontend/src/index.tsx",
		"web/wasm/securefetch.wasm", "web/wasm/asset-manifest.json", ".env", ".env.example", ".gitignore", "Makefile",
	} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("missing generated file %s: %v", name, err)
		}
	}
	// web/public/{app.js,app.css} are Node build output (see
	// .gitignore.tmpl/web/frontend/README.md) - a bare scaffold never runs
	// npm, so nothing should exist there yet. `make frontend` or
	// `fh-init -verify` builds them.
	for _, name := range []string{"web/public/app.js", "web/public/app.css", "web/public"} {
		if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("expected %s to not exist on a bare (non-verify) scaffold, got err=%v", name, err)
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
	if !strings.Contains(string(templateData), `id="app"`) {
		t.Fatal("generated page is missing the #app mount point")
	}
	// web/public/app.js (the compiled bundle) doesn't exist on a bare
	// scaffold - see the web/public non-existence check below - so assert
	// the same endpoint paths survive in the frontend source that `make
	// frontend`/`-verify` compiles them from.
	secureClientData, err := os.ReadFile(filepath.Join(root, "web/frontend/src/lib/secure-client.ts"))
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"/auth/login", "/auth/logout", "/auth/session", "/secure-config.json", "/api/me", "/api/echo"} {
		if !strings.Contains(string(secureClientData), required) {
			t.Fatalf("generated secure-client.ts is missing %q", required)
		}
	}
	wasmBridgeData, err := os.ReadFile(filepath.Join(root, "web/frontend/src/lib/wasm-bridge.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wasmBridgeData), "/wasm/index.js") {
		t.Fatal("generated wasm-bridge.ts is missing \"/wasm/index.js\"")
	}
	stylesData, err := os.ReadFile(filepath.Join(root, "web/frontend/src/styles/app.css"))
	if err != nil {
		t.Fatal(err)
	}
	if len(stylesData) == 0 {
		t.Fatal("generated app.css source is empty")
	}
	for _, name := range []string{
		"web/frontend/package.json", "web/frontend/src/index.tsx", "web/frontend/src/app.tsx",
		"web/frontend/src/lib/secure-client.ts", "web/frontend/src/lib/wasm-bridge.ts",
		"web/frontend/src/state/session.ts", "web/frontend/.gitignore",
	} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("missing generated frontend source file %s: %v", name, err)
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
