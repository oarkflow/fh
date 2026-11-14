package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeModule(t *testing.T, source, name, spec string, files map[string]string) {
	t.Helper()
	dir := filepath.Join(source, modulesDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "module.json"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	for rel, body := range files {
		p := filepath.Join(dir, "files", rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestResolveModulesOrdersDependenciesFirst(t *testing.T) {
	source := t.TempDir()
	writeModule(t, source, "rbac", `{"name":"rbac","package":"internal/modules/rbac","requires":["audit"]}`, nil)
	writeModule(t, source, "audit", `{"name":"audit","package":"internal/modules/audit"}`, nil)
	got, err := resolveModules(source, []string{"rbac", "audit"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "audit" || got[1].Name != "rbac" {
		t.Fatalf("order = %+v", got)
	}
}

func TestResolveModulesRejectsUnknownCyclicAndInvalid(t *testing.T) {
	source := t.TempDir()
	writeModule(t, source, "a", `{"name":"a","package":"p","requires":["b"]}`, nil)
	writeModule(t, source, "b", `{"name":"b","package":"p","requires":["a"]}`, nil)
	for name, want := range map[string]string{"nope": "unknown module", "a": "cycle", "../x": "invalid module name"} {
		if _, err := resolveModules(source, []string{name}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", name, err, want)
		}
	}
}

func TestApplyModulesRendersFilesEnvAndRegistry(t *testing.T) {
	source, root := t.TempDir(), t.TempDir()
	writeModule(t, source, "audit", `{"name":"audit","package":"internal/modules/audit","env":["APP_AUDIT=1"]}`,
		map[string]string{"internal/modules/audit/audit.go.tmpl": "package audit // __FH_MODULE__\n"})
	if err := os.MkdirAll(filepath.Join(root, "internal", "modules", "enabled"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".env.example"), []byte("A=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	mods, err := resolveModules(source, []string{"audit"})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyModules(source, root, "example.com/app", mods); err != nil {
		t.Fatal(err)
	}
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if got := read("internal/modules/audit/audit.go"); !strings.Contains(got, "example.com/app") {
		t.Errorf("module file not rendered: %q", got)
	}
	if got := read(".env.example"); !strings.Contains(got, "APP_AUDIT=1") || !strings.HasPrefix(got, "A=1\n") {
		t.Errorf(".env.example = %q", got)
	}
	if got := read("internal/modules/enabled/enabled.go"); !strings.Contains(got, `auditmod "example.com/app/internal/modules/audit"`) || !strings.Contains(got, "auditmod.Module()") {
		t.Errorf("registry = %q", got)
	}
}

func TestApplyFrontendModulesCopiesViewsAndWritesRegistry(t *testing.T) {
	root := t.TempDir()
	fe := filepath.Join(root, "web", "frontend")
	write := func(rel, body string) {
		p := filepath.Join(fe, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("src/modules/registry.ts", "export const modules = [];\n")
	write("modules/billing/module.json", `{"name":"billing","index":"src/modules/billing/index.tsx"}`)
	write("modules/billing/src/modules/billing/index.tsx", "export default [];\n")
	write("modules/rbac/module.json", `{"name":"rbac","index":"src/modules/rbac/index.tsx"}`)
	write("modules/rbac/src/modules/rbac/index.tsx", "export default [];\n")
	// settings has no frontend; rbac is not requested
	err := applyFrontendModules(root, []moduleSpec{{Name: "settings"}, {Name: "billing"}})
	if err != nil {
		t.Fatal(err)
	}
	reg, _ := os.ReadFile(filepath.Join(fe, "src", "modules", "registry.ts"))
	if !strings.Contains(string(reg), "import billingmod from './billing/index.tsx';") || !strings.Contains(string(reg), "...billingmod,") || strings.Contains(string(reg), "rbac") {
		t.Fatalf("registry = %s", reg)
	}
	if _, err := os.Stat(filepath.Join(fe, "src", "modules", "billing", "index.tsx")); err != nil {
		t.Fatal("module views were not copied")
	}
	if _, err := os.Stat(filepath.Join(fe, "modules")); !os.IsNotExist(err) {
		t.Fatal("the modules directory must not remain in the generated project")
	}
}
