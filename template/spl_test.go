package template

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSPLEngineRenderFile(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "hello.html"), []byte("<h1>Hello, ${name}!</h1>"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	out := &bytes.Buffer{}
	err = engine.Render(out, "hello.html", map[string]any{"name": "world"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Hello, world!") {
		t.Fatalf("expected 'Hello, world!', got %q", out.String())
	}
}

func TestSPLEngineAutoExtension(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "page.html"), []byte("<p>${msg}</p>"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	out := &bytes.Buffer{}
	err = engine.Render(out, "page", map[string]any{"msg": "auto-ext"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "auto-ext") {
		t.Fatalf("expected 'auto-ext', got %q", out.String())
	}
}

func TestSPLEngineWithLayout(t *testing.T) {
	dir := t.TempDir()

	err := os.WriteFile(filepath.Join(dir, "layout.html"), []byte("<html><body>@block(\"content\"){default}</body></html>"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(dir, "page.html"), []byte("@extends(\"layout.html\")\n@define(\"content\"){<h1>${title}</h1>}"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	out := &bytes.Buffer{}
	err = engine.Render(out, "page", map[string]any{"title": "My Page"}, "layout.html")
	if err != nil {
		t.Fatal(err)
	}
	result := out.String()
	if !strings.Contains(result, "My Page") {
		t.Fatalf("expected 'My Page', got %q", result)
	}
	if !strings.Contains(result, "<html>") {
		t.Fatalf("expected '<html>', got %q", result)
	}
}

func TestSPLEngineGlobals(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("${siteName} - ${pageTitle}"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	engine.Config(SPLConfig{
		Directory: dir,
		Globals: map[string]any{
			"siteName": "MySite",
		},
	})

	out := &bytes.Buffer{}
	err = engine.Render(out, "index", map[string]any{"pageTitle": "Home"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "MySite - Home") {
		t.Fatalf("expected 'MySite - Home', got %q", out.String())
	}
}

func TestSPLEngineInvalidData(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "test.html"), []byte("test"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	out := &bytes.Buffer{}
	err = engine.Render(out, "test", "not a map")
	if err == nil {
		t.Fatal("expected error for non-map data")
	}
	if !strings.Contains(err.Error(), "data must be map[string]any") {
		t.Fatalf("expected type error, got %v", err)
	}
	_ = out
}

func TestSPLEngineNilData(t *testing.T) {
	dir := t.TempDir()
	engine := NewSPL(dir)
	engine.engine.Globals["msg"] = "ok"
	err := os.WriteFile(filepath.Join(dir, "nil_test.html"), []byte("hello ${msg}"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	out := &bytes.Buffer{}
	err = engine.Render(out, "nil_test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Fatalf("expected 'ok', got %q", out.String())
	}
}

func TestSPLEngineCustomExtension(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "index.spl"), []byte("<h1>${title}</h1>"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir, ".spl")
	out := &bytes.Buffer{}
	err = engine.Render(out, "index", map[string]any{"title": "SPL Page"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "SPL Page") {
		t.Fatalf("expected 'SPL Page', got %q", out.String())
	}
}

func TestSPLEngineSSRWithLayout(t *testing.T) {
	dir := t.TempDir()
	layout := "<html><body>@block(\"content\"){default}</body></html>"
	err := os.WriteFile(filepath.Join(dir, "layout.html"), []byte(layout), 0644)
	if err != nil {
		t.Fatal(err)
	}

	page := `@extends("layout.html")@define("content"){@signal(count = 5)<h1>${count}</h1>@click("inc", count, "inc", "1")}`
	err = os.WriteFile(filepath.Join(dir, "page.html"), []byte(page), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	engine.Config(SPLConfig{
		Directory: dir,
		SSR:       true,
	})

	out := &bytes.Buffer{}
	err = engine.Render(out, "page", nil, "layout.html")
	if err != nil {
		t.Fatal(err)
	}
	result := out.String()

	if !strings.Contains(result, "5") {
		t.Fatalf("expected signal value '5' in output, got %q", result)
	}
	if !strings.Contains(result, "data-spl-hydration") {
		t.Fatalf("expected hydration script in output, got %q", result)
	}

	// Verify hydration scripts are before </body>
	bodyIdx := strings.LastIndex(result, "</body>")
	hydIdx := strings.LastIndex(result, "data-spl-hydration")
	if hydIdx > bodyIdx {
		t.Fatalf("hydration script should be before </body>, found at %d, body at %d", hydIdx, bodyIdx)
	}
}

func TestSPLEngineSSRWithoutLayout(t *testing.T) {
	dir := t.TempDir()
	tmpl := `<html><body>@signal(count = 3)<h1>${count}</h1>@click("inc", count, "inc", "1")</body></html>`
	err := os.WriteFile(filepath.Join(dir, "page.html"), []byte(tmpl), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	engine.Config(SPLConfig{
		Directory: dir,
		SSR:       true,
	})

	out := &bytes.Buffer{}
	err = engine.Render(out, "page", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := out.String()

	if !strings.Contains(result, "3") {
		t.Fatalf("expected signal value '3' in output, got %q", result)
	}
	if !strings.Contains(result, "data-spl-hydration") {
		t.Fatalf("expected hydration script in output, got %q", result)
	}

	bodyIdx := strings.LastIndex(result, "</body>")
	hydIdx := strings.LastIndex(result, "data-spl-hydration")
	if hydIdx > bodyIdx {
		t.Fatalf("hydration script should be before </body>, found at %d, body at %d", hydIdx, bodyIdx)
	}
}

func TestSPLEngineSSRNoBodyTag(t *testing.T) {
	dir := t.TempDir()
	tmpl := `@signal(count = 7)<h1>${count}</h1>@click("inc", count, "inc", "1")`
	err := os.WriteFile(filepath.Join(dir, "page.html"), []byte(tmpl), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	engine.Config(SPLConfig{
		Directory: dir,
		SSR:       true,
	})

	out := &bytes.Buffer{}
	err = engine.Render(out, "page", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := out.String()

	if !strings.Contains(result, "7") {
		t.Fatalf("expected signal value '7' in output, got %q", result)
	}
	if !strings.Contains(result, "data-spl-hydration") {
		t.Fatalf("expected hydration script in output, got %q", result)
	}
}

func TestSPLEngineExternalRuntimeAndHydrationAssets(t *testing.T) {
	dir := t.TempDir()
	tmpl := `<html><body>@signal(count = 3)<button>${count}</button>@click("inc", count, "inc", "1")</body></html>`
	if err := os.WriteFile(filepath.Join(dir, "page.html"), []byte(tmpl), 0600); err != nil {
		t.Fatal(err)
	}
	engine := NewSPL(dir)
	engine.Config(SPLConfig{Directory: dir, SSR: true, SecureMode: true})
	runtimeURL := "/static/spl-runtime.min.js?v=" + engine.RuntimeVersion()
	engine.HydrationRuntimeURL(runtimeURL).HydrationAssets("/static/hydration/")

	var out bytes.Buffer
	if err := engine.Render(&out, "page", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	result := out.String()
	if !strings.Contains(result, `data-spl-runtime src="`+runtimeURL+`"`) {
		t.Fatalf("missing external runtime: %q", result)
	}
	prefix := `data-spl-hydration src="/static/hydration/`
	start := strings.Index(result, prefix)
	if start < 0 {
		t.Fatalf("missing external hydration asset: %q", result)
	}
	start += len(prefix)
	end := strings.Index(result[start:], `"`)
	if end < 0 {
		t.Fatal("unterminated hydration URL")
	}
	name := result[start : start+end]
	if !strings.HasPrefix(name, "spl-hydration.") || !strings.HasSuffix(name, ".js") {
		t.Fatalf("unexpected asset name %q", name)
	}
	asset, ok := engine.HydrationAsset(name)
	if !ok || !strings.Contains(asset, "window.__SPL_HYDRATE__") {
		t.Fatalf("missing hydration program %q", asset)
	}
	if _, ok := engine.HydrationAsset("../" + name); ok {
		t.Fatal("accepted hydration asset traversal")
	}
	if strings.LastIndex(result, "data-spl-hydration") > strings.LastIndex(result, "</body>") {
		t.Fatal("hydration script is outside body")
	}
}

func TestSPLEngineRenderDoesNotMutateBinding(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "page.html"), []byte(`${site}-${page}`), 0600); err != nil {
		t.Fatal(err)
	}
	engine := NewSPL(dir).Config(SPLConfig{Directory: dir, Globals: map[string]any{"site": "global"}})
	binding := map[string]any{"page": "home"}
	var out bytes.Buffer
	if err := engine.Render(&out, "page", binding); err != nil {
		t.Fatal(err)
	}
	if _, mutated := binding["site"]; mutated {
		t.Fatal("render mutated caller binding")
	}
}

func TestSPLEngineRejectsPathTraversal(t *testing.T) {
	engine := NewSPL(t.TempDir())
	for _, name := range []string{"../secret", "/absolute/template"} {
		if err := engine.Render(&bytes.Buffer{}, name, nil); err == nil {
			t.Fatalf("accepted unsafe template path %q", name)
		}
	}
}

func TestSPLEngineNonSSRUnchanged(t *testing.T) {
	dir := t.TempDir()
	tmpl := `<html><body><h1>Hello</h1></body></html>`
	err := os.WriteFile(filepath.Join(dir, "page.html"), []byte(tmpl), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	out := &bytes.Buffer{}
	err = engine.Render(out, "page", nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != tmpl {
		t.Fatalf("non-SSR output should be unchanged, got %q", out.String())
	}
}

func TestSPLEngineReload(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "reload.html"), []byte("v1"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	engine := NewSPL(dir)
	engine.Config(SPLConfig{
		Directory: dir,
		Reload:    true,
	})

	out := &bytes.Buffer{}
	err = engine.Render(out, "reload", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "v1") {
		t.Fatalf("expected 'v1', got %q", out.String())
	}

	err = os.WriteFile(filepath.Join(dir, "reload.html"), []byte("v2"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	out.Reset()
	err = engine.Render(out, "reload", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "v2") {
		t.Fatalf("expected 'v2' after reload, got %q", out.String())
	}
}
