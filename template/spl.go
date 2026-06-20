// Package template provides adapter implementations for fasthttp's TemplateEngine
// interface, including a ready-to-use adapter for github.com/oarkflow/spl.
package template

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/oarkflow/spl"
)

// SPLConfig configures the SPL template engine adapter.
type SPLConfig struct {
	// Directory is the base directory for resolving template files.
	Directory string

	// Extension is the template file extension (default: ".html").
	Extension string

	// Reload enables cache invalidation on every render (for development).
	Reload bool

	// SSR enables Server-Side Rendering with hydration metadata.
	SSR bool

	// SecureMode enables CSP-safe output mode.
	SecureMode bool

	// HydrationRuntimeURL makes SSR pages load the SPL runtime from this URL
	// instead of embedding it inline.
	HydrationRuntimeURL string

	// HydrationAssetPrefix enables content-addressed external hydration
	// programs below this public URL prefix.
	HydrationAssetPrefix string

	// Globals are template variables merged into every render.
	Globals map[string]any
}

// SPLEngine wraps github.com/oarkflow/spl to implement fasthttp.TemplateEngine.
type SPLEngine struct {
	engine          *spl.Engine
	cfg             SPLConfig
	extension       string
	hydrationAssets sync.Map
}

// NewSPL creates a new SPL template engine adapter.
// directory is the base path for template files; extension defaults to ".html".
func NewSPL(directory string, extension ...string) *SPLEngine {
	ext := ".html"
	if len(extension) > 0 && extension[0] != "" {
		ext = extension[0]
	}
	e := &SPLEngine{
		engine:    spl.New(),
		extension: ext,
		cfg: SPLConfig{
			Directory: directory,
			Extension: ext,
			Globals:   make(map[string]any),
		},
	}
	e.engine.AutoEscape = true
	e.engine.BaseDir = directory
	e.engine.SecureMode = true
	return e
}

// Config applies configuration to the SPL engine adapter.
func (e *SPLEngine) Config(cfg SPLConfig) *SPLEngine {
	e.cfg = cfg
	e.engine.BaseDir = cfg.Directory
	if cfg.SecureMode {
		e.engine.SecureMode = true
	}
	e.engine.AutoEscape = true
	e.engine.HydrationRuntimeURL = cfg.HydrationRuntimeURL
	for k, v := range cfg.Globals {
		e.engine.Globals[k] = v
	}
	if cfg.Extension != "" {
		e.extension = cfg.Extension
	}
	if cfg.HydrationAssetPrefix != "" {
		e.HydrationAssets(cfg.HydrationAssetPrefix)
	} else {
		e.engine.HydrationAssetURL = nil
	}
	return e
}

// HydrationRuntimeURL configures an external, cacheable SPL runtime URL.
func (e *SPLEngine) HydrationRuntimeURL(url string) *SPLEngine {
	e.cfg.HydrationRuntimeURL = url
	e.engine.HydrationRuntimeURL = url
	return e
}

// HydrationAssets stores generated hydration programs by content hash and
// emits external script URLs rooted at prefix.
func (e *SPLEngine) HydrationAssets(prefix string) *SPLEngine {
	prefix = strings.TrimRight(prefix, "/")
	e.cfg.HydrationAssetPrefix = prefix
	e.engine.HydrationAssetURL = func(js string) string {
		name := "spl-hydration." + runtimeAssetVersion(js) + ".js"
		e.hydrationAssets.Store(name, js)
		if prefix == "" {
			return "/" + name
		}
		return prefix + "/" + name
	}
	return e
}

// HydrationAsset retrieves a generated hydration program by file name.
func (e *SPLEngine) HydrationAsset(name string) (string, bool) {
	if name == "" || filepath.Base(name) != name {
		return "", false
	}
	asset, ok := e.hydrationAssets.Load(name)
	if !ok {
		return "", false
	}
	js, ok := asset.(string)
	return js, ok
}

// ClearHydrationAssets releases generated assets, useful with Reload in long
// running development processes.
func (e *SPLEngine) ClearHydrationAssets() {
	e.hydrationAssets.Range(func(key, _ any) bool { e.hydrationAssets.Delete(key); return true })
}

// Load parses all templates under the configured directory to warm caches and
// fail startup on template errors.
func (e *SPLEngine) Load() error {
	e.engine.BaseDir = e.cfg.Directory
	return filepath.Walk(e.cfg.Directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, e.extension) {
			return nil
		}
		rel, err := filepath.Rel(e.cfg.Directory, path)
		if err != nil {
			return err
		}
		_, err = e.engine.RenderFile(rel, nil)
		if err != nil {
			return fmt.Errorf("spl: load %s: %w", rel, err)
		}
		return nil
	})
}

// RuntimeJS returns the JavaScript runtime for SPL hydration.
func (e *SPLEngine) RuntimeJS() string {
	return e.engine.RuntimeJS()
}

// RuntimeVersion returns the content hash used for cache-busting the external
// runtime URL.
func (e *SPLEngine) RuntimeVersion() string { return runtimeAssetVersion(e.engine.RuntimeJS()) }

// Render implements fasthttp.TemplateEngine.
func (e *SPLEngine) Render(w io.Writer, name string, data any, layout ...string) error {
	if e.cfg.Reload {
		e.engine.InvalidateCaches()
	}

	input, ok := data.(map[string]any)
	if !ok {
		if data != nil {
			return fmt.Errorf("spl: data must be map[string]any, got %T", data)
		}
		input = nil
	}
	binding := make(map[string]any, len(input)+len(e.engine.Globals))
	for k, v := range input {
		binding[k] = v
	}

	for k, v := range e.engine.Globals {
		if _, exists := binding[k]; !exists {
			binding[k] = v
		}
	}

	name, err := normalizeTemplateName(name, e.extension)
	if err != nil {
		return err
	}

	if len(layout) > 0 && layout[0] != "" {
		layoutName, err := normalizeTemplateName(layout[0], e.extension)
		if err != nil {
			return err
		}
		tmplPath := filepath.Join(e.cfg.Directory, name)
		content, err := os.ReadFile(tmplPath)
		if err != nil {
			return fmt.Errorf("spl: read %s: %w", name, err)
		}
		wrapped := fmt.Sprintf("@extends(%q)\n%s", layoutName, string(content))
		var out string
		if e.cfg.SSR {
			out, err = e.engine.RenderSSR(wrapped, binding)
		} else {
			out, err = e.engine.Render(wrapped, binding)
		}
		if err != nil {
			return fmt.Errorf("spl: render %s with layout %s: %w", name, layoutName, err)
		}
		_, err = io.WriteString(w, moveScriptsInsideBody(out))
		return err
	}

	var out string
	if e.cfg.SSR {
		out, err = e.engine.RenderSSRFile(name, binding)
	} else {
		out, err = e.engine.RenderFile(name, binding)
	}
	if err != nil {
		return fmt.Errorf("spl: render %s: %w", name, err)
	}
	_, err = io.WriteString(w, moveScriptsInsideBody(out))
	return err
}

func runtimeAssetVersion(src string) string {
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:16])
}

func normalizeTemplateName(name, extension string) (string, error) {
	if name == "" || strings.IndexByte(name, 0) >= 0 || filepath.IsAbs(name) {
		return "", fmt.Errorf("spl: invalid template path %q", name)
	}
	clean := filepath.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("spl: template path escapes base directory: %q", name)
	}
	if !strings.HasSuffix(clean, extension) {
		clean += extension
	}
	return clean, nil
}

// moveScriptsInsideBody moves hydration script tags from after </html>
// to just before </body>, ensuring valid HTML structure.
func moveScriptsInsideBody(s string) string {
	bodyIdx := strings.LastIndex(s, `</body>`)
	if bodyIdx < 0 {
		return s
	}
	markers := []string{`<script data-spl-runtime`, `<script data-spl-hydration`, `<script type="application/json" data-spl-hydration>`}
	idx := -1
	for _, marker := range markers {
		if found := strings.Index(s[bodyIdx:], marker); found >= 0 {
			found += bodyIdx
			if idx < 0 || found < idx {
				idx = found
			}
		}
	}
	if idx < 0 {
		return s
	}
	return s[:bodyIdx] + s[idx:] + s[bodyIdx:idx]
}
