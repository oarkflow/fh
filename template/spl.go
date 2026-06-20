// Package template provides adapter implementations for fasthttp's TemplateEngine
// interface, including a ready-to-use adapter for github.com/oarkflow/spl.
package template

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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

	// Globals are template variables merged into every render.
	Globals map[string]any
}

// SPLEngine wraps github.com/oarkflow/spl to implement fasthttp.TemplateEngine.
type SPLEngine struct {
	engine    *spl.Engine
	cfg       SPLConfig
	extension string
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
	e.engine.SecureMode = cfg.SecureMode
	e.engine.AutoEscape = true
	for k, v := range cfg.Globals {
		e.engine.Globals[k] = v
	}
	if cfg.Extension != "" {
		e.extension = cfg.Extension
	}
	return e
}

// RuntimeJS returns the JavaScript runtime for SPL hydration.
func (e *SPLEngine) RuntimeJS() string {
	return e.engine.RuntimeJS()
}

// Render implements fasthttp.TemplateEngine.
func (e *SPLEngine) Render(w io.Writer, name string, data any, layout ...string) error {
	if e.cfg.Reload {
		e.engine.InvalidateCaches()
	}

	binding, ok := data.(map[string]any)
	if !ok {
		if data != nil {
			return fmt.Errorf("spl: data must be map[string]any, got %T", data)
		}
		binding = make(map[string]any)
	}

	for k, v := range e.engine.Globals {
		if _, exists := binding[k]; !exists {
			binding[k] = v
		}
	}

	if !strings.HasSuffix(name, e.extension) {
		name += e.extension
	}

	if len(layout) > 0 && layout[0] != "" {
		layoutName := layout[0]
		if !strings.HasSuffix(layoutName, e.extension) {
			layoutName += e.extension
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
		_, err = io.WriteString(w, out)
		return err
	}

	var out string
	var err error
	if e.cfg.SSR {
		out, err = e.engine.RenderSSRFile(name, binding)
	} else {
		out, err = e.engine.RenderFile(name, binding)
	}
	if err != nil {
		return fmt.Errorf("spl: render %s: %w", name, err)
	}
	_, err = io.WriteString(w, out)
	return err
}
