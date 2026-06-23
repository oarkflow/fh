package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/spl"
)

type splEngine struct {
	engine  *spl.Engine
	dir     string
	ext     string
	ssr     bool
	globals map[string]any
	assets  map[string]string
}

func newSPLEngine(dir string) *splEngine {
	e := spl.New()
	e.SecureMode = false
	e.AutoEscape = true
	e.BaseDir = dir
	return &splEngine{
		engine:  e,
		dir:     dir,
		ext:     ".html",
		globals: make(map[string]any),
		assets:  make(map[string]string),
	}
}

func (e *splEngine) SetGlobals(g map[string]any) {
	for k, v := range g {
		e.globals[k] = v
	}
}

func (e *splEngine) RuntimeJS() string {
	// SPL v0.0.7 serializes compatibility-mode @handler bodies as strings, but
	// its runtime does not mark the evaluator as enabled before booting them.
	// Seed the flag in the served runtime itself; no page-level script is needed.
	return `window.__SPL__=window.__SPL__||{};window.__SPL__.compiledWithUnsafeEval=true;` + e.engine.RuntimeJS()
}

func (e *splEngine) HydrationAsset(name string) (string, bool) {
	js, ok := e.assets[name]
	return js, ok
}

func (e *splEngine) HydrationAssets(prefix string) {
	prefix = strings.TrimRight(prefix, "/")
	e.engine.HydrationAssetURL = func(js string) string {
		name := "spl-hydration." + runtimeAssetVersion(js) + ".js"
		e.assets[name] = js
		if prefix == "" {
			return "/" + name
		}
		return prefix + "/" + name
	}
}

func runtimeAssetVersion(src string) string {
	sum := sha256.Sum256([]byte(src))
	return fmt.Sprintf("%x", sum[:8])
}

func (e *splEngine) Render(w io.Writer, name string, data any, layout ...string) error {
	input, ok := data.(map[string]any)
	if !ok {
		if data != nil {
			return fmt.Errorf("spl: data must be map[string]any, got %T", data)
		}
		input = nil
	}
	binding := make(map[string]any, len(input)+len(e.globals))
	for k, v := range input {
		binding[k] = v
	}
	for k, v := range e.globals {
		if _, exists := binding[k]; !exists {
			binding[k] = v
		}
	}

	tmplName := normalizeName(name, e.ext)

	if len(layout) > 0 && layout[0] != "" {
		layoutName := normalizeName(layout[0], e.ext)
		tmplPath := filepath.Join(e.dir, tmplName)
		content, err := os.ReadFile(tmplPath)
		if err != nil {
			return fmt.Errorf("spl: read %s: %w", tmplName, err)
		}
		wrapped := fmt.Sprintf("@extends(%q)\n%s", layoutName, string(content))
		var out string
		if e.ssr {
			out, err = e.engine.RenderSSR(wrapped, binding)
		} else {
			out, err = e.engine.Render(wrapped, binding)
		}
		if err != nil {
			return fmt.Errorf("spl: render %s with layout %s: %w", tmplName, layoutName, err)
		}
		_, err = io.WriteString(w, out)
		return err
	}

	var out string
	var err error
	if e.ssr {
		out, err = e.engine.RenderSSRFile(tmplName, binding)
	} else {
		out, err = e.engine.RenderFile(tmplName, binding)
	}
	if err != nil {
		return fmt.Errorf("spl: render %s: %w", tmplName, err)
	}
	_, err = io.WriteString(w, out)
	return err
}

func normalizeName(name, ext string) string {
	clean := filepath.Clean(name)
	if !strings.HasSuffix(clean, ext) {
		clean += ext
	}
	return clean
}

func main() {
	splEngine := newSPLEngine("views")
	splEngine.ssr = true
	splEngine.SetGlobals(map[string]any{"siteName": "SPL File Upload Demo"})
	runtimeJS := splEngine.RuntimeJS()
	runtimeName := "spl-runtime." + runtimeAssetVersion(runtimeJS) + ".js"
	splEngine.engine.HydrationRuntimeURL = "/static/" + runtimeName
	splEngine.HydrationAssets("/static/hydration")

	app := fh.New(
		fh.WithReadTimeout(10*time.Second),
		fh.WithWriteTimeout(10*time.Second),
		fh.WithMaxRequestBodySize(32*1024*1024),
		fh.WithTemplateEngine(splEngine),
	)

	// CSRF must run before pages/API routes.
	// A safe GET creates the csrf_token cookie and exposes the token through
	// c.Locals("csrf_token"). Unsafe methods must send the same value in
	// X-CSRF-Token while the browser sends the cookie automatically.
	app.Use(csrf.New())

	app.Get("/static/"+runtimeName, func(c *fh.Ctx) error {
		c.Set("Content-Type", "application/javascript")
		c.Set("Cache-Control", "public, max-age=31536000, immutable")
		return c.SendString(runtimeJS)
	})
	app.Get("/static/hydration/:asset", func(c *fh.Ctx) error {
		asset, ok := splEngine.HydrationAsset(c.Param("asset"))
		if !ok {
			return c.SendStatus(404)
		}
		c.Set("Content-Type", "application/javascript; charset=utf-8")
		c.Set("Cache-Control", "public, max-age=31536000, immutable")
		return c.SendString(asset)
	})

	app.Static("/uploads", "./uploads", fh.StaticConfig{})

	app.Get("/", func(c *fh.Ctx) error {
		return c.Render("upload", map[string]any{
			"title":     "File Upload with Reactivity + CSRF",
			"csrfToken": c.Locals("csrf_token"),
		})
	})

	app.Get("/csrf-token", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"token": c.Locals("csrf_token"),
		})
	})

	app.Post("/upload", func(c *fh.Ctx) error {
		file, err := c.FormFile("file")
		if err != nil {
			return c.Status(400).JSON(map[string]any{
				"submitted": true,
				"success":   false,
				"error":     "Select a file before uploading",
			})
		}

		if err := os.MkdirAll("uploads", 0755); err != nil {
			return c.Status(500).JSON(map[string]any{
				"submitted": true,
				"success":   false,
				"error":     "Could not prepare the upload directory",
			})
		}

		originalName := filepath.Base(file.FileName)
		if originalName == "." || originalName == string(filepath.Separator) {
			return c.Status(400).JSON(map[string]any{
				"submitted": true,
				"success":   false,
				"error":     "Invalid file name",
			})
		}
		timestamp := time.Now().UnixMilli()
		savedName := fmt.Sprintf("%d_%s", timestamp, originalName)
		dstPath := filepath.Join("uploads", savedName)

		if err := c.SaveFile(file, dstPath); err != nil {
			return c.Status(500).JSON(map[string]any{
				"submitted": true,
				"success":   false,
				"error":     "Could not save the uploaded file",
			})
		}

		contentType := file.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		return c.JSON(map[string]any{
			"submitted":    true,
			"success":      true,
			"originalName": originalName,
			"savedName":    savedName,
			"size":         file.Size,
			"mimeType":     contentType,
			"url":          "/uploads/" + url.PathEscape(savedName),
			"uploadedAt":   time.Now().Format(time.RFC3339),
		})
	})

	addr := ":8082"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	log.Printf("Listening on %s", addr)
	log.Fatal(app.Listen(addr))
}
