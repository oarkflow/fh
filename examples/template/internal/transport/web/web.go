package web

import (
	"embed"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/csrf"
	"github.com/oarkflow/template"
)

//go:embed static/*
var assets embed.FS

type Engine = template.SPLEngine

func NewEngine() *Engine {
	directory := "web/templates"
	if cwd, err := os.Getwd(); err == nil {
		for current := cwd; ; current = filepath.Dir(current) {
			candidate := filepath.Join(current, directory)
			if info, statErr := os.Stat(filepath.Join(candidate, "index.html")); statErr == nil && !info.IsDir() {
				directory = candidate
				break
			}
			parent := filepath.Dir(current)
			if parent == current {
				break
			}
		}
	}
	return template.NewSPL(directory, ".html").Config(template.SPLConfig{Directory: directory, Extension: ".html", SecureMode: true})
}

func Register(app *fh.App, secureCookie bool, appName, addr string, allowedHosts []string) {
	app.Get("/", func(c fh.Ctx) error { return c.Render("index.html", map[string]any{"Name": appName}) })
	app.Get("/static/app.css", func(c fh.Ctx) error {
		data, err := assets.ReadFile("static/app.css")
		if err != nil {
			return err
		}
		c.Type("text/css; charset=utf-8")
		return c.SendBytes(data)
	})
	// SPL secure mode rejects all script tags. The browser form therefore uses
	// a normal POST and carries the CSRF token in its action URL; this adapter
	// moves it into the header expected by fh/mw/csrf before validation.
	csrfHeader := func(c fh.Ctx) error {
		if token := c.Query("csrf_token"); token != "" {
			c.RequestHeader().Set("X-CSRF-Token", token)
		}
		return c.Next()
	}
	form := app.Group("", csrfHeader, csrf.New(csrf.Config{CookieSecure: secureCookie, AllowInsecureCookie: !secureCookie, RequireOriginHeader: secureCookie, AllowMissingOrigin: !secureCookie, TrustedOrigins: trustedOrigins(allowedHosts, addr)}))
	form.Get("/form", func(c fh.Ctx) error { return c.Render("form.html", map[string]any{"Token": c.Locals("csrf_token")}) })
	form.Post("/form", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"ok": true, "message": "CSRF-protected form submission accepted"})
	})
}

func trustedOrigins(hosts []string, addr string) []string {
	origins := make([]string, 0, len(hosts)*4)
	_, port, _ := net.SplitHostPort(addr)
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
			origins = append(origins, strings.TrimRight(host, "/"))
			continue
		}
		originHost := host
		if ip := net.ParseIP(host); ip != nil && strings.Contains(host, ":") {
			originHost = "[" + host + "]"
		}
		origins = append(origins, "http://"+originHost, "https://"+originHost)
		_, _, splitErr := net.SplitHostPort(host)
		if port != "" && splitErr != nil {
			origins = append(origins, "http://"+net.JoinHostPort(host, port), "https://"+net.JoinHostPort(host, port))
		}
	}
	return origins
}
