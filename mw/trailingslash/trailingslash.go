package trailingslash

import (
	"strings"

	"github.com/oarkflow/fh"
)

// Config defines the configuration options for trailing slash normalization.
type Config struct {
	// AddSlash when true appends a trailing slash (e.g. /users -> /users/).
	// When false (default), strips trailing slashes (e.g. /users/ -> /users).
	AddSlash bool

	// Redirect when true issues an HTTP redirect.
	// When false (default), internally rewrites the request path transparently.
	Redirect bool

	// Status is the HTTP status code used when Redirect is true.
	// Default: 301 (StatusMovedPermanently). Use 308 (StatusPermanentRedirect)
	// to preserve HTTP methods like POST/PUT across the redirect.
	Status int

	// Next defines an optional predicate to skip normalization for matching requests.
	Next func(c fh.Ctx) bool
}

// New creates a new trailing slash normalization middleware.
func New(cfg ...Config) fh.HandlerFunc {
	c := Config{
		AddSlash: false,
		Redirect: false,
		Status:   fh.StatusMovedPermanently,
	}
	if len(cfg) > 0 {
		c = cfg[0]
		if c.Status == 0 {
			c.Status = fh.StatusMovedPermanently
		}
	}

	return func(ctx fh.Ctx) error {
		if c.Next != nil && c.Next(ctx) {
			return ctx.Next()
		}

		path := ctx.Path()
		// Root path "/" is never normalized
		if path == "" || path == "/" {
			return ctx.Next()
		}

		var targetPath string
		needsNormalization := false

		if c.AddSlash {
			// Append slash if not already present
			if !strings.HasSuffix(path, "/") {
				targetPath = path + "/"
				needsNormalization = true
			}
		} else {
			// Remove trailing slashes if present
			if strings.HasSuffix(path, "/") {
				targetPath = strings.TrimRight(path, "/")
				if targetPath == "" {
					targetPath = "/"
				}
				needsNormalization = true
			}
		}

		if !needsNormalization {
			return ctx.Next()
		}

		// Preserve query parameters if present
		query := string(ctx.RequestHeader().QueryString)
		targetURL := targetPath
		if query != "" {
			targetURL = targetPath + "?" + query
		}

		if c.Redirect {
			return ctx.Redirect(targetURL, c.Status)
		}

		return ctx.Rewrite(targetURL)
	}
}

// RemoveSlash returns a trailing slash middleware that removes trailing slashes.
// If redirect is true, an HTTP redirect is issued; otherwise it rewrites internally.
func RemoveSlash(redirect ...bool) fh.HandlerFunc {
	r := false
	if len(redirect) > 0 {
		r = redirect[0]
	}
	return New(Config{
		AddSlash: false,
		Redirect: r,
		Status:   fh.StatusMovedPermanently,
	})
}

// AddSlash returns a trailing slash middleware that appends trailing slashes.
// If redirect is true, an HTTP redirect is issued; otherwise it rewrites internally.
func AddSlash(redirect ...bool) fh.HandlerFunc {
	r := false
	if len(redirect) > 0 {
		r = redirect[0]
	}
	return New(Config{
		AddSlash: true,
		Redirect: r,
		Status:   fh.StatusMovedPermanently,
	})
}
