package favicon

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"

	"github.com/oarkflow/fh"
)

const (
	defaultURL          = "/favicon.ico"
	defaultCacheControl = "public, max-age=31536000, immutable"
	contentTypeICO      = "image/x-icon"
)

// Config defines options for serving the favicon.
type Config struct {
	// File specifies the filesystem path to a favicon file (e.g. "./public/favicon.ico").
	// When set, the file is read into memory once on initialization.
	File string

	// Data specifies raw favicon icon bytes directly in memory.
	Data []byte

	// URL specifies the URL path to intercept. Default: "/favicon.ico".
	URL string

	// CacheControl sets the Cache-Control header. Default: "public, max-age=31536000, immutable".
	CacheControl string

	// Next defines an optional predicate to skip this middleware.
	Next func(c fh.Ctx) bool
}

// New creates a new favicon middleware handler.
func New(cfg ...Config) fh.HandlerFunc {
	c := Config{
		URL:          defaultURL,
		CacheControl: defaultCacheControl,
	}
	if len(cfg) > 0 {
		c = cfg[0]
		if c.URL == "" {
			c.URL = defaultURL
		}
		if c.CacheControl == "" {
			c.CacheControl = defaultCacheControl
		}
	}

	var icon []byte
	if c.File != "" {
		if data, err := os.ReadFile(c.File); err == nil {
			icon = data
		}
	} else if len(c.Data) > 0 {
		icon = make([]byte, len(c.Data))
		copy(icon, c.Data)
	}

	var etag string
	if len(icon) > 0 {
		sum := sha256.Sum256(icon)
		etag = `"` + hex.EncodeToString(sum[:]) + `"`
	}

	return func(ctx fh.Ctx) error {
		if c.Next != nil && c.Next(ctx) {
			return ctx.Next()
		}

		if ctx.Path() != c.URL {
			return ctx.Next()
		}

		method := ctx.Method()
		if method != fh.MethodGET && method != fh.MethodHEAD {
			ctx.Set("Allow", "GET, HEAD")
			return ctx.Status(fh.StatusMethodNotAllowed).SendString("Method Not Allowed")
		}

		ctx.Set("Cache-Control", c.CacheControl)
		ctx.Set("Content-Type", contentTypeICO)

		// If no icon data is configured, respond with 204 No Content to silence browser 404 noise.
		if len(icon) == 0 {
			return ctx.Status(fh.StatusNoContent).SendBytes(nil)
		}

		ctx.Set("ETag", etag)

		if ifNoneMatch := ctx.Get("If-None-Match"); ifNoneMatch != "" {
			if strings.Contains(ifNoneMatch, etag) || ifNoneMatch == "*" {
				return ctx.Status(fh.StatusNotModified).SendBytes(nil)
			}
		}

		if method == fh.MethodHEAD {
			return ctx.Status(fh.StatusOK).SendBytes(nil)
		}

		return ctx.Status(fh.StatusOK).SendBytes(icon)
	}
}
