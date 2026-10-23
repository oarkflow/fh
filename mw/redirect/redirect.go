package redirect

import (
	"net"
	"strings"

	"github.com/oarkflow/fh"
)

// Config defines the configuration for the redirect middleware.
type Config struct {
	// HTTPSRedirect when true redirects HTTP requests to HTTPS.
	HTTPSRedirect bool

	// HTTPSPort is the port used for HTTPS redirects when non-standard (e.g. ":8443").
	// If empty, standard port 443 is assumed (omitted from redirect URL).
	HTTPSPort string

	// Status is the HTTP redirect status code.
	// Default: 301 (StatusMovedPermanently).
	// To preserve HTTP methods (e.g. POST/PUT) and body, use 308 (StatusPermanentRedirect) or 307.
	Status int

	// Rules maps exact request paths to their target redirect destinations.
	// Example: map[string]string{"/old-pricing": "/pricing", "/legacy": "/v2"}
	Rules map[string]string

	// HostRedirect maps source hostnames to destination hostnames.
	// Example: map[string]string{"www.example.com": "example.com"}
	HostRedirect map[string]string

	// Next defines an optional predicate to skip this middleware.
	Next func(c fh.Ctx) bool
}

// New creates a new redirect middleware with the given configuration.
func New(cfg ...Config) fh.HandlerFunc {
	c := Config{
		Status: fh.StatusMovedPermanently,
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

		currentHost := ctx.Hostname()
		if currentHost == "" {
			currentHost = string(ctx.RequestHeader().Host)
		}
		hostOnly := currentHost
		if h, _, err := net.SplitHostPort(currentHost); err == nil {
			hostOnly = h
		}

		// 1. Host-level redirection (e.g. www to apex or domain rename)
		if len(c.HostRedirect) > 0 {
			if targetHost, ok := c.HostRedirect[hostOnly]; ok && targetHost != "" && targetHost != hostOnly {
				proto := ctx.Protocol()
				if isHTTPS(ctx) {
					proto = "https"
				}
				uri := ctx.OriginalURL()
				if uri == "" {
					uri = ctx.Path()
				}
				target := proto + "://" + targetHost + uri
				return ctx.Redirect(target, c.Status)
			}
		}

		// 2. HTTPS enforcement redirection
		if c.HTTPSRedirect && !isHTTPS(ctx) {
			targetHost := hostOnly
			if c.HTTPSPort != "" {
				port := c.HTTPSPort
				if !strings.HasPrefix(port, ":") {
					port = ":" + port
				}
				targetHost = targetHost + port
			}
			uri := ctx.OriginalURL()
			if uri == "" {
				uri = ctx.Path()
			}
			target := "https://" + targetHost + uri
			return ctx.Redirect(target, c.Status)
		}

		// 3. Path-level exact rules
		if len(c.Rules) > 0 {
			path := ctx.Path()
			if dest, ok := c.Rules[path]; ok && dest != "" {
				return ctx.Redirect(dest, c.Status)
			}
		}

		return ctx.Next()
	}
}

// ToHTTPS creates a redirect middleware that enforces HTTPS.
func ToHTTPS(status ...int) fh.HandlerFunc {
	code := fh.StatusMovedPermanently
	if len(status) > 0 && status[0] > 0 {
		code = status[0]
	}
	return New(Config{
		HTTPSRedirect: true,
		Status:        code,
	})
}

// isHTTPS reports whether the request was served over HTTPS or passed through
// a trusted TLS-terminating proxy declaring X-Forwarded-Proto: https.
func isHTTPS(c fh.Ctx) bool {
	if c.Protocol() == "https" {
		return true
	}
	proto := c.Get("X-Forwarded-Proto")
	if strings.EqualFold(proto, "https") {
		return true
	}
	return false
}
