# Redirect Middleware

## What it does

Handles HTTP redirects, including automatic HTTPS enforcement, canonical host/domain redirection (e.g. `www` to apex domain), and path mapping rules.

## How to implement

### Enforce HTTPS

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/redirect"
)

func main() {
	app := fh.New()

	// Redirect all HTTP traffic to HTTPS with 301 Moved Permanently
	app.Use(redirect.ToHTTPS())

	app.Get("/", func(c fh.Ctx) error {
		return c.SendString("Secure connection")
	})

	app.Listen(":8080")
}
```

### Canonical Host Redirection & Path Rules

```go
app.Use(redirect.New(redirect.Config{
	// Redirect www to root domain
	HostRedirect: map[string]string{
		"www.example.com": "example.com",
	},
	// Redirect specific legacy paths
	Rules: map[string]string{
		"/old-docs": "/docs",
	},
	Status: fh.StatusPermanentRedirect, // 308 preserves request method & payload
}))
```

## Impact

Minimal overhead per request (string comparisons on scheme, host, and path).

## Ordering guidance

Mount early in the pipeline (before authentication, sessions, and route handlers) so unencrypted or non-canonical requests are redirected immediately without wasting downstream processing resources.

## Production considerations

- Use status `301` for simple GET/HEAD redirects and SEO canonicalization.
- Use status `308` (Permanent Redirect) if redirecting API endpoints where clients send `POST`, `PUT`, or `DELETE` requests so client libraries preserve the HTTP method and request body across the redirect.
- Ensure your load balancer passes `X-Forwarded-Proto: https` if terminating TLS at an upstream edge proxy.
