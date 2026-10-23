# Favicon Middleware

## What it does

Serves the `/favicon.ico` request from in-memory bytes or a file on disk with long-lived caching (`Cache-Control`) and `ETag` / `304 Not Modified` validation. If no icon file or data is configured, it responds with `204 No Content` to silence browser 404 noise and keep access logs clean.

## How to implement

### Basic (Silence 404s with 204 No Content)

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/favicon"
)

func main() {
	app := fh.New()

	// Intercepts /favicon.ico and returns 204 No Content
	app.Use(favicon.New())

	app.Get("/", func(c fh.Ctx) error {
		return c.SendString("Hello")
	})

	app.Listen(":8080")
}
```

### Serve from File or Raw Bytes

```go
// From file
app.Use(favicon.New(favicon.Config{
	File: "./public/favicon.ico",
}))

// Or embedded in-memory bytes
app.Use(favicon.New(favicon.Config{
	Data: iconBytes,
}))
```

## Impact

Zero disk I/O per request (the icon is cached in memory once at startup) with automated ETag/conditional response generation.

## Ordering guidance

Mount early in the pipeline so favicon requests are terminated before session, authentication, or logging middleware.

## Production considerations

- Configures `public, max-age=31536000, immutable` by default for maximum browser caching efficiency.
- Supports `304 Not Modified` on `If-None-Match` requests.
