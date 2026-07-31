# CORS Middleware

## What it does

Applies Cross-Origin Resource Sharing headers and handles preflight requests for browser clients.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/cors"
)

func main() {
	app := fh.New()
	app.Use(cors.New(cors.Config{AllowOrigins: []string{"https://example.com"}}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Enables browser integrations while controlling which origins, methods, and headers are allowed.

## Ordering guidance

Run before authentication for preflight handling, but ensure actual protected requests still pass auth.

## Production considerations

Avoid `*` with credentials. Prefer explicit origins, methods, and headers. Log unexpected origins during rollout.

## File-backed origin store

By default the allow-list comes from `Config.AllowOrigins` (in-memory). To manage it from a file instead, set `Config.OriginStore` to `cors.NewFileOriginStore(path, reloadInterval)`, where `path` points to a JSON array of origin patterns (same syntax as `AllowOrigins`: exact origins, `"*"`, or `"https://*.example.com"` subdomain wildcards). Pass a non-zero `reloadInterval` to poll the file for changes; a malformed edit is ignored and the last known-good list keeps serving.

