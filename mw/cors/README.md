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
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Enables browser integrations while controlling which origins, methods, and headers are allowed.

## Ordering guidance

Run before authentication for preflight handling, but ensure actual protected requests still pass auth.

## Production considerations

`New()` retains the non-credentialed wildcard origin default for compatibility.
For deployed applications, configure explicit origins, methods, and headers;
avoid `*` with credentials. `DefaultConfig` is a deprecated snapshot and
mutating it does not change defaults used by `New()`. Log unexpected origins
during rollout.
Private Network Access preflights are denied unless `AllowPrivateNetwork` is
enabled. Preflight responses vary on origin, requested method, requested
headers, and the private-network request header so shared caches keep distinct
policy variants.
Origins containing paths, queries, fragments, userinfo, or invalid ports are
rejected before custom origin stores or callbacks are consulted. Requests with
multiple `Origin` fields are also treated as ambiguous and receive no CORS
headers.

## File-backed origin store

By default the allow-list comes from `Config.AllowOrigins` (in-memory). To manage it from a file instead, set `Config.OriginStore` to `cors.NewFileOriginStore(path, reloadInterval)`, where `path` points to a JSON array of origin patterns (same syntax as `AllowOrigins`: exact origins, `"*"`, or `"https://*.example.com"` subdomain wildcards). Pass a non-zero `reloadInterval` to poll the file for changes; a malformed edit is ignored and the last known-good list keeps serving.
The file is limited to 1 MiB, 10,000 patterns, and 4 KiB per pattern; oversized
reloads are rejected and leave the last known-good list active.
