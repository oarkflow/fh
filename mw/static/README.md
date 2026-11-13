# Static Files Middleware

## What it does

Serves files from a directory with options for index files, cache headers, directory behavior, and asset delivery.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/static"
)

func main() {
	app := fh.New()
	app.Use(static.New("./public"))

	app.Get("/", func(c fh.Ctx) error {
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Efficient for local static assets. Large file serving consumes disk/network I/O.

## Ordering guidance

Mount on specific prefixes before fallback routes. Pair with compression, ETag, conditional requests, and cache headers.

## Production considerations

Prevent directory traversal. Disable directory listing unless intentional. Use CDN/object storage for very high traffic assets.

File responses are streamed with a fixed-size buffer instead of being loaded
fully into memory. When browsing is enabled, `MaxBrowseEntries` bounds the
listing (default 1000, maximum 10000); reads stop at the limit and the response
indicates when results were truncated.
