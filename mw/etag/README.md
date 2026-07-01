# ETag Middleware

## What it does

Adds or validates ETag headers for responses so clients and caches can avoid downloading unchanged content.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/etag"
)

func main() {
	app := fh.New()
	app.Use(etag.New(etag.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Reduces bandwidth and improves cache efficiency. Hashing response bodies can add CPU for large payloads.

## Ordering guidance

Run with cache/conditional/static middleware. Avoid wrapping streaming responses unless supported by configuration.

## Production considerations

Use stable representation hashes. Avoid ETags that reveal sensitive versioning information for private resources.

