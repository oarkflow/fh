# Compression Middleware

## What it does

Compresses eligible responses when the client supports an accepted encoding. It reduces bandwidth for text, JSON, HTML, and other compressible payloads.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/compress"
)

func main() {
	app := fh.New()
	app.Use(compress.New(compress.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Saves network bandwidth at the cost of CPU. For small responses compression can hurt latency.

## Ordering guidance

Run late enough to see the response body, but avoid compressing already-compressed assets. Usually after cache decisions.

## Production considerations

Set minimum body size. Disable or be careful for sensitive reflected content if BREACH-style risks apply. Do not compress images, archives, or already-compressed data.

