# Compression Middleware

## What it does

Compresses eligible responses when the client supports an accepted encoding. It reduces bandwidth for text, JSON, HTML, and other compressible payloads.
Responses carry `Vary: Accept-Encoding` so shared caches distinguish compressed
and uncompressed variants, including requests where compression is skipped.

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
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Saves network bandwidth at the cost of CPU. For small responses compression can hurt latency.

## Ordering guidance

Run late enough to see the response body, but avoid compressing already-compressed assets. Usually after cache decisions.

## Production considerations

Set minimum body size. Breach protection is on by default, including when passing a partial `Config`; it skips requests carrying `Authorization`, `Proxy-Authorization`, or `Cookie`, and responses carrying `Set-Cookie`, `Content-Security-Policy`, or `Authorization`. To deliberately compress those responses, set `DisableBreachProtection: true` and assess BREACH-style side-channel risk for the endpoint. Do not compress images, archives, or already-compressed data.
