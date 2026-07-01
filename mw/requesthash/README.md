# Request Hash Middleware

## What it does

Computes a request body hash and stores/exposes it for idempotency, audit, signature validation, and duplicate detection.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/requesthash"
)

func main() {
	app := fh.New()
	app.Use(requesthash.New(requesthash.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Hashing reads the body and adds CPU proportional to payload size. It can improve dedupe and forensic traceability.

## Ordering guidance

Run after body limit and before idempotency/reliability/handlers that need the hash.

## Production considerations

Avoid hashing very large streaming bodies unless explicitly supported. Use bounded memory and restore the body for downstream handlers.

