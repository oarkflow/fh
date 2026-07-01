# Idempotency Middleware

## What it does

Applies idempotency-key handling so unsafe operations can be retried without duplicate side effects.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/idempotency"
)

func main() {
	app := fh.New()
	app.Use(idempotency.New(func(c fh.Ctx) string { return c.Get("Idempotency-Key") }))

	app.Post("/payments", func(c fh.Ctx) error { return c.JSON(fh.Map{"status":"accepted"}) })
}
```

## Impact

Prevents duplicate writes from client retries and network failures. Storage choice determines durability and cluster safety.

## Ordering guidance

Run before handlers that perform side effects. Run after body limit and before transactional enqueue/outbox logic.

## Production considerations

Use distributed storage in multi-node deployments. Detect body-hash conflicts for reused keys. Set TTL and replay response policies.

