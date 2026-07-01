# Reliability Middleware

## What it does

Applies route-level reliability policies and typed reliable endpoint behavior for idempotency, journaling, queue handoff, and response replay patterns.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/reliability"
)

func main() {
	app := fh.New()
	app.Use(reliability.New(fh.ReliabilityPolicy{}))

	app.Post("/work", func(c fh.Ctx) error { return c.JSON(fh.Map{"queued": true}) })
}
```

## Impact

Adds durability and replay protection depending on configured policy and storage. The strongest guarantees require durable/distributed stores.

## Ordering guidance

Run around unsafe operations that need reliability guarantees. Pair with body limit, idempotency, request hash, and transaction/outbox logic.

## Production considerations

Use SQL/Redis/NATS/Kafka-style backends for production. Test crash recovery and duplicate request behavior.

