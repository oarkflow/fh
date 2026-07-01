# Bulkhead Middleware

## What it does

Limits concurrent execution globally or by key, isolating expensive routes, tenants, or dependencies so one hot path cannot consume the whole server.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bulkhead"
)

func main() {
	app := fh.New()
	app.Use(bulkhead.New(bulkhead.Config{MaxConcurrent: 128}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves fault isolation. Excess requests are rejected instead of increasing latency for every request.

## Ordering guidance

Place around expensive route groups or before handlers that call constrained dependencies. Use route-level bulkheads for high-cost operations.

## Production considerations

Tune limits per dependency capacity. Expose metrics for in-flight, accepted, and rejected requests. Combine with timeouts and circuit breakers.

