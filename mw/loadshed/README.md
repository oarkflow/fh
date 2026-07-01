# Load Shedding Middleware

## What it does

Rejects new requests when the server is already above configured in-flight or latency/error thresholds.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/loadshed"
)

func main() {
	app := fh.New()
	app.Use(loadshed.New(loadshed.Config{MaxInFlight: 10000}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Protects tail latency and process health during overload. Some requests are intentionally rejected.

## Ordering guidance

Run early, before expensive middleware and handlers.

## Production considerations

Return `Retry-After` where useful. Monitor sheds by route and tenant. Combine with adaptive concurrency and backpressure.

