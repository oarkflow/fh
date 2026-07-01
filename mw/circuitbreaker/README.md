# Circuit Breaker Middleware

## What it does

Stops calling unhealthy handlers or downstream paths after failures cross a threshold, then probes for recovery after a cooldown.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/circuitbreaker"
)

func main() {
	app := fh.New()
	app.Use(circuitbreaker.Middleware(circuitbreaker.Config{FailureThreshold: 5}))

	app.Get("/", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Prevents repeated expensive failures and gives dependencies time to recover. It may temporarily reject requests even after the underlying issue improves until half-open recovery succeeds.

## Ordering guidance

Place around handlers or route groups that depend on external systems. Usually after authentication and before proxy/DB/service calls.

## Production considerations

Track breaker state with metrics and logs. Use fallback responses for non-critical features. Avoid one global breaker for unrelated dependencies.

