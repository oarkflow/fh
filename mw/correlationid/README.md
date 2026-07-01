# Correlation ID Middleware

## What it does

Propagates or creates a correlation ID so a single business transaction can be followed across services.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/correlationid"
)

func main() {
	app := fh.New()
	app.Use(correlationid.New(correlationid.Config{Header: "X-Correlation-ID"}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Very small overhead. Greatly improves debugging across distributed systems.

## Ordering guidance

Run at the beginning of the chain before logging, tracing, audit, proxy, and handlers.

## Production considerations

Validate externally provided IDs to avoid log injection. Forward the correlation ID to downstream services.

