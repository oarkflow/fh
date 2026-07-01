# Tracing Middleware

## What it does

Creates or propagates trace identifiers and stores span metadata in context so requests can be followed across services.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/tracing"
)

func main() {
	app := fh.New()
	app.Use(tracing.New(tracing.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Small overhead for ID parsing/generation and timestamps. Enables distributed debugging.

## Ordering guidance

Run at the start of the chain after request ID/correlation ID or before logger/metrics depending on desired fields.

## Production considerations

Propagate W3C trace context to downstream services. Avoid high-cardinality attributes and redact sensitive data.

