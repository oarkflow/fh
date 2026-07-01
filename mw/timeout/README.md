# Timeout Middleware

## What it does

Applies a request deadline and returns a controlled timeout response when handlers exceed the configured duration.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/timeout"
)

func main() {
	app := fh.New()
	app.Use(timeout.New(5 * time.Second))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Protects workers from hanging requests and downstream stalls. Long-running handlers may be canceled.

## Ordering guidance

Run before handlers and dependency calls. Pair with context-aware downstream clients.

## Production considerations

Choose route-specific timeouts. Ensure handlers honor context cancellation and release resources.

