# Request ID Middleware

## What it does

Creates or validates a per-request identifier and exposes it through context/headers for logs, traces, responses, and downstream calls.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/requestid"
)

func main() {
	app := fh.New()
	app.Use(requestid.New(requestid.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Very low overhead. Essential for production debugging.

## Ordering guidance

Run first or near first before logging, tracing, audit, metrics, and handlers.

## Production considerations

Validate incoming IDs to prevent log injection. Use high-entropy IDs when generating new IDs.

