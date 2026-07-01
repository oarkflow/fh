# Lifecycle Middleware

## What it does

Provides hooks around request start, before handler, after handler, error handling, and request end. It centralizes cross-cutting lifecycle behavior.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/lifecycle"
)

func main() {
	app := fh.New()
	app.Use(lifecycle.New(lifecycle.Hooks{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Hook overhead depends on configured callbacks. Useful for instrumentation, cleanup, and compensation.

## Ordering guidance

Run near the outside of the middleware chain so it can observe most behavior.

## Production considerations

Hooks must be fast and safe. Do not panic in hooks. Avoid blocking external calls in lifecycle callbacks.

