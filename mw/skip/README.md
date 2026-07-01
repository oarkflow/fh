# Skip Middleware

## What it does

Conditionally bypasses another middleware based on a predicate. It is useful for excluding health checks, static assets, or public routes.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/skip"
)

func main() {
	app := fh.New()
	app.Use(skip.New(logger.New(logger.Config{}), func(c fh.Ctx) bool { return c.Path() == "/health" }))

	app.Get("/health", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Adds a predicate check per request. Helps avoid unnecessary middleware work on excluded routes.

## Ordering guidance

Wrap only the middleware that should be skipped. Keep predicates cheap and deterministic.

## Production considerations

Avoid broad skips that bypass security accidentally. Add tests for protected and skipped paths.

