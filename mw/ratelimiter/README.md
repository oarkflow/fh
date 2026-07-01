# Rate Limiter Middleware

## What it does

Limits request rate by IP, user, tenant, API key, or custom key to protect fairness and prevent abuse.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/ratelimiter"
)

func main() {
	app := fh.New()
	app.Use(ratelimiter.New(ratelimiter.Config{Limit: 100}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Controls abuse and overload. Store choice affects memory usage and multi-node correctness.

## Ordering guidance

Run after real IP and authentication if the key depends on client identity. Run before expensive handlers.

## Production considerations

Use distributed stores for multi-node deployments. Return clear `429` responses with retry headers. Track limit hits by route and key type.

