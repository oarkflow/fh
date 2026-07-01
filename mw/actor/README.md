# Actor Middleware

## What it does

Derives an actor key for the request and stores it in request context. It is useful for audit trails, policy decisions, rate limiting, and tenant/user-aware logging.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/actor"
)

func main() {
	app := fh.New()
	app.Use(actor.New(actor.Config{Key: func(c fh.Ctx) string { return c.Get("X-Actor-ID") }}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds a small amount of per-request work to resolve the actor. The value can improve observability and authorization traceability across the stack.

## Ordering guidance

Run after authentication or header normalization middleware if the actor is derived from a principal or trusted header. Run before audit/policy/logging middleware that consumes the actor.

## Production considerations

Do not trust public actor headers unless they are set by a trusted gateway. Prefer deriving the actor from `fh.Principal`, JWT claims, mTLS identity, or a verified API key.

