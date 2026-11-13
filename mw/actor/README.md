# Actor Middleware

## What it does

Serializes downstream work by actor key across this process. Requests with the
same key wait for one another, while different keys run concurrently.

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
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Adds a small amount of per-request work to resolve the actor. The value can improve observability and authorization traceability across the stack.

## Ordering guidance

Run after authentication or header normalization middleware if the actor is derived from a principal or trusted header. Run before audit/policy/logging middleware that consumes the actor.

## Production considerations

Do not trust public actor headers unless they are set by a trusted gateway. Prefer deriving the actor from `fh.Principal`, JWT claims, mTLS identity, or a verified API key.

Keys are limited to 256 bytes and the process-wide lock registry tracks at
most 65,536 simultaneously active or waiting keys. Oversized keys receive
`400`; new keys receive `503` when the registry is full. Idle lock entries are
removed as soon as their final waiter exits.
