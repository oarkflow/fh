# Idempotency Middleware

## What it does

`idempotency.New` does **not** perform deduplication or storage itself. It is
a small adapter: it derives an idempotency key from the request (via a
caller-supplied function) and writes it to the `Idempotency-Key` request
header, so that fh's reliability layer — the actual dedup engine, enabled
with `fh.WithReliability` — picks it up.

Use this middleware when the key isn't simply the client-sent
`Idempotency-Key` header value as-is (for example, deriving it from a
business identifier, normalizing/validating a client-sent key, or
synthesizing one for clients that don't send the header at all). If your
clients already send a usable `Idempotency-Key` header, you don't need this
middleware — just enable `fh.WithReliability` and it reads that header
directly.

## How to implement

The dedup, request-hash-conflict detection, response replay, and storage
all live in `fh.WithReliability`. `idempotency.New` only needs to run
*before* that engine's middleware, which fh appends to every route
automatically once reliability is enabled — no separate `app.Use(rel.Middleware())`
call is needed.

```go
package main

import (
	"log"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/idempotency"
)

func main() {
	app := fh.New(fh.WithReliability(fh.ReliabilityConfig{
		Enabled:            true,
		IdempotencyEnabled: true,
		DataDir:            "./data/reliability", // file-backed store; see below for Redis/PostgreSQL
	}))

	// Trust the client-sent header if present, otherwise derive a key from a
	// caller identifier the client is expected to supply on every retry.
	app.Use(idempotency.New(func(c fh.Ctx) string {
		if v := c.Get("Idempotency-Key"); v != "" {
			return v
		}
		return c.Get("X-Client-Request-Id")
	}))

	app.Post("/payments", func(c fh.Ctx) error {
		// This handler's side effects (and its response) run at most once per
		// key: a retry with the same key and the same request body/method/path
		// gets the original response replayed without re-executing this
		// handler; a retry with the same key but a *different* body gets a 409
		// conflict instead of silently executing a different operation under
		// the same key.
		return c.JSON(fh.Map{"status": "accepted"})
	})

	log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```

With no `idempotency.New` at all, `fh.WithReliability`'s dedup still works —
it just reads whatever `Idempotency-Key` header the client already sent.

## Impact

Prevents duplicate side effects from client retries and network failures,
*provided* `fh.WithReliability`'s `IdempotencyEnabled` is on somewhere in the
app — this middleware alone has no effect on request handling.

## Ordering guidance

Mount `idempotency.New` before routes register (so it runs ahead of the
auto-appended reliability middleware) and after body-size limiting. It must
run before any handler that performs the side effect being deduplicated.

## Production considerations

- The default store (`DataDir`) is file-backed and single-host; supply
  `ReliabilityConfig.IdempotencyRepository` with a Redis/PostgreSQL-backed
  implementation for multi-node deployments — see
  [Shared State](../../docs/shared-state.md).
- Idempotency keys are scoped by caller identity internally (principal, or
  client IP when unauthenticated), so one caller can't replay another
  caller's cached response by guessing or observing their key.
- A reused key with a different request body/method/path is rejected with
  409, not silently executed as a different operation.
- Set `IdempotencyTTL` to bound how long replay records are retained.
