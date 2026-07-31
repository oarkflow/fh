# HMAC Signature Middleware

## What it does

Verifies signed requests using HMAC signatures, timestamps, and key resolution. It is useful for partner APIs and webhook-like endpoints.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/signature"
)

func main() {
	app := fh.New()
	app.Use(signature.New(signature.Config{SecretResolver: func(c fh.Ctx, keyID string) [][]byte { return [][]byte{[]byte("secret")} }}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds body hashing and HMAC verification. Protects integrity and authenticity.

## Ordering guidance

Run after body limit/request hash and before handlers. Pair with replay protection.

## Production considerations

Use timestamp skew limits, key IDs, key rotation, constant-time comparison, and distributed replay stores.

## Replay protection

Replay protection is on by default: `Config.Replay` falls back to a bounded in-memory store if unset, so a captured signature cannot be resent within `Tolerance`. This default store is process-local — for multi-instance deployments behind a load balancer, supply a shared `ReplayStore` (e.g. Redis-backed) so replay detection works across instances.

For a single-node deployment where replay markers should survive a process restart, use `signature.NewFileStore(dir, gcInterval)` as `Config.Replay`. It persists one file per hashed key under `dir` (atomic writes, optional background GC) and fails safe: any file I/O error is treated as a replay (`Seen` returns `true`) rather than silently allowing a request through.

