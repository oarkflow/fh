# Webhook Verification Middleware

## What it does

Verifies webhook signatures using configured secrets, timestamp checks, and optional replay protection.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/webhook"
)

func main() {
	app := fh.New()
	app.Use(webhook.New(webhook.Config{Secret: func(c fh.Ctx) ([]byte, error) { return []byte("secret"), nil }}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Protects inbound webhooks from spoofing and replay. Adds HMAC/hash cost per request.

## Ordering guidance

Run after body limit and before webhook handlers. Pair with replay store and idempotency.

## Production considerations

Use provider-specific signature formats where needed. Rotate secrets safely and record failed verification attempts.

## Replay protection

Replay protection is on by default: `Config.Replay` falls back to a bounded in-memory store if unset, so a captured (signature, timestamp) pair cannot be resent within `Tolerance`. This default store is process-local — for multi-instance deployments behind a load balancer, supply a shared `ReplayStore` (e.g. Redis-backed) so replay detection works across instances.

