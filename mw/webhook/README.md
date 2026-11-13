# Webhook Verification Middleware

## What it does

Verifies webhook signatures using configured secrets, timestamp checks, and optional replay protection. The default response uses a generic 401 for invalid signatures and a generic 503 when secret lookup, verifier configuration, or replay storage is unavailable; internal error details are not returned to webhook callers.

Replay protection retains markers until their TTL expires and defaults to a
100,000-entry limit (`MaxReplayEntries`). New requests fail closed when the
limit is reached; existing replay markers are not evicted early. Configure a
shared `Replay` store and matching capacity policy when running multiple nodes.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/webhook"
)

func main() {
	app := fh.New()
	app.Use(webhook.New(webhook.Config{
		SecretFunc: func(c fh.Ctx) ([]byte, error) {
			return []byte("secret"), nil
		},
	}))

	app.Get("/", func(c fh.Ctx) error {
		return c.Status(fh.StatusOK).SendString("ok")
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

Replay protection is on by default: `Config.Replay` falls back to a process-local `kv.Store` if unset, so a captured (signature, timestamp) pair cannot be resent within `Tolerance`. For multi-instance deployments behind a load balancer, supply a shared `kv.Store` (e.g. Redis-backed) so replay detection works across instances.

For a single-instance deployment that needs replay markers to survive a restart, construct `kv.NewFileStore(dir, kv.WithFileGCInterval(gcInterval))` and pass it as `Config.Replay`. Storage failures reject rather than silently allow a replay.

If you provide `Config.Error`, it receives the original verification error for
custom handling. Avoid returning `err.Error()` to the webhook caller; it may
contain secret-source or replay-store details.
