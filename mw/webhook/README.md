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

