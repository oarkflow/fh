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

