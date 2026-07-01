# JWT Authentication Middleware

## What it does

Verifies signed JWTs, validates standard claims, stores claims in `Ctx.Locals`, and can set `fh.Principal`. Authorization remains in core helpers or `contrib/mw/authz`.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/jwt"
)

func main() {
	app := fh.New()
	app.Use(jwt.New(jwt.Config{Secret: []byte("dev-secret"), Algorithms: []string{"HS256"}}))

	app.Get("/me", func(c fh.Ctx) error {
		return c.JSON(c.Locals(jwt.ClaimsLocalKey))
	})
}
```

## Impact

Adds cryptographic verification per request. HMAC is fast; remote JWKS/key lookup should be cached.

## Ordering guidance

Run after request ID/tracing/real IP and before authorization, tenant extraction, and protected handlers.

## Production considerations

Use strong secrets or asymmetric signing in production. Validate issuer, audience, expiration, and not-before. Do not implement authorization here; use `fh.RequireRole`, `fh.RequireScope`, `fh.RequirePermission`, or `contrib/mw/authz`.

