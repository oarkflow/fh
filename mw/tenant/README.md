# Tenant Middleware

## What it does

Extracts tenant identity from headers, hostnames, path, JWT/principal, or a custom function and stores it in context.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/tenant"
)

func main() {
	app := fh.New()
	app.Use(tenant.New(tenant.Config{Header: "X-Tenant-ID"}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Enables tenant-aware authz, rate limits, logging, storage routing, and queue partitioning.

## Ordering guidance

Run after authentication if tenant comes from the principal; otherwise after trusted header/host normalization. Run before audit, rate limiting, and handlers.

## Production considerations

Do not trust public tenant headers unless set by a trusted gateway. Validate tenant membership against the authenticated subject.

