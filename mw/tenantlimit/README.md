# Tenant Limit Middleware

## What it does

Limits concurrent requests per tenant, preventing one tenant from consuming all server capacity.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/tenantlimit"
)

func main() {
	app := fh.New()
	app.Use(tenantlimit.New(tenantlimit.Config{MaxConcurrent: 64}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves fairness and blast-radius isolation. Excess tenant requests may receive `429`/`503`.

## Ordering guidance

Run after tenant extraction and before expensive handlers.

## Production considerations

Tune per-tenant limits by plan or SLA. Emit metrics for accepted/rejected counts by tenant category, not raw tenant IDs if high cardinality.

