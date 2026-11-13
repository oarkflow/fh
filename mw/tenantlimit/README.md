# Tenant Limit Middleware

## What it does

Limits concurrent requests per tenant, preventing one tenant from consuming all server capacity.
Tenant identifiers are limited to 256 bytes by default before being stored in
the in-flight map; set `Config.MaxTenantIDBytes` to a different positive value
for applications with longer trusted identifiers. Overlong identifiers are
rejected with `400`. The map also tracks at most 10,000 distinct active tenants
by default (`Config.MaxTenants`); a new tenant is rejected with `503` when that
capacity is reached, while requests for tracked tenants can finish normally.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/tenantlimit"
)

func main() {
	app := fh.New()
	app.Use(tenantlimit.New(tenantlimit.Config{Limit: 64}))

	app.Get("/", func(c fh.Ctx) error {
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Improves fairness and blast-radius isolation. Excess tenant requests may receive `429`/`503`.

## Ordering guidance

Run after tenant extraction and before expensive handlers.

## Production considerations

Tune per-tenant limits by plan or SLA. Emit metrics for accepted/rejected counts by tenant category, not raw tenant IDs if high cardinality.
