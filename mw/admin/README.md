# Admin Ops Middleware

## What it does

Registers protected operational endpoints for runtime information, route inspection, queue statistics, queue job listing, and failed-job retry/discard operations.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/admin"
)

func main() {
	app := fh.New()
	admin.Enable(app, admin.Config{
		Prefix: "/_fh/admin",
		Auth: admin.StaticToken("X-Admin-Token", "change-me"),
	})

	app.Get("/", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Adds an operations plane that makes production debugging and queue recovery easier. The impact is negligible unless admin routes are called.

## Ordering guidance

Register after the app is created and after queue/runtime features are configured. Do not expose before security controls are decided.

## Production considerations

Always protect with mTLS, VPN/private network, IP allowlist, or strong token auth. Rotate admin tokens and audit every admin call. Never expose debug or queue mutation endpoints publicly.

