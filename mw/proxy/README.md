# Proxy / Gateway Middleware

## What it does

Forwards requests to upstream services or implements simple gateway routing by path/prefix.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/proxy"
)

func main() {
	app := fh.New()
	app.Use(proxy.New(proxy.Config{Target: "http://localhost:8081"}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds network I/O and upstream dependency risk. Enables gateway composition and service migration.

## Ordering guidance

Run after security middleware when proxying protected routes. Run after real IP/correlation/tracing so headers can be forwarded.

## Production considerations

Set timeouts, max body size, header allow/deny lists, upstream health checks, circuit breakers, and retry budgets. Avoid blindly forwarding sensitive internal headers.

## SSRF protection

By default this middleware refuses to dial well-known cloud metadata endpoints (169.254.169.254, 169.254.170.2, fd00:ec2::254) even if `Target` or a custom `Director` ever resolves there — this closes the metadata-credential-theft class of SSRF. The check happens at dial time against the resolved IP (not just the configured hostname), so DNS rebinding cannot bypass it. Set `DisableSSRFGuard: true` only if this proxy intentionally targets a metadata endpoint. Use `DeniedCIDRs` to additionally block private ranges (e.g. `10.0.0.0/8`) if `Target`/`Director` could ever be influenced by request data.

