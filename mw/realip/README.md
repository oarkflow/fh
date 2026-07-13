# Real IP Middleware

## What it does

Normalizes the client IP from trusted proxy headers such as `X-Forwarded-For` or `X-Real-IP`.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/realip"
)

func main() {
	app := fh.New()
	_, edge, _ := net.ParseCIDR("10.20.0.0/16")
	app.Use(realip.New(realip.Config{TrustedProxies: []*net.IPNet{edge}}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Low overhead. Enables accurate logs, rate limits, allowlists, and audit trails behind proxies.

## Ordering guidance

Run very early before logger, rate limiter, IP whitelist, audit, and authz decisions that use IP.

## Production considerations

Forwarding headers are ignored unless the socket peer belongs to a configured
`TrustedProxies` CIDR. The middleware walks multi-proxy chains from right to
left and stops at the first untrusted hop. `TrustAll` is available only for
isolated test/listener setups where untrusted clients cannot connect directly.
