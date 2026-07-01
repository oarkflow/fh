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
	app.Use(realip.New(realip.Config{}))

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

Only trust proxy headers from trusted proxy CIDRs. Otherwise attackers can spoof client IPs.

