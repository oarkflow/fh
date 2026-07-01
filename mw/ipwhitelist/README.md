# IP Whitelist Middleware

## What it does

Allows only configured client IPs or CIDR ranges. It is useful for admin routes, partner callbacks, and internal endpoints.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/ipwhitelist"
)

func main() {
	app := fh.New()
	app.Use(ipwhitelist.New("10.0.0.0/8", "127.0.0.1"))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Low overhead. Blocks unauthorized networks before expensive processing.

## Ordering guidance

Run early, after real IP/proxy header middleware if behind a trusted proxy.

## Production considerations

Only trust forwarded headers from known proxies. Keep allowlists updated and include monitoring for denied requests.

