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

