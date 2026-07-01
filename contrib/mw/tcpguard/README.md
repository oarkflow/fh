# TCPGuard Middleware

## What it does

Integrates `github.com/oarkflow/tcpguard` request inspection and abuse/anomaly decisions with `fh` middleware.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/contrib/mw/tcpguard"
)

func main() {
	app := fh.New()
	// guard := guard.New(...)
	// app.Use(tcpguard.Middleware(guard))
	app.Get("/", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Adds security inspection overhead based on configured TCPGuard rules and feeds. It can block suspicious or abusive requests early.

## Ordering guidance

Run early after real IP/proxy normalization and before expensive handlers.

## Production considerations

Keep rules and threat feeds current. Use concise production responses but detailed internal logs. Test trusted proxy handling and false-positive rates.

