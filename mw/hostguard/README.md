# Host Guard Middleware

## What it does

Rejects requests with unexpected Host headers. This mitigates host-header attacks and accidental traffic for wrong domains.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/hostguard"
)

func main() {
	app := fh.New()
	app.Use(hostguard.New(hostguard.Config{Allowed: []string{"api.example.com"}}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Low overhead. Prevents cache poisoning, bad redirects, and routing confusion caused by forged Host headers.

## Ordering guidance

Run very early, after trusted proxy normalization if Host is rewritten by infrastructure.

## Production considerations

Configure every legitimate domain, including internal health-check names if used. Be careful with wildcard hosts.

