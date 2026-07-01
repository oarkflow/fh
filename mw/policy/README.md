# Policy Middleware

## What it does

Evaluates configurable policy rules to allow, reject, or otherwise control requests based on request context.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/policy"
)

func main() {
	app := fh.New()
	app.Use(policy.New(policy.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Centralizes request admission decisions. Complexity depends on policy rules.

## Ordering guidance

Run after identity, tenant, and request metadata extraction. Run before handlers.

## Production considerations

Version policies, test with fixtures, log decisions, and keep fail-open/fail-closed behavior explicit.

