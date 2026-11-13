# Retry Budget Middleware

## What it does

Limits retry traffic per key so retries cannot amplify outages or overload dependencies.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/retrybudget"
)

func main() {
	app := fh.New()
	app.Use(retrybudget.New(retrybudget.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Reduces retry storms. Some retry attempts may be rejected when the budget is exhausted.

## Ordering guidance

Run before handlers or proxy routes that perform retries. Pair with circuit breakers and backpressure.

## Production considerations

Budget by tenant, route, upstream, or API key. Monitor exhausted budgets and tune refill rates.
Custom keys are limited to 256 bytes by default (`MaxKeyBytes`); oversized keys
are rejected with `400` before they enter the tracking map. The number of
tracked keys is also bounded by `MaxKeys`.
