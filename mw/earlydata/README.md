# Early Data Middleware

## What it does

Detects and controls TLS 0-RTT early data requests to prevent replay-sensitive operations from being executed unsafely.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/earlydata"
)

func main() {
	app := fh.New()
	app.Use(earlydata.New(earlydata.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Protects unsafe operations from replay risks. May reject requests marked as early data.

## Ordering guidance

Run before handlers for unsafe methods and before idempotency-sensitive operations.

## Production considerations

Only allow early data for safe, idempotent operations. Document behavior behind TLS terminators/CDNs.

