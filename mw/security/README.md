# Security Headers Middleware

## What it does

Adds common defensive HTTP headers such as content type protection, frame options, referrer policy, HSTS, and CSP-related headers depending on config.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/security"
)

func main() {
	app := fh.New()
	app.Use(security.New(security.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves browser security with very low overhead.

## Ordering guidance

Run late enough to apply headers to normal responses, but before response is sent.

## Production considerations

Tune CSP carefully; start with report-only where needed. Enable HSTS only when HTTPS is permanent for the domain.

