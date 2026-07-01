# Logger Middleware

## What it does

Writes access logs with request/response metadata such as method, path, status, latency, IP, request ID, and errors.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/logger"
)

func main() {
	app := fh.New()
	app.Use(logger.New(logger.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds I/O overhead; structured asynchronous logging is preferred for high throughput.

## Ordering guidance

Run after request/correlation ID and real IP so logs include normalized identifiers. Usually wraps most middleware.

## Production considerations

Redact secrets, authorization headers, cookies, and sensitive query parameters. Use sampling for very high RPS routes.

