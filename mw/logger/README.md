# Logger Middleware

## What it does

Writes access logs with request/response metadata such as method, path, status, latency, IP, request ID, and errors. Query parameters and key-value pairs in error text whose names match the framework's sensitive-field list (including token, password, cookie, and OAuth credential names) are redacted by default in both text and JSON formats. Add application-specific names with `RedactQueryKeys`; `DisableQueryRedaction` opts out of both filters.

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
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Adds I/O overhead; structured asynchronous logging is preferred for high throughput.

## Ordering guidance

Run after request/correlation ID and real IP so logs include normalized identifiers. Usually wraps most middleware.

## Production considerations

Sensitive query and error key-value filtering is on by default. It cannot identify arbitrary application secrets in paths, free-form error text, or custom log fields. Review those sources separately. `DisableQueryRedaction` also disables the default error-string filter. Use sampling for very high RPS routes.
