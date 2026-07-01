# Body Limit Middleware

## What it does

Rejects requests whose body exceeds the configured maximum size. It protects memory, CPU, and storage from oversized payloads.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
)

func main() {
	app := fh.New()
	app.Use(bodylimit.New(10 << 20))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Reduces DoS risk and prevents accidental large uploads from exhausting resources. Minimal overhead.

## Ordering guidance

Run before body parsing, typed handlers, compression, validation, or handlers that read the body.

## Production considerations

Set different limits by route where needed. File-upload routes should use streaming and explicit limits. Return clear errors without echoing payload content.

