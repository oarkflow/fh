# Recover Middleware

## What it does

Catches panics, logs stack traces when configured, and returns a controlled error response instead of crashing the process.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/recover"
)

func main() {
	app := fh.New()
	app.Use(recover.New(recover.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves service availability. It does not fix the underlying bug; it prevents a panic from killing the server.

## Ordering guidance

Run near the beginning of the chain so it wraps most middleware and handlers.

## Production considerations

Log enough detail for debugging but do not leak stack traces to clients in production. Alert on recovered panics.

