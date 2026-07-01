# Conditional Requests Middleware

## What it does

Handles `If-None-Match`, `If-Match`, `If-Modified-Since`, and related precondition headers using configured ETag or Last-Modified functions.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/conditional"
)

func main() {
	app := fh.New()
	app.Use(conditional.New(conditional.Config{ETag: func(c fh.Ctx) string { return `"v1"` }}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Reduces bandwidth and handler work when clients have fresh cached data. It can return `304` or precondition failures.

## Ordering guidance

Run before expensive handlers if tags can be computed cheaply. Pair with ETag/static/cache middleware.

## Production considerations

Use strong validators for write preconditions. Ensure ETags change whenever the representation changes.

