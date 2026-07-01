# Backpressure Middleware

## What it does

Rejects or slows request admission when queue or worker saturation indicates that the service cannot safely accept more work.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/backpressure"
)

func main() {
	app := fh.New()
	app.Use(backpressure.New(backpressure.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Prevents unbounded memory growth and protects downstream durable queues. It can return `503`/`429` instead of allowing the process to overload.

## Ordering guidance

Place before endpoints that enqueue background work. Combine with load shedding and retry-after responses.

## Production considerations

Set thresholds according to queue depth, lag, and worker capacity. Monitor rejection counts, queue lag, and DLQ growth.

