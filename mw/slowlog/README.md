# Slow Log Middleware

## What it does

Logs requests whose latency exceeds a configured threshold, helping identify expensive routes or downstream stalls.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/slowlog"
)

func main() {
	app := fh.New()
	app.Use(slowlog.New(slowlog.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Very small overhead for timing. Logging occurs only for slow requests.

## Ordering guidance

Run near the outside of the chain to measure total request latency.

## Production considerations

Set route-specific thresholds where needed. Include request ID, route, status, and dependency metadata without leaking secrets.

