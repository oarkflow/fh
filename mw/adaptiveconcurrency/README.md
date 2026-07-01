# Adaptive Concurrency Middleware

## What it does

Automatically adjusts allowed in-flight requests based on observed latency and errors. It protects the service from overload while allowing throughput to increase when the service is healthy.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/adaptiveconcurrency"
)

func main() {
	app := fh.New()
	app.Use(adaptiveconcurrency.New(adaptiveconcurrency.Config{MinLimit: 32, MaxLimit: 2048}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves stability during traffic spikes and downstream slowdowns. It may reject requests with `503` when the adaptive limit is reached, which is preferable to cascading latency or process failure.

## Ordering guidance

Place early, after request ID/tracing/real IP, and before expensive authentication, decoding, database, proxy, or business handlers.

## Production considerations

Tune minimum and maximum limits per deployment. Watch rejection rate and latency histograms. For multi-node deployments, combine with external load balancing and queue backpressure.

