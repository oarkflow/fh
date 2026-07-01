# Metrics Middleware

## What it does

Collects request counts, status counts, latency buckets, and exposes Prometheus-style metrics through a handler.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/metrics"
)

func main() {
	app := fh.New()
	m := metrics.New()
	app.Use(m.Middleware())
	app.Get("/_fh/metrics", m.Handler())

	app.Get("/", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Small per-request overhead for counters and latency measurement. Provides essential production visibility.

## Ordering guidance

Run near the outside of the chain after request ID/tracing. Register the metrics endpoint separately and protect if needed.

## Production considerations

Use stable route labels to avoid high-cardinality metrics. Avoid labeling by raw path, user, token, or request ID.

