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
	m := metrics.NewWithConfig(metrics.Config{MaxRoutes: 256})
	app.Use(m.Middleware())
	app.Get("/_fh/metrics", m.Handler())

	app.Get("/", func(c fh.Ctx) error { return c.Status(fh.StatusOK).SendString("ok") })
}
```

## Impact

Small per-request overhead for counters and latency measurement. Provides essential production visibility.

## Ordering guidance

Run near the outside of the chain after request ID/tracing. Register the metrics endpoint separately and protect if needed.

## Production considerations

Route counts use matched route templates (for example `/users/:id`) rather than
raw request paths. Unmatched requests use `<unmatched>`. The default cap is 256
distinct method-and-route series; `MaxRoutes` changes that cap. Additional
series are counted under `<overflow>`.

The Prometheus output includes `fh_route_requests_total` and
`fh_route_request_duration_seconds` by method, route template, and status
class. The route cap also applies to these series; overflow uses one shared
`<overflow>` route label. The existing JSON fields are unchanged.
