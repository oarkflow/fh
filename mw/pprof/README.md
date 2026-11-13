# pprof Middleware

## What it does

Registers protected Go profiling endpoints for CPU, heap, goroutine, mutex, block, and trace diagnostics.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/pprof"
)

func main() {
	app := fh.New()
	pprof.Enable(app, pprof.Config{
		Prefix: "/_fh/debug/pprof",
		Auth: pprof.StaticToken("X-Admin-Token", "change-me"),
	})

	app.Get("/", func(c fh.Ctx) error { return c.Status(fh.StatusOK).SendString("ok") })
}
```

## Impact

No request-path overhead except for the registered debug endpoints. Profiling itself can add overhead while active.
Generated profile output is capped at 16 MiB by default. Set
`Config.MaxProfileBytes` to a different positive limit for the expected heap
and goroutine profile sizes; oversized profiles return HTTP 413. One profile
capture may run per `Enable` instance at a time; overlapping captures return
HTTP 429 to bound aggregate memory use.

## Ordering guidance

Register separately from public routes. Keep behind admin authentication and private networking.

## Production considerations

Never expose pprof publicly. Use temporary access during incidents and audit access.
