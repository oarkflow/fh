# Workflow Middleware

## What it does

Composes multiple middleware/handler steps into conditional, branched, parallel, or job-oriented workflows.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/workflow"
)

func main() {
	app := fh.New()
	wf := workflow.New("demo").Use("step", func(c fh.Ctx) error { return c.Next() })
	app.Use(wf.Handler())

	app.Get("/", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Enables orchestration inside request handling. Complexity and latency depend on workflow structure.

## Ordering guidance

Use for clear request workflows, not as a replacement for durable background workflow engines.

## Production considerations

Keep workflows observable and test each branch. Use durable queues for long-running or failure-sensitive jobs.

