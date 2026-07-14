# Workflow Middleware

## What it does

Composes multiple middleware/handler steps into a single, ordered request workflow: sequential steps, conditional steps, branching, parallel fan-out, retry, per-step timeout, compensation on failure, observability hooks, and handoff to fh's durable job queue for async work.

## How to implement

```go
package main

import (
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/workflow"
)

func main() {
	app := fh.New()

	wf := workflow.New("checkout").
		UseWithOptions("charge-payment", chargePayment,
			workflow.WithRetry(2, 50*time.Millisecond),
			workflow.WithTimeout(3*time.Second)).
		Use("respond", func(c fh.Ctx) error {
			return c.SendString("ok")
		})

	app.Post("/orders", wf.Handler())
	app.Listen(":8080")
}

func chargePayment(c fh.Ctx) error {
	// call payment gateway
	return nil
}
```

### Sequential steps

```go
wf := workflow.New("demo").
	Use("step-a", handlerA).
	Use("step-b", handlerB)
```

`Use` accepts an optional condition function as its third argument to skip a step:

```go
wf.Use("admin-only", handler, func(c fh.Ctx) bool {
	return c.Locals("role") == "admin"
})
```

### Timeout and retry

`UseWithOptions` accepts functional options for per-step behavior:

```go
wf.UseWithOptions("call-upstream", handler,
	workflow.WithTimeout(2*time.Second),         // bounds the step via a deadline context
	workflow.WithRetry(3, 100*time.Millisecond), // up to 3 retries with fixed backoff
	workflow.WithCondition(cond),
)
```

Timeouts set `c.Context()` to a deadline context for the duration of the step and restore the parent context afterward — the handler is expected to observe `c.Context().Done()` for long-running work, the same cooperative model used by `mw/timeout`. Retries stop early once the context is done and do not retry after a successful attempt.

### Branching

Runs the first branch whose `Condition` passes:

```go
wf.Branch("shipping",
	workflow.New("express").Condition(isExpress).Use("schedule", scheduleExpress),
	workflow.New("standard").Use("schedule", scheduleStandard), // fallback (no condition)
)
```

### Parallel fan-out

```go
wf.Parallel("fan-out", reserveInventory, sendConfirmation)     // fail-fast: first error wins
wf.ParallelJoin("fan-out", reserveInventory, sendConfirmation) // waits for all, joins errors
```

Each branch runs in its own goroutine against the same `fh.Ctx`; panics inside a branch are recovered and converted to errors.

### Async job handoff

```go
wf.Job("schedule-shipment", "shipment.schedule")
```

Hands the step off to fh's durable queue via `fh.AtomicHandoff` instead of running inline. Requires `Reliability.QueueEnabled` in `fh.Config`. The assigned job ID is stored in `c.Locals("job_id")`.

### Compensation

`OnError` is invoked whenever a step fails (including recovered panics). Returning `nil` swallows the error and continues the workflow — useful for compensating transactions. Returning a non-nil error aborts the workflow with that error:

```go
wf.OnError(func(step string, err error) error {
	if step == "reserve-inventory" {
		releaseHold(step)
		return nil // compensate and continue
	}
	return err // abort
})
```

### Observability

```go
wf.OnStepStart(func(step string) { ... }).
	OnStepComplete(func(step string, err error, dur time.Duration) { ... }).
	OnComplete(func(err error, dur time.Duration) { ... })
```

## Impact

Enables orchestration inside request handling. Complexity and latency depend on workflow structure; parallel branches add goroutines per request.

## Ordering guidance

Use for request-scoped workflows with a handful of steps, not as a replacement for a durable background workflow engine. Long-running or failure-sensitive work should go through `Job` and the durable queue rather than running inline.

## Production considerations

- Keep workflows observable with `OnStepStart`/`OnStepComplete`/`OnComplete` and correlate against `request_id`.
- Bound every external call with `WithTimeout`; a slow step without a timeout blocks the whole request.
- Use `OnError` for compensation, not to silently mask bugs — log every compensated error.
- Test each branch and the parallel fan-out under both success and failure independently.
- See [`examples/workflow`](../../examples/workflow) for a runnable checkout example.
