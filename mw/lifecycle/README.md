# lifecycle middleware

`lifecycle` runs hooks around request processing. It is useful for metrics, auditing, debugging, request state tracking, and cleanup.

## Import

```go
import "github.com/oarkflow/fh/mw/lifecycle"
```

## Usage

```go
app.Use(lifecycle.New(lifecycle.Hooks{
    OnRequestStart: func(c *fh.Ctx) {
        c.Locals("started_at", time.Now())
    },
    OnBeforeHandler: func(c *fh.Ctx) {
        log.Printf("start %s %s", c.Method(), c.Path())
    },
    OnError: func(c *fh.Ctx, err error) {
        log.Printf("request failed: %v", err)
    },
    OnAfterHandler: func(c *fh.Ctx) {
        log.Printf("status=%d", c.StatusCode())
    },
    OnRequestEnd: func(c *fh.Ctx) {
        log.Printf("end %s %s", c.Method(), c.Path())
    },
}))
```

## Hook order

1. `OnRequestStart`
2. `OnBeforeHandler`
3. downstream middleware/handler
4. `OnError` when the downstream returned an error
5. `OnAfterHandler`
6. `OnRequestEnd`

## Best practice

Keep lifecycle hooks lightweight and non-blocking. Use them to enqueue audit events or update metrics, not to perform slow external calls inline.
