# circuitbreaker middleware

`circuitbreaker` protects downstream services or expensive handlers by opening after repeated failures.

## Import

```go
import "github.com/oarkflow/fh/mw/circuitbreaker"
```

## Usage

```go
app.Get("/payments/status",
    circuitbreaker.Middleware(circuitbreaker.Config{
        FailureThreshold: 5,
        SuccessThreshold: 2,
        ResetAfter: 30 * time.Second,
    }),
    paymentStatus,
)
```

## Custom failure detection

```go
breaker := circuitbreaker.New(circuitbreaker.Config{
    FailureThreshold: 3,
    IsFailure: func(c *fh.Ctx, err error) bool {
        return err != nil || c.StatusCode() == 502 || c.StatusCode() == 503
    },
    OnOpen: func(c *fh.Ctx) error {
        return c.Status(503).JSON(fh.Map{"error": "payments_unavailable"})
    },
})

app.Use("/payments", breaker.Handler())
```

## States

- `closed`: requests pass normally.
- `open`: requests fail immediately until `ResetAfter` passes.
- `half-open`: limited trial state; closes after enough successes.

## Best practice

Use one breaker per downstream dependency or route group. Do not share one breaker across unrelated services.
