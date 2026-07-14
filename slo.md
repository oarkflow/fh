# SLO

Route-level Service Level Objective tracking for fh. Monitors availability, latency percentiles, error budgets, and burn rates per route.

## Why

Without per-route SLO tracking, teams discover availability problems from user complaints. SLO tracking lets you detect violations before they become outages. Burn rate alerts tell you when you are consuming your error budget too fast, giving time to remediate before the budget is exhausted.

## Features

- Per-route availability targets (e.g., 99.99%)
- P50/P95/P99 latency tracking
- Error budget remaining calculation
- Burn rate calculation over configurable windows
- Alert and recovery callbacks
- Background periodic evaluation
- Thread-safe concurrent recording

## Usage

```go
app := fh.New()

tracker := fh.NewSLOTracker(fh.SLOTrackerConfig{
    CheckInterval:   5 * time.Second,
    AlertThreshold:  2.0,  // alert when burn rate >= 2x
    OnAlert: func(route string, state fh.SLOSnapshot) {
        log.Printf("SLO violation on %s: burn_rate=%.2f", route, state.BurnRate)
    },
    OnRecovery: func(route string, state fh.SLOSnapshot) {
        log.Printf("SLO recovered on %s", route)
    },
})
defer tracker.Stop()

// Register SLOs
tracker.Register("/api/users", fh.SLO{
    Availability: 0.999,     // 99.9%
    P99Latency:   200 * time.Millisecond,
    BurnRateWindow: 5 * time.Minute,
})

// Apply middleware
app.Use(fh.SLOMiddleware(tracker, "/api/users"))

app.Get("/api/users", func(c fh.Ctx) error {
    return c.JSON(fh.Map{"users": []string{"alice"}})
})

// Query SLO state
app.Get("/admin/slo", func(c fh.Ctx) error {
    return c.JSON(tracker.Snapshot())
})
```

## SLOSnapshot

```go
type SLOSnapshot struct {
    TotalRequests        int64
    FailedRequests       int64
    SuccessRequests      int64
    BurnRate             float64
    ErrorBudgetRemaining float64  // 0.0-1.0
    Compliant            bool
    P50, P95, P99        float64  // milliseconds
}
```

## Burn Rate

Burn rate = actual error rate / allowed error rate. A burn rate of 2.0 means you are consuming your error budget twice as fast as allowed. At that rate, a 30-day budget is exhausted in 15 days.
