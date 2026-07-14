# SLO

Route-level Service Level Objective tracking for fh. Monitors availability, latency percentiles, error budgets, and burn rates per route.

## Why

Without per-route SLO tracking, teams discover availability problems from user complaints. SLO tracking lets you detect violations before they become outages. Burn rate alerts tell you when you are consuming your error budget too fast, giving time to remediate before the budget is exhausted.

## Features

- Route patterns: static paths, `:param` segments, terminal `*` wildcards, and `^regex` patterns
- Single `tracker.Handler()` middleware matches every request against all registered patterns
- Per-route availability targets (e.g., 99.99%)
- P50/P95/P99 latency targets and tracking
- Error budget remaining calculation
- Sliding-window burn rate over `BurnRateWindow`
- Alert and recovery callbacks
- Background periodic evaluation
- Thread-safe concurrent recording with a bounded path-match cache

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

// Register SLOs — any router-style pattern works
tracker.Register("/api/users", fh.SLO{             // static
    Availability:   0.999, // 99.9%
    P99Latency:     200 * time.Millisecond,
    P95Latency:     100 * time.Millisecond,
    BurnRateWindow: 5 * time.Minute,
})

tracker.Register("/api/orders/:id", fh.SLO{        // dynamic
    Availability:   0.9999, // 99.99%
    P99Latency:     500 * time.Millisecond,
    BurnRateWindow: 5 * time.Minute,
})

tracker.Register("/files/*", fh.SLO{               // wildcard
    Availability: 0.99,
    P99Latency:   time.Second,
})

tracker.Register(`^/api/v[0-9]+/reports$`, fh.SLO{ // regex
    Availability: 0.995,
    P99Latency:   800 * time.Millisecond,
})

// One middleware for all registered patterns
app.Use(tracker.Handler())

app.Get("/api/users", func(c fh.Ctx) error {
    return c.JSON(fh.Map{"users": []string{"alice"}})
})

// Query SLO state
app.Get("/admin/slo", func(c fh.Ctx) error {
    return c.JSON(tracker.Snapshot())
})
```

## Route Patterns

`Register` accepts the same pattern syntax as the router plus regex:

| Form | Example | Matches |
| ---- | ------- | ------- |
| Static | `/api/users` | exactly `/api/users` |
| Named param | `/api/users/:id` | `/api/users/42` |
| Multiple params | `/api/users/:id/posts/:postID` | `/api/users/42/posts/7` |
| Wildcard | `/files/*` or `/files/*filepath` | `/files/a/b/c.txt` |
| Regex | `^/api/v[0-9]+/reports$` | `/api/v1/reports`, `/api/v12/reports` |

Non-regex patterns use the router's own trie matcher (`CompileRoutePattern`), so an SLO pattern matches exactly the paths the equivalent route would serve. Patterns starting with `^` are compiled as regular expressions (`Register` panics on invalid regex).

Match precedence: exact static match first, then dynamic patterns (more static segments win, wildcards last), then regex patterns in registration order. Resolved paths are cached (bounded) so steady-state matching is a single map lookup.

`Unregister(pattern)` removes a route; `Match(path)` resolves a concrete path to its registered pattern for debugging.

## SLOSnapshot

```go
type SLOSnapshot struct {
    Route                string
    TotalRequests        int64
    FailedRequests       int64
    SuccessRequests      int64
    WindowRequests       int64    // requests inside BurnRateWindow
    WindowFailed         int64    // failures inside BurnRateWindow
    BurnRate             float64
    ErrorBudgetRemaining float64  // 0.0-1.0
    Compliant            bool
    P50, P95, P99        float64  // milliseconds
}
```

## Burn Rate

Burn rate = windowed error rate / allowed error rate, computed over the sliding `BurnRateWindow` (default 5 minutes) so old traffic neither masks a fresh violation nor prolongs a resolved one. A burn rate of 2.0 means you are consuming your error budget twice as fast as allowed. At that rate, a 30-day budget is exhausted in 15 days.

Compliance requires all of: overall availability meets the target, every configured latency percentile (P50/P95/P99) meets its target, and burn rate below `AlertThreshold`.
