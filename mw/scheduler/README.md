# scheduler

Priority-based request scheduler for fh. Routes requests through weighted admission control with per-priority concurrency limits.

## Why

Not all requests are equal. A health check should not fail because background batch jobs consumed all workers. An interactive API call should not queue behind a webhook delivery. Without priority scheduling, a burst of low-priority traffic degrades the experience for high-priority users.

## Features

- 5 priority levels: Critical, High, Normal, Low, Lowest
- Per-priority concurrency limits
- Global admission control
- Automatic shedding of low-priority traffic during overload
- Custom priority extraction from request headers/claims
- Retry-After headers on shed responses

## Usage

```go
import "github.com/oarkflow/fh/mw/scheduler"

app := fh.New()

sched := scheduler.New(scheduler.Config{
    MaxConcurrent: 500,
    QueueTimeout:  3 * time.Second,
    PerPriority: map[scheduler.Priority]int{
        scheduler.PriorityCritical: 50,
        scheduler.PriorityHigh:     100,
        scheduler.PriorityNormal:   200,
        scheduler.PriorityLow:      50,
        scheduler.PriorityLowest:   20,
    },
    PriorityFunc: func(c fh.Ctx) scheduler.Priority {
        if c.Get("X-Priority") == "high" {
            return scheduler.PriorityHigh
        }
        return scheduler.PriorityNormal
    },
})

app.Use(sched.Handler())

app.Get("/api/data", func(c fh.Ctx) error {
    return c.JSON(fh.Map{"data": "result"})
})
```

## Priorities

| Level | Value | Use Case |
|-------|-------|----------|
| Critical | 0 | Health checks, admin endpoints |
| High | 1 | Interactive user requests |
| Normal | 2 | Standard API requests |
| Low | 3 | Background tasks, batch jobs |
| Lowest | 4 | Best-effort, can be shed |

## Stats

```go
stats := sched.Stats()
// stats.TotalInFlight - total active requests
// stats.ByPriority    - per-priority active counts
// stats.Rejected      - total shed requests
```
