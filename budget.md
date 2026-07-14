# budget

Hierarchical request execution budgets for fh. Every request carries explicit limits on time, memory, upstream calls, and retries. Budgets propagate through middleware, handlers, and child operations.

## Why

A single request timeout is insufficient for complex workflows. If a request has a 2-second deadline but spends 1.9 seconds on a database call, there is only 100ms left for everything else — but no component knows this. Hierarchical budgets let each component know its own allocation and fail fast when the budget is exhausted.

## Features

- Deadline, CPU time, queue time, body size, response size, memory, upstream calls, retries, log bytes
- `Child()` method to carve sub-budgets from parents
- Context integration via `BudgetFromContext()`
- Automatic deadline propagation
- Memory reservation before large allocations
- Middleware for automatic budget attachment

## Usage

```go
app := fh.New()

// Attach budget to every request
app.Use(fh.BudgetMiddleware(fh.BudgetConfig{
    Deadline:         2 * time.Second,
    MaxCPUTime:       1 * time.Second,
    MaxBodyBytes:     1 << 20,
    MaxMemoryBytes:   64 << 20,
    MaxUpstreamCalls: 5,
    MaxRetries:       3,
}))

app.Get("/order/:id", func(c fh.Ctx) error {
    budget := fh.BudgetFromContext(c.Context())

    // Carve sub-budget for database (700ms, 30% memory)
    dbBudget := budget.Child(700*time.Millisecond, fh.BudgetSplit{
        MemoryFraction:   0.3,
        UpstreamFraction: 0.5,
        RetryFraction:    1.0,
        LogFraction:      1.0,
    })

    if dbBudget.Remaining() < 100*time.Millisecond {
        return c.Status(503).JSON(fh.Map{"error": "deadline_exceeded"})
    }

    if !dbBudget.CheckMemory(10 << 20) {
        return c.Status(507).JSON(fh.Map{"error": "memory_budget_exceeded"})
    }

    // ... database call ...

    return c.JSON(fh.Map{"remaining": budget.Remaining().String()})
})
```

## BudgetSplit

```go
type BudgetSplit struct {
    MemoryFraction   float64  // 0.0-1.0 of parent memory budget
    UpstreamFraction float64  // 0.0-1.0 of parent upstream call budget
    RetryFraction    float64  // 0.0-1.0 of parent retry budget
    LogFraction      float64  // 0.0-1.0 of parent log budget
}
```

## Example hierarchy

```
Request budget: 2 seconds
├── authentication: 100ms
├── database:       700ms (30% memory)
├── upstream API:   800ms (50% upstream calls)
└── serialization:  100ms
```
