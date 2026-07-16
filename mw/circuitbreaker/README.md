# Production Circuit Breaker for fh

This package is a drop-in replacement for the original middleware while adding a complete concurrent state machine.

## Guarantees

- Lock-free admission while closed.
- Explicit `closed`, `open`, and `half-open` states.
- Generation tokens prevent stale requests from modifying newer state.
- Bounded half-open probes prevent recovery storms.
- A pending failed probe wins over an early successful probe.
- Consecutive-failure and optional rolling failure-rate policies.
- Manual open, close, and reset operations.
- Transition callbacks run outside locks; callback panics are contained.
- Panic outcomes are recorded and then re-panicked for the normal recovery middleware.
- Bounded keyed registry for independent dependency/route/tenant breakers.
- Snapshot counters for metrics and diagnostics.
- Injectable clock for deterministic tests.

## Basic use

```go
breaker := circuitbreaker.New(circuitbreaker.Config{
    FailureThreshold:    5,
    SuccessThreshold:    2,
    HalfOpenMaxRequests: 2,
    ResetAfter:          30 * time.Second,
})

app.Use(breaker.Handler())
```

Apply a breaker around the smallest meaningful failure domain. Do not put one global breaker in front of unrelated dependencies.

## Per-dependency registry

```go
registry, err := circuitbreaker.NewRegistry(circuitbreaker.RegistryConfig{
    Breaker: circuitbreaker.Config{
        FailureThreshold: 5,
        ResetAfter:       30 * time.Second,
    },
    MaxEntries: 500,
    IdleTTL:    15 * time.Minute,
})
if err != nil {
    panic(err)
}

app.Use(registry.Handler(func(c fh.Ctx) string {
    // Use a bounded value such as a configured upstream name, not arbitrary
    // attacker-controlled input.
    return "payments-api"
}))
```

Call `registry.Sweep()` periodically when `IdleTTL` is enabled.

## Rolling failure rate

```go
breaker := circuitbreaker.New(circuitbreaker.Config{
    FailureThreshold:     10,
    FailureRateThreshold: 0.50,
    MinimumRequests:      20,
    RollingWindow:        30 * time.Second,
    RollingBuckets:       10,
})
```

The breaker opens when either the consecutive threshold or the rolling-rate policy trips.

## Failure classification

By default:

- response status >= 500 is a failure;
- `*fh.HTTPError` status >= 500 is a failure;
- non-HTTP errors are failures;
- `context.Canceled` is ignored unless a 5xx status was produced.

Use `IsFailure` for dependency-specific behavior. Keep it fast and non-blocking.

## Rejection handling

```go
OnReject: func(c fh.Ctx, snapshot circuitbreaker.Snapshot) error {
    // Optionally set Retry-After using snapshot.RetryAfter with the header API
    // available in the fh version used by the application.
    return fh.NewHTTPError(
        fh.StatusServiceUnavailable,
        "PAYMENTS_UNAVAILABLE",
        "payments are temporarily unavailable",
    )
},
```

## Metrics

Export `breaker.Snapshot()` through the application's metrics integration. Useful fields include state, generation, accepted/rejected counts, failures, transitions, panics, rolling counts, and half-open probe counts.

## Middleware order

Place the breaker:

1. after request ID, panic recovery, and observability middleware;
2. before the expensive or failure-prone downstream operation;
3. outside retry middleware when each user request should count once, or inside it when every dependency attempt should count separately.

Choose one interpretation deliberately and test it.

## Verification

Run:

```bash
go test -race ./...
go test -run Test -count=100 ./...
go test -bench=. -benchmem ./...
```
