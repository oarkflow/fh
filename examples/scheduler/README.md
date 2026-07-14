# Priority Scheduler Example

Demonstrates priority-based request scheduling with per-priority concurrency limits.

## What it does

- Critical requests (admin) get 50 dedicated workers
- High/Normal/Low/Lowest get separate concurrency pools
- Total concurrency capped at 500
- Excess low-priority requests are shed with 503 + Retry-After

## Run

```bash
go run examples/scheduler/main.go
```

## Test

```bash
# Normal priority
curl http://localhost:3000/api/data

# Critical priority (admin path)
curl http://localhost:3000/admin/health

# Custom priority header
curl -H "X-Priority: high" http://localhost:3000/api/data
```
