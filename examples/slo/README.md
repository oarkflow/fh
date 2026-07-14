# SLO Tracking Example

Demonstrates route-level SLO monitoring with burn rate alerts.

## What it does

- Tracks 99.9% availability for /api/users (200ms P99)
- Tracks 99.99% availability for /api/orders (500ms P99)
- Fires alerts when burn rate exceeds 2x
- Fires recovery alerts when SLO returns to normal
- Exposes SLO dashboard at /admin/slo

## Run

```bash
go run examples/slo/main.go
```

## Test

```bash
# Normal requests
curl http://localhost:3000/api/users
curl http://localhost:3000/api/orders

# View SLO state
curl http://localhost:3000/admin/slo
```
