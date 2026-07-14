# Config Reload Example

Demonstrates atomic configuration reload with generation tracking.

## What it does

- Every response includes X-Config-Generation headers
- POST /admin/reload atomically reloads all configuration
- Reload validates config, runs health checks, then swaps
- Failed reloads auto-rollback

## Run

```bash
go run examples/configreload/main.go
```

## Test

```bash
# View current generation
curl http://localhost:3000/

# Trigger reload
curl -X POST http://localhost:3000/admin/reload

# Check generation incremented
curl http://localhost:3000/admin/generation
```
