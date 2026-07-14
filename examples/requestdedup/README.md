# Request Dedup Example

Demonstrates request deduplication to prevent duplicate processing.

## What it does

- POST /payments uses dedup with a 10-second window
- POST /webhooks uses dedup to prevent webhook replays
- Duplicate requests within the window return 409 Conflict

## Run

```bash
go run examples/requestdedup/main.go
```

## Test

```bash
# First request succeeds
curl -X POST http://localhost:3000/payments -d '{"amount": 99.99}'

# Duplicate within 10s returns 409
curl -X POST http://localhost:3000/payments -d '{"amount": 99.99}'
```
