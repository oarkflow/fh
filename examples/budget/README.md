# Request Budget Example

Demonstrates hierarchical execution budgets with sub-budget carving.

## What it does

- Every request gets a 2-second deadline budget
- Budget includes memory, upstream calls, retries, log bytes limits
- Handler carves a 700ms sub-budget for database operations
- Budget checks prevent exceeding memory or body size limits

## Run

```bash
go run examples/budget/main.go
```

## Test

```bash
# Shows budget remaining after processing
curl http://localhost:3000/order/123

# Tests body size budget check
curl -X POST http://localhost:3000/checkout -d '{"items": [...]}'
```
