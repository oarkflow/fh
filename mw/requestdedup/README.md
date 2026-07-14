# requestdedup

Request deduplication middleware for fh. Prevents duplicate processing of identical requests within a configurable time window.

## Why

In distributed systems, clients may accidentally or maliciously send the same request multiple times due to retries, network duplicates, or UI double-clicks. Without deduplication, this causes duplicate charges, duplicate orders, duplicate email sends, and other side effects.

## Features

- SHA-256 based dedup keys (method + URL + body)
- Configurable time window and max key count
- Automatic LRU eviction when capacity is reached
- Custom key function support
- Returns 409 Conflict with original request metadata on duplicates

## Usage

```go
import "github.com/oarkflow/fh/mw/requestdedup"

app := fh.New()

dedup := requestdedup.New(requestdedup.Config{
    Window:  10 * time.Second,
    MaxKeys: 5000,
})

app.Post("/payments", dedup.Handler(), func(c fh.Ctx) error {
    // Only processes once per dedup window
    return c.JSON(fh.Map{"status": "success"})
})
```

## Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| Window | `time.Duration` | 5s | Time window for dedup detection |
| MaxKeys | `int` | 10000 | Max tracked keys before LRU eviction |
| KeyFunc | `func(Ctx) string` | SHA-256(method+URL+body) | Custom key extraction |
| OnDuplicate | `func(Ctx, *Entry) error` | 409 Conflict JSON | Custom duplicate handler |
