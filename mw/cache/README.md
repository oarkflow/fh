# Cache Middleware

## What it does

Caches eligible responses using an in-memory or custom store. It can reduce handler work and improve latency for repeated safe requests.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/cache"
)

func main() {
	app := fh.New()
	app.Use(cache.New(cache.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Can significantly reduce CPU/database load. Memory usage depends on response size, TTL, and key cardinality.

## Ordering guidance

Run after request normalization and authentication only when cache keys include identity/tenant. Run before expensive handlers.

## Production considerations

Be careful with personalized responses. Include tenant/user/version/query in cache keys. Respect cache-control rules and avoid caching sensitive data.

## Persistent storage

For a cache that survives process restarts, pass `Store: cache.NewFileStore(dir, maxEntries)` (or `cache.NewFileStoreWithGC(dir, maxEntries, gcInterval)` for periodic background eviction of expired entries) in `cache.Config`. Entries are written atomically as one JSON file per hashed cache key under `dir`; response bodies larger than 8MB are rejected defensively to avoid disk exhaustion. `MemoryStore` remains the default when `Store` is left nil.

