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

For a cache that survives process restarts, construct `kv.NewFileStore(dir, kv.WithMaxEntrySize(maxEntryBytes), kv.WithFileGCInterval(gcInterval))` and pass it as `cache.Config.Store`. `Store` is a `kv.Store`; `kv.NewMemoryStore` remains the default when left nil.
