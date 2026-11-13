# Replay Protection Middleware

## What it does

Rejects repeated requests within a TTL using a nonce/signature/key store. It is useful for signed webhooks and high-risk operations.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/replay"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func main() {
	app := fh.New()
	app.Use(replay.New(replay.Config{Store: kv.NewMemoryStore()}))

	app.Get("/", func(c fh.Ctx) error {
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Prevents replay attacks. Store cardinality and TTL affect memory/storage usage.
The built-in store retains up to 100,000 markers by default. At capacity it
rejects new nonces and preserves existing markers until expiry.
Replay keys are limited to 256 bytes by default (`MaxKeyBytes`), including
when a custom store is supplied. Oversized request keys receive `400` before
they reach the store.

## Ordering guidance

Run after signature/key extraction and before side-effect handlers.

## Production considerations

Use a distributed store for multi-node deployments. Include timestamp skew validation and key scoping.

## Persistent storage

`Config.Store` is a `kv.Store` and defaults to `kv.NewMemoryStore`. For replay markers that must survive a process restart on a single node, construct a `kv.NewFileStore(dir, kv.WithFileGCInterval(gcInterval))` and pass it as `Config.Store`. It is not a substitute for a distributed store across multiple nodes.
