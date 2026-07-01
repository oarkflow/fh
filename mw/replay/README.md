# Replay Protection Middleware

## What it does

Rejects repeated requests within a TTL using a nonce/signature/key store. It is useful for signed webhooks and high-risk operations.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/replay"
)

func main() {
	app := fh.New()
	app.Use(replay.New(replay.Config{Store: replay.NewMemoryStore()}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Prevents replay attacks. Store cardinality and TTL affect memory/storage usage.

## Ordering guidance

Run after signature/key extraction and before side-effect handlers.

## Production considerations

Use a distributed store for multi-node deployments. Include timestamp skew validation and key scoping.

