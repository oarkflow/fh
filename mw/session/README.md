# Session Middleware

## What it does

Loads and persists server-side session data using memory, file, or custom stores with secure cookie handling.

## How to implement

```go
package main

import (
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func main() {
	app := fh.New()
	store := kv.NewMemoryStore(kv.WithGCInterval(time.Hour), kv.WithMaxEntries(100000))
	manager := session.NewSessionManager(store, session.SessionSecret([]byte("at-least-32-bytes-of-random-secret")))
	app.Use(session.New(manager))

	app.Get("/", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Adds cookie parsing and store I/O. The default memory-backed `kv.Store` is fast but not cluster-safe.

## Ordering guidance

Run after security/real IP and before CSRF/auth/handlers that need session state.

## Production considerations

Use secure, HttpOnly, SameSite cookies. Rotate session IDs after login. Use Redis/SQL or sticky sessions for multi-node deployments.
