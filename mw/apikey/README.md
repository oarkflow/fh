# API Key Middleware

## What it does

Authenticates requests using an API key from a header or custom extractor. It is suitable for service-to-service APIs, partner APIs, and machine clients.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apikey"
)

func main() {
	app := fh.New()
	app.Use(apikey.New(apikey.Config{Header: "X-API-Key", Lookup: func(c fh.Ctx, key string) bool { return key == "dev-key" }}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds a key lookup per request. With an in-memory lookup the overhead is small; with a remote lookup, cache positive and negative results carefully.

## Ordering guidance

Run after request ID/real IP/tracing and before authorization, rate limits that depend on client identity, and handlers.

## Production considerations

Store only hashed API keys. Support rotation, expiration, scopes, tenant binding, and audit logs. Avoid putting keys in query strings because they leak into logs and proxies.

