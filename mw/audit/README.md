# Audit Middleware

## What it does

Records structured audit events for requests, responses, identity, tenant, route, and error outcomes. It is intended for compliance, security review, and incident investigation.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/audit"
)

func main() {
	app := fh.New()
	app.Use(audit.New(audit.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds logging/sink overhead. The impact depends on the sink; asynchronous/batched sinks are preferred for high-throughput production services.

## Ordering guidance

Run after authentication/tenant extraction and before handlers. Run after request ID/correlation ID so audit events are traceable.

## Production considerations

Redact secrets and regulated data. Use append-only durable sinks for compliance. Define retention and access controls. Avoid logging request bodies by default.

