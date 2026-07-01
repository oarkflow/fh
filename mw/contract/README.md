# Contract Middleware

## What it does

Enforces route/API contract rules such as required headers, version compatibility, or request/response expectations depending on configuration.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/contract"
)

func main() {
	app := fh.New()
	app.Use(contract.New(contract.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves API correctness and client compatibility. Overhead depends on the number and complexity of checks.

## Ordering guidance

Run after version extraction and before handlers. In development, run stricter checks; in production, keep only required admission checks.

## Production considerations

Keep contracts under source control. Add CI checks for OpenAPI/schema compatibility and route behavior.

