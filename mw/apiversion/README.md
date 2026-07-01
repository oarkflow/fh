# API Version Middleware

## What it does

Extracts and validates API version information from headers, query parameters, or route context so the app can enforce supported versions.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apiversion"
)

func main() {
	app := fh.New()
	app.Use(apiversion.New(apiversion.Config{Header: "X-API-Version", Supported: []string{"v1", "v2"}, Default: "v1"}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Prevents unsupported client versions from reaching handlers and provides consistent API lifecycle behavior.

## Ordering guidance

Run before contract validation and handlers. Run after rewrite/proxy normalization if the version is encoded in the path.

## Production considerations

Document deprecation dates, use `Sunset`/deprecation headers where applicable, and add contract tests per supported version.

