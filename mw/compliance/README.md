# Compliance Middleware

## What it does

Adds compliance-oriented request processing such as policy headers, audit integration, redaction awareness, or controls required by regulated workloads.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/compliance"
)

func main() {
	app := fh.New()
	app.Use(compliance.New(compliance.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Improves governance but may add logging/header work. Actual impact depends on enabled controls.

## Ordering guidance

Run after identity/tenant extraction and before audit/logging/handlers that need compliance context.

## Production considerations

Define data classification, retention, redaction, and audit rules. Validate behavior with compliance tests and privacy reviews.

