# CSRF Middleware

## What it does

Protects browser session-based applications from cross-site request forgery using tokens and origin checks.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/csrf"
)

func main() {
	app := fh.New()
	app.Use(csrf.New(csrf.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.String(fh.StatusOK, "ok")
	})
}
```

## Impact

Adds token validation to unsafe methods. Essential for cookie-authenticated browser apps.

## Ordering guidance

Run after session/cookie middleware and before handlers for POST/PUT/PATCH/DELETE.

## Production considerations

Use SameSite cookies and HTTPS. APIs using bearer tokens generally do not need CSRF, but browser cookie flows do.

