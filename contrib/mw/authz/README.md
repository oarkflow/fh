# Authorization Policy Middleware

## What it does

Integrates `github.com/oarkflow/authz` with `fh`. It evaluates policy decisions using subjects from principals, headers, or JWT claims and resources/actions from request context.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/contrib/mw/authz"
)

func main() {
	app := fh.New()
	// engine := authzmw.LoadEngineFromAuthzFile("policy.authz")
	app.Use(authzmw.FHWithConfig(authzmw.FHConfig{
		Subject: authzmw.SubjectFromPrincipal(),
		Action: authzmw.ActionFromMethod(),
		Resource: authzmw.ResourceFromRoute("route", ""),
	}))

	app.Get("/admin", func(c fh.Ctx) error { return c.String(fh.StatusOK, "ok") })
}
```

## Impact

Centralizes RBAC/ABAC/permission decisions without duplicating authorization middleware in core `mw`. Evaluation overhead depends on policy complexity.

## Ordering guidance

Run after authentication/JWT/API-key/tenant extraction and before protected handlers.

## Production considerations

Keep authorization here instead of creating parallel RBAC/ABAC middleware. Version policy files, test decisions, log denials, and choose fail-closed behavior for protected routes.

