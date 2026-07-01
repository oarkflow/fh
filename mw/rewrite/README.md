# Rewrite Middleware

## What it does

Rewrites request paths or hosts according to configured rules. It is useful for migrations, vanity URLs, and gateway routing.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/rewrite"
)

func main() {
	app := fh.New()
	app.Use(rewrite.New(rewrite.Rule{From: "/old", To: "/new"}))

	app.Get("/new", func(c fh.Ctx) error { return c.String(fh.StatusOK, "new") })
}
```

## Impact

Low overhead per rule. Poorly designed rules can make routing confusing.

## Ordering guidance

Run before router-dependent middleware/handlers if the rewritten path should drive route matching.

## Production considerations

Keep rewrite rules explicit and tested. Avoid open redirect patterns and log rewrites during migrations.

