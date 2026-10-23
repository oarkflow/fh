# Trailing Slash Middleware

## What it does

Normalizes trailing slashes in request paths by either stripping or appending trailing slashes. It supports both HTTP redirection (`301 Moved Permanently` or `308 Permanent Redirect`) and transparent in-flight request rewriting.

## How to implement

### Strip Trailing Slashes with HTTP Redirect

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/trailingslash"
)

func main() {
	app := fh.New()

	// Redirect /users/ -> /users (301)
	app.Use(trailingslash.RemoveSlash(true))

	app.Get("/users", func(c fh.Ctx) error {
		return c.SendString("Users list")
	})

	app.Listen(":8080")
}
```

### Internal Transparent Rewrite (Zero Extra Network Roundtrip)

```go
// Transparently routes /users/ to /users without issuing an HTTP redirect
app.Use(trailingslash.RemoveSlash(false))
```

### Append Trailing Slash

```go
// Redirect /users -> /users/
app.Use(trailingslash.AddSlash(true))
```

## Impact

Sub-microsecond string comparison. When using `Redirect: false`, it uses `c.Rewrite` to reroute the request inside the existing connection, avoiding any additional round-trip latency.

## Ordering guidance

Run early in the middleware pipeline before routing and authorization handlers.

## Production considerations

- For public SEO-indexed web pages, HTTP redirection (`Redirect: true`) with status `301` is recommended to avoid duplicate content penalties from search engines.
- For internal microservices and high-throughput APIs, internal rewrite (`Redirect: false`) avoids the network latency of a second roundtrip.
- Root path `/` is automatically preserved.
