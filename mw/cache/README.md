# Cache Middleware

## What it does

Caches eligible responses using an in-memory or custom store. It can reduce handler work and improve latency for repeated safe requests.
By default it stores up to 128 entries and caches bodies up to 256 KiB each,
limiting retained response bodies to roughly 32 MiB before encoding and
metadata overhead. Tune `MaxEntries` and `MaxBodySize` for the application's
memory budget.

## How to implement

```go
package main

import (
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/cache"
)

func main() {
	app := fh.New()
	app.Use(cache.New(cache.Config{}))

	app.Get("/", func(c fh.Ctx) error {
		return c.Status(fh.StatusOK).SendString("ok")
	})
}
```

## Impact

Can significantly reduce CPU/database load. Memory usage depends on response size, TTL, and key cardinality.

## Key and response safety

Authorization-bearing requests bypass the cache by default. Cookie-bearing
requests also bypass it unless `AllowRequestCookies` is enabled; when enabled,
the cookie value becomes part of the hashed key. Requests with credential or
identity-like query parameters always bypass caching. Requests with identity,
tenant, or credential headers bypass caching unless the header is explicitly
listed in `VaryHeaders`. Malformed query strings and conditional or range
requests bypass the cache. The default key includes the request scheme, host,
and URI, then the complete key is hashed before it reaches the store.
Request `max-age` and `min-fresh` constraints are enforced against cached
entries; `no-cache` and `no-store` requests go to the handler. Duplicate
request cache directives, `Pragma`, and conditional fields are all inspected,
so a later field cannot hide a cache-bypass instruction.
The HTTP method is always part of the key. A custom `KeyGenerator` must still
identify the resource and any application-specific dimensions that are not
represented by the URI or `VaryHeaders`.

Responses with `Cache-Control: private`, `no-cache`, or `no-store`, response
cookies, or `Vary: *` are not stored. Every named response `Vary` header must
also appear in `VaryHeaders`; otherwise fh skips storage rather than serving a
representation under an incomplete key. All occurrences of response
`Cache-Control`, `Vary`, `Set-Cookie`, `Age`, and `Expires` are considered;
ambiguous repeated `Age` or `Expires` values cause the response to be skipped.
Response `max-age`, `s-maxage`,
`Expires`, and `Age` values cap the configured TTL; zero or expired freshness
does not enter the cache. Eligible end-to-end response headers are preserved
on hits; hop-by-hop headers and `Set-Cookie` are excluded, and responses with
cookies are not cached.

## Ordering guidance

Run after request normalization and authentication only when cache keys include identity/tenant. Run before expensive handlers.

## Production considerations

Be careful with personalized responses. Add representation headers such as
`Accept-Language` or `Accept-Encoding` to `VaryHeaders` when responses use
them. Identity-bearing headers must be explicitly varied before they can be
cached. Keep sensitive values out of custom key generators; the resulting key
is hashed, but application code and custom stores may have their own logs.

## Persistent storage

For a cache that survives process restarts, construct `kv.NewFileStore(dir, kv.WithMaxEntrySize(maxEntryBytes), kv.WithFileGCInterval(gcInterval))` and pass it as `cache.Config.Store`. `Store` is a `kv.Store`; `kv.NewMemoryStore` remains the default when left nil.
