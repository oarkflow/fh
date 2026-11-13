# fh HTTP Client

`fh.NewClient` provides a root-package outbound HTTP client with a middleware-first design, production defaults, typed JSON helpers, retries, circuit breaking, rate limiting, bulkheads, lifecycle hooks, metrics adapters, SSRF protection, request/response body limits, multipart upload, file download, async calls, batch calls, and `fh` JSON engine integration.

The default transport retains at most 100 idle connections total and 10 per
host, and allows at most 100 connections per host. Set
`MaxIdleConns`, `MaxIdleConnsPerHost`, and `MaxConnsPerHost` explicitly when
your measured workload needs different pool limits; larger limits can raise
file-descriptor and upstream connection usage across many client instances.
Set `ForceAttemptHTTP2` when custom dial or TLS settings would otherwise keep
the standard transport from automatically enabling HTTP/2. HTTP/2 is enabled
by default; set `DisableHTTP2` to turn it off for HTTP/1-only upstreams. If both
are set, `ForceAttemptHTTP2` takes precedence.

## Minimal usage

```go
client := fh.NewClient(fh.ClientConfig{
    BaseURL: "https://api.example.com",
    Timeout: 3 * time.Second,
})
defer client.Close()

var out UserResponse
res, err := client.R().
    Header("Authorization", "Bearer token").
    Query("active", "true").
    Decode(&out).
    Get(ctx, "/users")
```

## Production middleware stack

```go
signingSecret, err := config.RequireSecretString(
    "SIGNING_SECRET",      // legacy direct-value fallback
    "SIGNING_SECRET_FILE", // preferred mounted secret
)
if err != nil {
    log.Fatal(err)
}
client.Use(
    fh.ClientRecover(),
    fh.ClientLogger(fh.NewDefaultLogger()),
    fh.ClientCircuitBreaker(fh.CircuitConfig{FailureThreshold: 5}),
    fh.ClientBulkhead(512, 100*time.Millisecond),
    fh.ClientRateLimit(5000),
    fh.ClientHMACSigner("X-Signature", signingSecret),
)
```

`ClientBulkhead` holds its slot until the response body reaches EOF or is
closed. Close response bodies, including streamed responses, so capacity is
returned promptly.

`ClientLogger` and client error records use a sanitized URL: userinfo is fully
masked, fragments are omitted, and query keys associated with credentials
(including OAuth `code`, `state`, `nonce`, `signature`, and token fields) have
their values removed. Error messages also scrub embedded URLs and common
credential key/value forms before logging or formatting a `ClientError`.
Lifecycle hooks receive the original `ClientEvent.Err` for `errors.Is` and
`errors.As`; sanitize it before writing it to application logs.

## Security

Enable strict outbound protections for untrusted URLs:

```go
client := fh.NewClient(fh.ClientConfig{
    Security: fh.ClientSecurity{
        Strict: true,
        RequireHTTPS: true,
        AllowedHosts: map[string]bool{"api.example.com": true},
    },
})
```

Strict mode blocks localhost, link-local and private IP targets unless explicitly allowed.
Allowed and blocked host rules canonicalize DNS case/root dots and IP literal
spellings, including bracketed IPv6 and IPv4-mapped IPv6.
Strict mode also rejects a configured proxy because the proxy resolves the final
destination and the client cannot pin that DNS result. Apply equivalent destination
restrictions at the proxy if proxying is required. It also rejects a caller-supplied
`Transport`, since fh cannot guarantee that a custom round tripper uses its validating
dial path.
Outbound security checks run after client middleware, so URL rewrites such as
load balancing are validated against the final destination before transport.

The default retry policy bounds total retry time and uses capped exponential
backoff with jitter. If a custom policy leaves `MaxElapsed` unset, an upstream
`Retry-After` longer than `MaxDelay` (two seconds when unset) ends retries
instead of keeping the request goroutine waiting for an unbounded interval.

## Typed helpers

```go
user, err := fh.GetJSON[User](ctx, client, "/users/42")
created, err := fh.PostJSON[CreateUser, User](ctx, client, "/users", req)
```

## Lifecycle hooks

```go
client := fh.NewClient(fh.ClientConfig{
    Hooks: fh.ClientHooks{
        OnBeforeRequest: func(e fh.ClientEvent) {},
        OnAfterResponse: func(e fh.ClientEvent) {},
        OnRetry: func(e fh.ClientEvent) {},
        OnError: func(e fh.ClientEvent) {},
    },
})
```

## Testing

The implementation includes unit coverage for JSON decoding, retries, middleware application, circuit breaker behavior, and strict SSRF protection.

```sh
go test -run 'TestHTTPClient' .
```

## Additional Production Features

The root `fh` package HTTP client also includes:

- replayable JSON/form/multipart bodies for safe retry behavior
- status-error middleware through `ClientRequireStatus`
- pluggable bearer token source through `ClientBearerTokenSource`
- request id propagation through `ClientRequestID`
- W3C trace context injection through `ClientTraceParent` and `ClientTraceContext`
- in-memory HTTP cache with ETag / Last-Modified support through `ClientCache`
- service clients through `client.Service(baseURL)`
- round-robin load balancing through `NewRoundRobinSelector` and `ClientLoadBalance`
- health-aware endpoint selection through `NewHealthAwareSelector`
- atomic download support through `Response.SaveAtomic`
- streaming download support through `Response.DownloadTo`
- stricter dial-path SSRF protection through `ClientSecurityDialContext`

`ResponseBodyLimit` applies to `Response.Raw.Body`, `Response.Reader`, `Bytes`,
`DownloadTo`, and `SaveAtomic`. It defaults to 16 MiB so response reads cannot grow without
bound; set it to a negative value to disable the limit. `Request.ResponseLimit`
overrides it for one request, with the same negative-value opt-out. An oversized
response returns `ErrClientBodyTooLarge`. Streaming reads and downloads expose
at most the configured number of bytes; an atomic download leaves its
destination untouched on this error.

`Request.Param` values are escaped as individual URL path components, so
reserved characters such as `/` remain escaped on the wire and are not
double-encoded.

### Streaming multipart uploads and body limits

`Request.Multipart` streams fields and files as the request is sent. File paths
are reopened for retries; one-shot `io.Reader` files are sent once even if the
retry policy would otherwise retry the method. Upload errors are returned by
`Do` instead of being discarded. A configured `BodyLimit` rejects known
oversized bodies before sending and returns `ErrClientBodyTooLarge` when a
stream crosses the limit. A streaming body may already have sent bytes when
the limit is reached, so use a known-size body when the server must receive
nothing on rejection.

### Streaming request compression

`ClientGzipRequest(min)` compresses request bodies through a pipe instead of
buffering them in memory. A positive `min` uses a known `ContentLength` as the
uncompressed-size threshold; if the length is unknown or below the threshold,
the body is passed through unchanged. Replayable requests receive a matching
gzip `GetBody` callback so retries and redirects send compressed data too.

### Service client

```go
api := client.Service("https://api.example.com/v1").
    Header("Authorization", "Bearer token")

res, err := api.Get(ctx, "/users")
```

### Cache

```go
client.Use(fh.ClientCache(fh.NewMemoryHTTPCache(time.Minute, 1<<20)))
```

The in-memory cache keeps at most 1,024 entries and an estimated 16 MiB of
cached data. It bypasses credentialed requests (including URL userinfo),
requests with sensitive query keys, conditional/range requests, responses with
`Vary`, and `no-store`/`no-cache` requests or responses. It also bypasses
requests with custom headers whose names indicate credentials or identity
(such as API keys, tenant, account, or user identifiers). When composing it
with middleware that adds credentials, install that middleware before
`ClientCache` so cache hits also see the credentials and bypass storage. Cache
keys are fixed-size hashes, so raw URL paths and query values are not retained
as map keys.

### Request id and trace propagation

```go
client.Use(fh.ClientRequestID("X-Request-ID"), fh.ClientTraceContext())
ctx = fh.ClientTraceParent(ctx, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
```

### Round-robin load balancing

```go
selector, _ := fh.NewRoundRobinSelector(
    "https://api-1.example.com",
    "https://api-2.example.com",
)
client.Use(fh.ClientLoadBalance(selector))
```

### Health-aware load balancing

```go
selector, err := fh.NewHealthAwareSelector(fh.HealthSelectorConfig{
    FailureThreshold: 3,
    EjectionDuration: 30 * time.Second,
}, "https://api-1.example.com", "https://api-2.example.com")
if err != nil { return err }
client.Use(fh.ClientLoadBalance(selector))
```

Transport errors and HTTP 500, 502, 503, and 504 count toward ejection by
default. Successful responses reset the failure count. After the cooldown,
one request probes the endpoint; other requests use healthy endpoints. If all
endpoints are ejected or being probed, requests fail with
`ErrClientNoHealthyEndpoints`. `selector.Snapshot()` exposes current state.
Canceled requests do not count as endpoint failures. The selector affects
which endpoint each retry uses; the client's retry policy still decides
whether a request is retried.

### Atomic file download

```go
res, err := client.R().Stream().Get(ctx, "https://example.com/file.zip")
if err == nil {
    err = res.SaveAtomic("file.zip", 0644)
}
```

## Complete method coverage

The root client exposes the standard HTTP methods directly and supports custom/extension methods without forcing another package:

```go
client.Get(ctx, "/users")
client.Head(ctx, "/users")
client.Options(ctx, "/users")
client.Trace(ctx, "/trace")
client.Delete(ctx, "/users/42")
client.Post(ctx, "/users", req)
client.Put(ctx, "/users/42", req)
client.Patch(ctx, "/users/42", patch)
client.Query(ctx, "/users/search", filter) // HTTP QUERY method
client.Search(ctx, "/users/search", filter)
client.Do(ctx, fh.MethodPropFind, "/dav/path")
```

The fluent request builder exposes the same operations where there is no naming conflict. `Request.Query(k, v)` remains the URL query-parameter helper, so the HTTP `QUERY` method is called through `Do`, `Method`, or the root `Client.Query` helper:

```go
res, err := client.R().
    Query("active", "true").
    QueryMap(map[string]string{"sort": "name"}).
    QueryRaw("page=1&limit=50").
    Get(ctx, "/users")

res, err = client.R().
    JSON(filter).
    Do(ctx, fh.MethodQuery, "/users/search")
```

Built-in extension method constants include `MethodQuery`, `MethodSearch`, `MethodPropFind`, `MethodPropPatch`, `MethodMKCol`, `MethodCopy`, `MethodMove`, `MethodLock`, `MethodUnlock`, `MethodReport`, `MethodPurge`, `MethodLink`, and `MethodUnlink`.
