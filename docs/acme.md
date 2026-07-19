# ACME / Let's Encrypt (TLS-ALPN-01)

fh can obtain and renew TLS certificates automatically via ACME, built on
`golang.org/x/crypto/acme/autocert` — already a dependency of this module
(used for OCSP stapling in `tls_config.go`), so this adds no new third-party
package.

## Why TLS-ALPN-01 only

fh has no `net/http` dependency, so it deliberately supports only the
`tls-alpn-01` challenge type (RFC 8737): it validates entirely through the
existing TLS/ALPN listener fh already runs for HTTP/2 negotiation, with no
second listener, no port-80 HTTP handler, and no extra plumbing. `autocert`
enables `tls-alpn-01` automatically as long as `HTTPHandler` is never called
— which fh never does.

**Caveat**: environments that terminate TLS in front of fh (some CDNs/load
balancers) cannot complete `tls-alpn-01`, since the challenge is answered at
the TLS handshake layer. If that's your deployment, provision certificates
out-of-band and use `CertificateReloader` (see [Security](security.md))
instead.

## Usage

```go
app := fh.New()
app.Get("/", func(c fh.Ctx) error { return c.SendString("hello") })

log.Fatal(app.ListenAutoTLS(
    []string{"example.com", "www.example.com"},
    "/var/lib/fh/acme-cache", // persists issued certs + account state across restarts
))
```

Graceful-shutdown counterpart:

```go
log.Fatal(app.ListenAutoTLSWithGracefulShutdown(
    []string{"example.com"},
    "/var/lib/fh/acme-cache",
))
```

Both listen on `:443` (the port ACME validation and browsers both expect) and
enforce TLS 1.3, exactly like `ListenTLS`.

**`CacheDir` is required.** Without a persistent cache, every restart
re-issues certificates from scratch and risks hitting Let's Encrypt's rate
limits.

## Custom manager

For anything beyond the defaults (custom `HostPolicy`, sharing one manager
across multiple listeners, inspecting the manager directly), build it
yourself and hand its `TLSConfig()` to `ServeTLS`:

```go
mgr, err := fh.NewACMEManager(fh.ACMEOptions{
    Domains:  []string{"example.com"},
    CacheDir: "/var/lib/fh/acme-cache",
    Email:    "ops@example.com",
    HostPolicy: func(ctx context.Context, host string) error {
        // e.g. allow dynamic subdomains instead of an exact whitelist
        if strings.HasSuffix(host, ".example.com") {
            return nil
        }
        return errors.New("host not allowed")
    },
})
if err != nil {
    log.Fatal(err)
}

cfg := mgr.TLSConfig()
cfg.MinVersion = tls.VersionTLS13

ln, _ := net.Listen("tcp", ":443")
log.Fatal(app.ServeTLS(ln, cfg))
```

## Reference

| Type / Func | Description |
|---|---|
| `fh.ACMEOptions` | `Domains` (required), `CacheDir` (required), `Email`, `HostPolicy` |
| `fh.NewACMEManager(opt) (*autocert.Manager, error)` | Builds the manager; validates `Domains`/`CacheDir` |
| `app.ListenAutoTLS(domains, cacheDir) error` | Serves `:443` with ACME-issued certs |
| `app.ListenAutoTLSWithGracefulShutdown(domains, cacheDir) error` | Same, draining on SIGINT/SIGTERM |
