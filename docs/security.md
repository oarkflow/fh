# Modern server and security capabilities

## TLS and mutual TLS

`NewServerTLSConfig` defaults to TLS 1.3 and validates certificate and client-CA
requirements. `CertificateReloader.Reload` atomically publishes a new PEM pair
for subsequent handshakes.

```go
reloader, err := fh.NewCertificateReloader("server.crt", "server.key")
if err != nil { log.Fatal(err) }

tlsConfig, err := fh.NewServerTLSConfig(fh.ServerTLSOptions{
    GetCertificate: reloader.GetCertificate,
    ClientCAs: clientRoots,
    RequireClientCertificate: true,
})
if err != nil { log.Fatal(err) }

log.Fatal(app.ServeTLS(listener, tlsConfig))
```

TLS state and verified client chains are propagated to HTTP/1.1 and HTTP/2
request contexts. `mw/mtls` consumes that native state; client-certificate
headers are not trusted.

## Absolute read budgets

Production mode defaults `ReadHeaderTimeout` to five seconds. It is armed once
after the first request byte and is not extended by incremental reads. Chunked
bodies likewise use one absolute body deadline, preventing slow senders from
renewing a deadline indefinitely.

## Proxy identity

`mw/realip` ignores forwarding headers unless the immediate socket peer is in
`TrustedProxies`. It supports `Forwarded`, `X-Forwarded-For`, and common
single-IP edge headers, walking chains from right to left until the first
untrusted hop. Rate limiters and logging then consume the normalized `Ctx.IP()`.

## Message integrity and compressed input

- `mw/contentdigest` verifies and emits RFC 9530 `Content-Digest` fields using
  SHA-256 or SHA-512 and constant-time comparison.
- `mw/decompress` supports gzip request content with both decoded-size and
  expansion-ratio limits.
- `mw/signature` provides timestamped HMAC request/webhook authentication.
- `mw/replay` provides nonce replay protection with a pluggable store.

## HTTP QUERY

`mw/acceptquery` emits RFC 10008 `Accept-Query` as a Structured Fields List and
can enforce the advertised media types on QUERY request content.

