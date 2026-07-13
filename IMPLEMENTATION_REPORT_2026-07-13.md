# fh implementation report — 2026-07-13

## Outcome

This revision closes the measured HTTP/1 benchmark gap while adding modern
server and security features outside the default fast path. The normal `Ctx`
handlers used by the comparison were retained; benchmark routes were not
replaced by static-response helpers.

## Performance work

- One HTTP/1 context and response buffer per keep-alive connection.
- No per-request context/response `sync.Pool` traffic for HTTP/1.
- ModeFast removes graceful activity atomics and bounded reference clears.
- Direct map JSON encoding into reserved connection-buffer space.
- Zero-allocation direct JSON header assembly.
- Cached small 200 text/JSON header blocks.
- Opt-in benchmark CPU profiling through `FH_CPU_PROFILE`.

Five-round local medians at 100 connections put fh ahead of both peers on
plaintext, JSON, params and query. See `docs/PERFORMANCE_CHANGES_2026-07-13.md`
for the exact short-run numbers and limitations.

## Server and protocol work

- Production `ReadHeaderTimeout` with an absolute, non-renewable header budget.
- Absolute chunked-body read deadlines.
- TLS state propagated into HTTP/1.1 and HTTP/2 request contexts.
- TLS 1.3 server config builder, verified mTLS policy and atomic certificate
  reload callback.
- HTTP/2 stream window synchronization fixed; the full race suite passes.
- Native QUERY remains supported and `mw/acceptquery` adds RFC 10008 discovery
  and media-type enforcement.

## Security and middleware work

- `mw/contentdigest`: RFC 9530 SHA-256/SHA-512 request verification and response
  generation using constant-time comparison.
- `mw/decompress`: bounded gzip request decompression with decoded-size and
  expansion-ratio limits.
- `mw/realip`: secure-by-default trusted CIDRs, RFC 7239 Forwarded parsing and
  right-to-left proxy-chain validation.
- Rate-limit key extraction now consumes normalized `Ctx.IP()` and no longer
  trusts forwarding headers directly.
- `mw/mtls` consumes native verified TLS chains and rejects unverified peer
  certificates unless explicitly configured otherwise.

## Validation

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- HTTP/1 TLS, HTTP/2 TLS, verified mTLS, read-timeout, Content-Digest,
  decompression-limit, real-IP chain and zero-copy JSON regression tests.

## Deliberate boundaries

The core remains standard-library-only and does not wrap `net/http` or
`fasthttp`. HTTP/3/QUIC and WebTransport are not represented as fake stubs;
they require a production QUIC transport and should live in an optional
adapter/module so the HTTP/1 fast path and zero-dependency core remain intact.
The existing timestamped HMAC signature middleware is not labeled as a full
RFC 9421 HTTP Message Signatures implementation.

