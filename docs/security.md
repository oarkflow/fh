# Modern server and security capabilities

## Fail-closed server baseline

Enable the framework-wide baseline when creating the app:

```go
app := fh.New(fh.WithSecureByDefault(true))
// or: fh.NewWithConfig(fh.Config{SecureByDefault: true})
```

The flag is resolved once during construction. It enables strict HTTP input
validation, bounds body/header/request-line/stream sizes and timeouts, keeps
panic recovery and redaction enabled, disables debug error and server-version
exposure, and emits HSTS, nosniff, frame, referrer, permissions, COOP, and CORP
headers. Stricter caller-supplied limits are preserved. Configuration loaded
through `pkg/config` accepts `server.secure_by_default` or
`FH_SECURE_BY_DEFAULT=true`.

Authentication, authorization, CORS, CSRF, trusted-host/proxy, rate-limit, and
application input-schema policies remain explicit middleware because safe
values depend on the deployment. Serve public traffic with `ServeTLS` or TLS at
a trusted edge; the flag cannot provision certificates.

`SecureByDefault` and production profiles also enforce a simultaneous
connection cap per socket peer. Configure it with
`WithMaxConnectionsPerIP`; the secure cap is 100. When an origin is reachable
only through a CDN or load balancer, this address is the edge proxy rather than
the end user, so size the listener cap for edge connection pools and apply
end-user limits after `mw/realip` validates the forwarding chain.

When reliability/idempotency is enabled, fh places it after global, group, and
route middleware and immediately before the final endpoint handler. Identity
middleware should populate `fh.Principal` before calling `Next`; idempotency
keys are then scoped to that authenticated principal. Replayed response
metadata never includes `Set-Cookie`.

The secure profile also disables h2c. Cleartext prior-knowledge and upgrade
HTTP/2 can be controlled independently with `WithDisableH2C`, without disabling
HTTP/2 negotiated through TLS ALPN.

HTTP/1 request lines and fields require CRLF framing, control bytes are rejected,
absolute-form authority must agree with `Host`, and only the transfer coding the
server actually decodes (`chunked`) is accepted. HTTP/1 and HTTP/2 both enforce
configured header-list, header-count, body-size, and absolute body-time limits.

`App.Static` and `Group.Static` use an OS-backed rooted filesystem. Symlinks may
resolve within the configured root but cannot escape it. `StaticFS` trusts the
confinement semantics of the caller-supplied `fs.FS` implementation.

For endpoints that may receive large bodies, configure `WithRequestHeadHandler`
to perform header-only authentication, admission control, or rate limiting before
the server sends `100 Continue` or reads the body. The hook receives matched route
parameters but must not attempt to access the body.

## Configuration and secrets

Use environment variables for small deployment overrides and for paths to
mounted secrets, not for persistent application data. `pkg/config.SecretString`
supports a direct-value variable for compatibility and a preferred file
variable such as `SIGNING_KEY_FILE=/run/secrets/signing-key`. It rejects both
being configured together, bounds file reads, and is intended to run once at
startup. Store sessions, queues, audit records, and other mutable data in their
configured storage backends.

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

For `SecureByDefault` applications, `ServeTLS` rejects TLS versions below 1.3.
The outbound client explicitly sets TLS 1.2 as its compatibility floor and
rejects `InsecureSkipVerify` unless `AllowInsecureTLS` is deliberately enabled.
Certificate pinning does not replace CA and hostname verification. OCSP staples
are generated with the Go `x/crypto/ocsp` implementation and validated for
signature, certificate status, and freshness before publication.

## DDoS and resource exhaustion

No in-process HTTP framework can absorb an attack that saturates the origin's
network link. Internet-facing production deployments should use a DDoS-capable
CDN/WAF or provider scrubbing service and firewall the origin so only that edge
can connect. Keep an emergency contact and mitigation procedure with the ISP or
cloud provider.

At the fh layer:

- use `SecureByDefault`, absolute read/body/TLS/idle timeouts, global and
  per-peer connection caps, bounded request dimensions, and quiet overload
  connection drops;
- install `mw/realip` only with explicit trusted proxy CIDRs, then apply both
  coarse per-IP and authenticated principal/API-key quotas;
- use tighter route-specific concurrency/rate limits for login, search,
  export, upload, webhook, and other expensive endpoints;
- use `WithRequestHeadHandler` to reject unauthorized or over-quota uploads
  before reading their bodies;
- cap decompressed size and expansion ratio, queue depth, cache cardinality,
  replay-store cardinality, WebSocket connections, message size, fragments,
  message rate, and idle lifetime;
- return generic overload responses and export metrics for rejected
  connections, 429/503 rates, latency, goroutines, memory, queue depth, and
  open HTTP/WebSocket connections.

The in-memory replay/timestamp/signature stores are bounded and fail closed;
they never evict a still-valid marker to admit a new one. Multi-instance
deployments must supply a bounded shared store so replay protection is global.

## Browser and WebSocket security

The CSRF middleware now preserves `Secure` cookies and requires Origin/Referer
validation even when passed a partial `Config`. Non-browser compatibility
requires the explicit `AllowInsecureCookie` or `AllowMissingOrigin` opt-outs;
prefer bypassing CSRF only on routes protected by non-cookie authentication.
Use an exact host allowlist to prevent Host-header and DNS-rebinding attacks.

WebSockets reject browser origins unless explicitly allowed, bound message and
frame sizes, fragmentation and message rates, and atomically enforce manager
connection limits. Authenticate the HTTP upgrade and authorize every topic or
event; do not treat a successful upgrade as permanent authorization.

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

IP reputation and allowlisting never parse forwarding headers themselves;
install `mw/realip` first. Basic Auth `RequireTLS` uses native verified fh TLS
state. When TLS terminates at an edge, provide `basicauth.Config.TLSCheck` backed
by server-validated listener/edge state rather than reading a client-supplied
`X-Forwarded-Proto` value.

## Message integrity and compressed input

- `mw/contentdigest` verifies and emits RFC 9530 `Content-Digest` fields using
  SHA-256 or SHA-512 and constant-time comparison.
- `mw/decompress` supports gzip request content with both decoded-size and
  expansion-ratio limits.
- `mw/signature` provides timestamped HMAC request/webhook authentication.
- `mw/replay` and `mw/timestamp` provide bounded nonce/timestamp replay
  protection with pluggable stores. For signed requests, require both a nonce
  and timestamp and bind method, canonical path, body digest, identity, nonce,
  and timestamp into the signature.

## HTTP QUERY

`mw/acceptquery` emits RFC 10008 `Accept-Query` as a Structured Fields List and
can enforce the advertised media types on QUERY request content.
