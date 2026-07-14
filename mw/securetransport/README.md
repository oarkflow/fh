# securetransport

`mw/securetransport` adds an application-layer encrypted Fetch transport for FH applications. It is designed for browser clients that load the Go-WASM client from `wasm/` and then call protected application routes through `secure.fetch`.

This is not a replacement for HTTPS. Use it with TLS. The middleware protects selected FH routes by encrypting application request bodies, response bodies, and application headers inside a device-bound, replay-resistant session.

## What It Provides

- Device registration with an Ed25519 browser-held key.
- Session establishment with pinned X25519 server keys and a per-session server ephemeral key.
- Separate AES-256-GCM keys for client-to-server and server-to-client traffic.
- Authenticated method, exact target path/query, session ID, request ID, sequence, timestamps, and expiry.
- Encrypted request and response bodies for any byte payload, not only JSON.
- Encrypted application headers, while stripping untrusted outer application headers.
- Replay protection by request sequence and request ID.
- Origin and Fetch Metadata validation for browser control and secure requests.
- Device revocation and session revocation hooks.
- Pluggable device, session, and replay stores.

## Quick Start

Generate and persist a server key:

```sh
go run ./examples/secure_wasm -generate-key
```

Install the middleware:

```go
serverKey, err := securetransport.DecodeServerPrivateKey(os.Getenv("FH_SECURE_SERVER_KEY"))
if err != nil {
    log.Fatal(err)
}

transport, err := securetransport.Install(app, securetransport.Config{
    ServerPrivateKey: serverKey,
    KeyID:            "server-v1",
    RequireSecure:    true,
    Protect: func(c fh.Ctx) bool {
        return strings.HasPrefix(c.Path(), "/api/")
    },
    AllowedOrigins: []string{"https://app.example.com"},
    RequireOrigin:  true,
    AuthorizeDeviceRegistration: func(c fh.Ctx, req protocol.DeviceRegistrationRequest) (string, error) {
        return "user-123", nil
    },
})
if err != nil {
    log.Fatal(err)
}
_ = transport
```

Expose the public key and WASM asset integrity values to the browser from a same-origin bootstrap endpoint:

```go
app.Get("/secure-config.json", func(c fh.Ctx) error {
    c.Set("Cache-Control", "no-store")
    return c.JSON(fh.Map{
        "baseURL":         "https://app.example.com",
        "pinnedServerKey": transport.PublicKeyBase64(),
        "keyID":           transport.KeyID(),
    })
})
```

## Configuration Notes

- `ServerPrivateKey` is required in production. `AllowEphemeralServerKey` is only for local development and tests because restart changes the server identity and invalidates active sessions and pins.
- `RequireSecure` plus `Protect` selects routes that must arrive through secure transport.
- `AuthorizeDeviceRegistration` should require an already-authenticated bootstrap signal, such as a one-time cookie or token.
- `AllowedOrigins` should contain every browser origin that may call the transport.
- `RequireOrigin` should be enabled for browser deployments. If a same-origin service worker path omits `Origin`, the middleware accepts the request only when `Sec-Fetch-Site: same-origin` is present and the `Host` matches an allowed origin.
- `ValidateSession` is the place to bind a secure transport session to your current login, tenant, risk decision, or authorization model.
- `HideResponseHeaders` defaults to true. Application response headers are carried inside the encrypted response unless explicitly exposed.

## Data Handling

The protocol carries raw bytes. JSON, text, form data, files, and binary payloads all work as long as the client encodes them into a Fetch body. The middleware restores the decrypted bytes with `fh.ReplaceRequestBody`, restores the encrypted `Content-Type`, and then your normal FH handlers read `c.Body()`, bind JSON, stream files, or inspect headers as usual.

Do not rely on plaintext outer request headers for application authorization. For secure requests, application headers must be inside the encrypted payload. The middleware strips untrusted outer headers before calling downstream handlers.

## Runtime Endpoints

The middleware registers these endpoints under `Prefix`, which defaults to `/__fh/secure/v1`:

- `POST /device/register`: plaintext control endpoint for authorized device registration.
- `POST /session`: plaintext control endpoint for creating an encrypted session.
- `POST /session/revoke`: secure endpoint. It must be called through secure transport so `SessionFromContext` is available.

Application routes protected by `Protect` are still your normal FH routes.

## Stores

The default memory stores are useful for tests and single-process development. Production deployments should use durable or distributed implementations:

- `DeviceStore`: persists registered devices and revocation status.
- `SessionStore`: stores active session keys. Protect this store carefully; do not serialize plaintext keys into an ordinary database.
- `ReplayStore`: tracks request sequence/request ID replay windows.

If a browser keeps a device identity but the server loses its `DeviceStore` state, the next session creation fails authentication. The WASM client treats a 401/403 from `/session` as a recoverable stale-device signal, clears the stored browser device, re-registers, and retries once. Production systems should still persist the device store so device identity survives normal deploys.

## Handler Helpers

```go
session, ok := securetransport.SessionFromContext(c)
deviceID, ok := securetransport.DeviceIDFromContext(c)
requestID, ok := securetransport.RequestIDFromContext(c)
```

Use `RevokeDevice(id)` to revoke a device and remove its active sessions.

## Testing

```sh
go test ./pkg/securetransport ./mw/securetransport
go test -race ./pkg/securetransport ./mw/securetransport
```

Build the browser client with:

```sh
make wasm
```

See `docs/secure-wasm-transport.md`, `wasm/README.md`, and `examples/secure_wasm/README.md` for the browser client and full example.
