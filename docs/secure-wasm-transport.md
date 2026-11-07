# FH Secure WASM Transport

`mw/securetransport` and `wasm/` implement an application-layer encrypted Fetch transport for FH. It is additional protection on top of HTTPS/TLS 1.3, not a TLS replacement.

For the full current browser/WASM startup, handshake, v1 request/response, and v2 streaming walkthrough, see [`wasm/WASM.md`](../wasm/WASM.md). This page focuses on deployment and threat-model guidance.

## Security properties

- A persistent pinned X25519 server key authenticates the FH secure-transport endpoint.
- Every session also uses a fresh server X25519 ephemeral key, providing forward secrecy after that ephemeral key is discarded.
- Each browser installation owns an Ed25519 device-signing key.
- The browser generates a non-extractable WebCrypto Ed25519 private `CryptoKey` and stores it in IndexedDB; the private key is never exported to JavaScript or WASM.
- Device-signed handshakes bind the device ID, client ephemeral key, build ID, timestamps, and nonce.
- HKDF-SHA-256 derives independent client-to-server and server-to-client AES-256-GCM keys.
- Method, exact request target, session ID, request ID, sequence, timestamps, expiry, and nonce are authenticated as AEAD associated data.
- Request bodies, application content type, and application headers such as `Authorization` are encrypted.
- Response status, content type, application headers, body, session, request ID, and request sequence are encrypted/authenticated.
- Sequence numbers and request IDs are independently replay-protected.
- The server validates exact origins and Fetch Metadata context.
- Application response headers are hidden from the outer response by default and restored only after decryption.
- GET/HEAD requests carry their small encrypted request envelope in `X-FH-Envelope`; body-capable methods use a binary body.
- HEAD, 1xx, 204, and 304 responses carry the authenticated response envelope in `X-FH-Response` because HTTP forbids a response body.
- Device and session stores, replay storage, registration authorization, key persistence, security event reporting, and device revocation are pluggable.
- Optional RFC 9421/RFC 9530 verification signs the outer ciphertext response with Ed25519 and is checked before decryption.

## Browser security boundary

WASM is tamper-resistant, not tamperproof. A compromised page, malicious extension, injected script, browser exploit, or compromised operating system can observe data before encryption or after decryption. FH must still enforce authentication, authorization, validation, rate limits, and transaction policy on the server.

Do not embed server private keys, permanent API secrets, database credentials, or token-signing secrets in JavaScript or WASM.

## Runtime trust and asset integrity

A non-loopback WASM client requires a runtime X25519 transport public key and key ID. The WASM artifact contains no server key. The host application supplies those public pins during initialization. If RFC 9421 response signatures are required, the host also supplies the response verification public key and key ID.

The public key is not a secret; its source and the WASM artifact's integrity establish trust. A key fetched over an unauthenticated channel from the API it is supposed to authenticate is not an independent pin. HTTPS authenticates the application/configuration origin. If that origin is outside the trust boundary, deliver the runtime pins and WASM integrity manifest through a separately authenticated channel such as a signed release, native wrapper, or managed browser policy. A compromised application page can replace its WASM, configuration, and integrity reference together.

The browser trusts the application page through browser PKI and uses the runtime X25519 pin to authenticate the secure transport server handshake. SRI checks that loaded JS/WASM matches the hash supplied by the page; it does not authenticate a compromised page or a manifest and artifact delivered through the same compromised channel. For stronger separation, publish the asset manifest and runtime pins through a separately authenticated release system, signed native application, managed extension policy, TUF/Sigstore metadata, or another channel the attacker cannot rewrite. No JavaScript/WASM protocol can defeat a hostile installed root CA that can replace the page, loader, artifact, and pins together.

## Build

Requires [TinyGo](https://tinygo.org) and Binaryen's `wasm-opt` (`brew install
tinygo binaryen`); see [`wasm/README.md`](../wasm/README.md) for the Go/TinyGo
version pairing this needs.

```bash
make wasm
```

This performs all of the following:

1. Builds `wasm/cmd/securefetch` with `tinygo build -target wasm -no-debug`.
2. Copies the matching `wasm_exec.js` checked in next to `cmd/securefetch` --
   a patched copy of TinyGo's runtime glue, not the stock TinyGo or Go one.
3. Compiles the TypeScript facade and encrypted IndexedDB storage bridge.
4. Writes `wasm/dist/SHA256SUMS` and `wasm/dist/asset-manifest.json` with SHA-256/SRI pins.
5. Synchronizes a complete runnable copy into `examples/secure_wasm/wasm`.

Artifacts:

```text
wasm/dist/
├── securefetch.wasm
├── wasm_exec.js
├── secure-fetch.js
├── storage.js
├── index.js
├── *.d.ts
├── asset-manifest.json
└── SHA256SUMS
```

Serve `.wasm` as `application/wasm`. Serve all artifacts from the same trusted origin or a tightly controlled origin permitted by CSP.

## Generate and persist the server key

```bash
go run ./examples/secure_wasm -generate-key
```

Store the returned base64url value in a secret manager or KMS-protected configuration:

```bash
export FH_SECURE_SERVER_KEY_FILE='/run/secrets/fh-transport-key'
```

Production initialization rejects an absent server key. `AllowEphemeralServerKey` exists only for tests/local development; it invalidates pins and sessions on restart.

## Server installation

```go
privateValue, err := config.RequireSecretString(
    "FH_SECURE_SERVER_KEY",      // legacy direct-value fallback
    "FH_SECURE_SERVER_KEY_FILE", // preferred mounted secret
)
if err != nil {
    log.Fatal(err)
}
serverKey, err := securetransport.DecodeServerPrivateKey(privateValue)
if err != nil {
    log.Fatal(err)
}

app := fh.NewProduction()

transport, err := securetransport.Install(app, securetransport.Config{
    ServerPrivateKey: serverKey,
    KeyID:            "api-transport-2026-01",
    RequireSecure:    true,
    Protect: func(c fh.Ctx) bool {
        return strings.HasPrefix(c.Path(), "/api/")
    },
    AllowedOrigins: []string{"https://app.example.com"},
    RequireOrigin:  true,
    AuthorizeDeviceRegistration: func(c fh.Ctx, request protocol.DeviceRegistrationRequest) (string, error) {
        // Require an existing authenticated session or one-time, same-origin,
        // server-issued bootstrap. Return the principal bound to the device.
        userID, ok := currentAuthenticatedUser(c)
        if !ok {
            return "", fh.NewHTTPError(fh.StatusForbidden, "DEVICE_REGISTRATION_FORBIDDEN", "device registration is forbidden")
        }
        return userID, nil
    },
})
if err != nil {
    log.Fatal(err)
}
_ = transport

responseSigner, err := httpsignature.New(httpsignature.Config{
    PrivateKey: responseSigningPrivateKey,
    KeyID:      "api-response-2026-01",
    Origin:     "https://api.example.com",
    Skip: func(c fh.Ctx) bool {
        return !strings.HasPrefix(c.Path(), "/api/")
    },
})
if err != nil {
    log.Fatal(err)
}
// Must be registered after securetransport: encrypt first, sign ciphertext last.
app.Use(responseSigner)
```

The safe defaults deliberately reject:

- Missing persistent server keys.
- Unauthorised device registration.
- Replayed handshakes.
- Reused request sequences.
- Reused request IDs.
- Expired or future-dated messages outside the configured skew.
- Cross-site Fetch Metadata contexts.
- Unprotected requests selected by `Protect` when `RequireSecure` is enabled.
- Dangerous restored headers such as `Host`, `Content-Length`, `Transfer-Encoding`, `Connection`, or FH envelope headers.

## Browser usage

```ts
import { createSecureFetch } from "/wasm/index.js";

const secure = await createSecureFetch({
  baseURL: "https://api.example.com",
  pinnedServerKey: "<base64url-x25519-public-key>",
  pinnedServerKeyID: "api-transport-2026-01",
  responseSigningPublicKey: "<base64url-ed25519-public-key>",
  responseSigningKeyID: "api-response-2026-01",
  requireResponseSignature: true,
  wasmURL: "/wasm/securefetch.wasm",
  wasmExecURL: "/wasm/wasm_exec.js",
  wasmIntegrity: "sha256-<from-asset-manifest>",
  wasmExecIntegrity: "sha256-<from-asset-manifest>",
  requireAssetIntegrity: true,
  credentials: "same-origin",
  clientBuild: "portal-2026.07.13",
  deviceName: "Operations browser",
});

const response = await secure.fetch("/api/orders", {
  method: "POST",
  headers: {
    "content-type": "application/json",
    authorization: "Bearer protected-inside-envelope",
  },
  body: { id: 42 },
});

console.log(response.status, await response.json());
```

`pinnedServerKey` is required by default. `allowUnpinnedServerKey: true` is enforced as a loopback-only development option and removes application-layer server authentication. `pinnedServerKeyID` additionally prevents an unexpected key/version from being selected during deployment or rotation.

With `requireResponseSignature: true`, `responseSigningPublicKey` and `responseSigningKeyID` are also mandatory. WASM generates a fresh RFC 9421 nonce, puts the negotiation inside the encrypted request, verifies the RFC 9530 digest and Ed25519 signature over the ciphertext response, and only then decrypts it. For cross-origin use, CORS must expose `Content-Digest`, `Signature-Input`, and `Signature`.

Every non-loopback execution requires runtime transport pins and reports `runtime-pinned` from `secure.sessionInfo().trustMode`. Unpinned loopback development reports `loopback-development`.

Set `requireAssetIntegrity: true` in production and provide the SHA-256 SRI values generated in `wasm/dist/asset-manifest.json`. The loader verifies the WASM bytes before instantiation and lets the browser enforce SRI on `wasm_exec.js`.

Set `installGlobal: true` only after compatibility testing. The wrapper preserves the original native Fetch function privately so it never recursively calls itself.

## Device registration

Device registration must be authorized. Recommended approaches, in descending order:

1. Existing authenticated user session plus step-up/WebAuthn for sensitive environments.
2. Short-lived, single-use server bootstrap bound to an HttpOnly, SameSite=Strict cookie and authenticated user.
3. Administrative pre-registration.

`AllowUnauthenticatedDeviceRegistration` is development-only.

The included example uses `mw/session`: `POST /auth/login` creates the normal FH session, device registration reads that session and binds the device principal to the logged-in user ID, and `ValidateSession` rejects encrypted API calls whose secure transport principal no longer matches the current session user. Replace the demo `demo` / `demo` login with your real authentication flow.

## Distributed deployment

The default stores are process-local and bounded. Multi-instance production deployments must provide shared `kv.Store` implementations:

- `ReplayStore`: Redis/Valkey or another atomic TTL-capable store.
- `SessionStore`: encrypted distributed session records or sticky sessions with explicit failover policy.
- `DeviceStore`: durable SQL/NoSQL device registry with revocation and audit fields.

Do not serialize plaintext session keys to an unencrypted database. Wrap session key material with a KMS/HSM key or keep it in a dedicated protected cache.

## Middleware ordering

Recommended order:

```text
connection/TLS limits
host guard
security response headers
CORS/preflight
session/authentication
FH secure transport
RFC 9421 response signer
authorization
schema validation
business handler
```

Put cookie/session middleware before FH secure transport when device registration or `ValidateSession` needs the normal login session. Register the RFC 9421 signer immediately after secure transport, then authorization and business handlers. On the response path, secure transport encrypts first and the signer covers the actual outer ciphertext representation.

## Operational requirements

- HTTPS/TLS 1.3 is mandatory outside loopback development.
- Use a strict CSP. Go WASM requires the narrower `'wasm-unsafe-eval'` source expression for WebAssembly compilation in browsers: `default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'`.
- Serve immutable, content-addressed WASM/JS files, enforce their SRI pins, and publish the generated manifest through a separately signed release process.
- Embed both public keys and the origin in the trusted WASM build. Runtime values are assertions only, not trust anchors.
- Rotate server keys with an overlap window and explicit key IDs. This implementation currently supports one active transport key per `Transport`; deploy parallel versions during rotation.
- Set bounded body/header limits.
- Export `OnSecurityEvent` to the audit/SIEM pipeline.
- Use `SessionFromContext`, `DeviceIDFromContext`, and `RequestIDFromContext` in authorization and audit records.

## Current limitations

- Protocol v1 buffers whole request and response bodies. Protocol v2 (`streaming: true`) encrypts/decrypts bounded records incrementally and reports real byte progress. The optional RFC 9421/RFC 9530 whole-body response signature is supported only by v1; v2 rejects that configuration until a streaming signature profile is defined.
- GET/HEAD encrypted request metadata must fit within the server/browser header limit.
- Native Fetch redirect behavior is deliberately set to `error`; redirects would otherwise break exact target binding or risk credential/envelope forwarding.
- Browser certificate pinning is unavailable through normal Fetch. The application-level server key pin protects this protocol but does not replace PKI/TLS validation.
- WebAuthn/hardware-backed device assertions are not yet wired into the transport. The current Ed25519 key is non-extractable but remains software-backed and can still be invoked by compromised same-origin code.
- A compromised browser runtime can still invoke the legitimate key operation or inspect plaintext at application boundaries.
- The custom protocol has tests and fuzz-friendly bounded parsers, but it has not undergone an independent external cryptographic audit. Perform one before high-value financial, healthcare, or regulated deployment.

## Verification

```bash
make secure-test
make wasm

go test ./pkg/securetransport ./mw/securetransport
go test -race ./pkg/securetransport ./mw/securetransport
```

The integration test performs device registration, signed handshake, dual static/ephemeral X25519 derivation, encrypted authorization/body delivery, encrypted response header/body recovery, request binding, and replay rejection against the native FH server.
