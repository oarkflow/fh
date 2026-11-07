# FH WASM Secure Request and Response Flow

## Summary

`@oarkflow/fh-secure-fetch` is a browser client for FH's application-layer secure transport. It runs a Go client compiled to WebAssembly, uses native Fetch for networking, WebCrypto and IndexedDB for device identity, and returns ordinary Fetch `Response` objects to application code.

There are two request formats:

- **Protocol v1 (default):** encrypts a complete request and response body before sending or exposing it. Optional RFC 9421/RFC 9530 Ed25519 response signatures cover the complete encrypted response.
- **Protocol v2 (opt-in):** encrypts request and response bodies as authenticated records and streams them with backpressure. The current v2 client does not support the optional whole-body response-signature policy; it rejects that configuration rather than weakening it.

Both modes use a device-bound X25519 session, AES-256-GCM, request target and method binding, session expiry, sequence numbers, request IDs, and server-side replay checks. Use HTTPS in production. This transport adds application-layer protection; it does not replace TLS, server authorization, or validation.

```ts
// Obtain runtime pins and SRI values from the trusted host application's
// authenticated bootstrap endpoint. The server public key is not compiled
// into the WASM binary.
const configResponse = await fetch("/secure-config.json", {
  credentials: "same-origin",
  cache: "no-store",
});
if (!configResponse.ok) throw new Error("Secure bootstrap failed");
const secureConfig = await configResponse.json();
const secure = await createSecureFetch(secureConfig);

const response = await secure.fetch("/api/profile");
const profile = await response.json();
```

For v2, omit `responseSigningPublicKey` and `requireResponseSignature`, set `streaming: true`, and pass a browser `ReadableStream` body for methods that allow one. `response.body` is then a `ReadableStream`; each chunk is authenticated before it is delivered.

This page describes the complete browser, WASM, wire-protocol, middleware, and application flow. The security threat model and server deployment guide are in [`docs/secure-wasm-transport.md`](../docs/secure-wasm-transport.md). The runnable server and browser example is in [`examples/secure_wasm`](../examples/secure_wasm).

## Components

| Component | Responsibility |
| --- | --- |
| `wasm/src/secure-fetch.ts` | Public Fetch-compatible API, runtime startup, asset integrity checks, conversion to/from browser `Request` and `Response` |
| `wasm/src/storage.ts` | IndexedDB device identity and WebCrypto signing bridge |
| `wasm/cmd/securefetch` | Go protocol client compiled by TinyGo; handshake, encryption, validation, and native Fetch calls |
| `pkg/securetransport` | Shared wire formats, key derivation, envelope encoding, AEAD, replay and time-related protocol helpers |
| `mw/securetransport` | Server handshake and device endpoints, request decryption, replay enforcement, response encryption |
| `wasm/dist` | Generated JS, declarations, TinyGo runtime, WASM binary, checksums, and integrity manifest |
| `examples/secure_wasm` | End-to-end server, login/device-registration flow, and browser example |

The browser wrapper owns URL and Request normalization. Go/WASM owns the cryptographic protocol. The FH middleware owns server-side session and replay decisions. The application handler sees the restored method, headers, and body, then writes a normal FH response.

## Startup and trust checks

`createSecureFetch(config)` performs these steps before its handle is returned:

1. Checks that WebAssembly, WebCrypto, IndexedDB, native Fetch, and a secure browser context are available. HTTP is allowed only for loopback development.
2. Resolves the WASM and `wasm_exec.js` URLs. Cross-origin assets require integrity pins; redirects, credentials in asset URLs, and fragments are rejected.
3. Saves the native Fetch function privately so installing the optional global wrapper cannot recursively call itself.
4. Installs the IndexedDB storage bridge and loads the patched TinyGo runtime. The runtime script uses browser SRI when configured.
5. Downloads the WASM binary. With an integrity pin, it starts compilation from a cloned response while hashing the original bytes. The module is instantiated only after the SHA-256 SRI value matches. Without an integrity pin, it uses `instantiateStreaming` where available and falls back to byte compilation.
6. Starts the Go runtime and waits for `FHSecureWasm` to register its API.
7. Initializes the Go client with origin, URL prefix, timeouts, body/header limits, credentials, and runtime trust pins. Non-loopback use requires the X25519 server public key and key ID; the WASM binary does not contain server keys.
8. Loads the browser device identity from IndexedDB, or creates and registers a device when none exists.
9. Establishes an encrypted session before returning the handle.

The returned `SecureFetchHandle` has `fetch`, `sessionInfo`, `metrics`, `revokeSession`, and `resetDevice` methods. `metrics()` reports initialization time and the selected stream protocol version. It does not estimate network throughput.

### Main client settings

| Setting | Purpose |
| --- | --- |
| `baseURL`, `prefix` | API origin and secure-transport endpoint prefix. Destinations outside this origin are rejected. |
| `pinnedServerKey`, `pinnedServerKeyID` | Runtime X25519 server public key and key ID. Both are required outside loopback. |
| `responseSigningPublicKey`, `responseSigningKeyID`, `requireResponseSignature` | Optional v1 response signature verification policy. It cannot be combined with v2 streaming. |
| `wasmURL`, `wasmExecURL` | WASM module and patched TinyGo runtime locations. |
| `wasmIntegrity`, `wasmExecIntegrity`, `requireAssetIntegrity` | SHA-256 SRI pins for the module and runtime script. |
| `streaming` | Select protocol v2. The server must support it and upload streams are one-use. |
| `credentials`, `deviceName`, `clientBuild`, `registrationToken` | Fetch credentials and device registration/handshake metadata. |
| `handshakeTTL`, `requestTTL`, `maxSessionTTL`, `clockSkew` | Client time bounds in milliseconds; align them with server policy. |
| `maxBody`, `maxHeaders` | Client protocol limits. The server and proxy enforce their own limits too. |
| `installGlobal` | Replace `globalThis.fetch` after startup. Prefer the returned handle when possible. |

The transport and response public keys are public, not secrets. They are kept out of the WASM binary; the host application supplies them when initializing the client. Store private server keys only on the server.

## Device identity and registration

The device signing key is an Ed25519 WebCrypto key. Its private `CryptoKey` is non-extractable and remains in the browser storage bridge; Go/WASM receives signing input and receives the signature back. The public key and generated device ID are registered with the server.

Registration must be authorized by the application. The server's `AuthorizeDeviceRegistration` callback should bind the registered device to an authenticated user or to a short-lived, single-use bootstrap grant. The included example uses a normal FH login session and a one-time registration grant. Do not enable unauthenticated registration in production.

The browser wrapper is not a security boundary against a compromised page. Same-origin injected code can ask the legitimate storage bridge to sign a handshake and can observe plaintext at application boundaries. Keep normal authentication, authorization, CSRF defenses, and server-side validation in place.

## Session establishment

The client and server establish an authenticated session using two X25519 shared secrets:

1. The browser creates or loads the device identity and generates a fresh client X25519 ephemeral key pair.
2. It builds a `ClientHello` with device ID, client ephemeral public key, build identifier, issued/expiry times, and a random nonce.
3. The browser storage bridge signs the canonical hello bytes with the device Ed25519 key. The private key is not exported.
4. The client sends the hello to the secure transport session endpoint.
5. The server checks hello format, time bounds, device existence/status, signature, and replay nonce. It creates a fresh server X25519 ephemeral key and session ID.
6. The browser checks the server's static X25519 public key against the runtime pin and key ID supplied by the host application.
7. Browser and server each derive one shared secret from the client ephemeral key and the server static key, and a second from the two ephemeral keys. HKDF-SHA-256 derives separate client-to-server and server-to-client AES-256-GCM keys from both secrets and the handshake transcript.
8. The server includes a keyed proof over the transcript. The client verifies the proof before it stores the session keys.

The ephemeral contribution provides forward secrecy if ephemeral secrets are discarded. Session keys stay in process memory on the Go side and in the WASM client state on the browser side. A session is renewed near expiry. Session and device stores should be durable/shared for multi-instance deployment; replay storage must provide atomic insert-if-absent with expiry.

## Protocol v1: complete-body request and response

V1 is the default and supports the optional whole-response signature policy.

### Request construction in the browser

1. The wrapper creates a native `Request`, normalizes JSON-like bodies to JSON, checks the method/mode, and computes the exact target as `pathname + search`.
2. For body-capable methods, it reads the full request body into memory. GET and HEAD do not carry a body.
3. Go/WASM increments the session sequence and creates a random request ID and nonce plus issued/expiry times.
4. Application content type and permitted application headers are encoded with the body in a request payload. The request's method, exact target, session, request ID, sequence, times, and nonce are authenticated as AEAD associated data.
5. AES-256-GCM encrypts and authenticates the payload. The outer request uses `X-FH-Secure: 1` and `Accept: application/fh-secure`.
6. GET/HEAD carry the encoded encrypted envelope in `X-FH-Envelope`. Other methods send the encrypted bytes as the request body with the secure media type.
7. Native Fetch uses `cache: no-store`, `redirect: error`, `referrerPolicy: no-referrer`, caller credentials and AbortSignal, and only `cors` or `same-origin` mode.

### Request processing in FH

1. `Transport.Middleware()` decides whether the route is protected and detects the v1 marker. The middleware validates origin and Fetch Metadata according to server configuration.
2. It parses bounded metadata, looks up the session/device, checks expiry and device state, decrypts with the client-to-server key, and authenticates the method and original request target.
3. It inserts the sequence and request ID in the replay store. Duplicate values are rejected.
4. It strips outer transport headers, restores only allowed protected headers and content type, replaces the request body with the authenticated plaintext, and adds secure session/device/request values to context locals.
5. The configured session validator and downstream middleware/handler run. The handler uses the normal FH context API.

### Response construction and verification

1. FH captures the handler status, allowed response headers, content type, and body. The response payload is encrypted with the server-to-client key and bound to the request method, target, and status.
2. Ordinary responses carry the encrypted envelope body and outer marker `X-FH-Secure: 1` with media type `application/fh-secure`. HEAD and body-forbidden statuses carry the encrypted envelope in `X-FH-Response`.
3. The browser requires the protection marker and media type, bounds and reads the encrypted response, and optionally verifies the negotiated RFC 9421 signature over the ciphertext first.
4. It decrypts/authenticates the response and checks the session ID, request ID, sequence, status, issue/expiry times, and request timing.
5. The wrapper restores the protected headers and content type and returns an ordinary browser `Response` to the caller.

When `responseSigningPublicKey` is configured, each v1 request negotiates a fresh signature nonce inside the encrypted request. The client validates the RFC 9530 content digest and Ed25519 RFC 9421 signature over the outer ciphertext before decrypting. Cross-origin deployments must expose the signature headers through CORS.

V1 necessarily buffers request and response bodies. Apply explicit body limits and use v2 for large or incremental bodies where the response-signature policy permits it.

## Protocol v2: authenticated streaming

Set `streaming: true` on `createSecureFetch` to choose protocol v2. The protocol is a sequence of independently authenticated AES-GCM records. Records use a 32 KiB plaintext chunk size; the browser bridge also rejects an individual input chunk larger than 1 MiB. Backpressure is preserved through browser `ReadableStream` pulls and Go `io.Reader`/`io.Writer` interfaces.

### V2 request

1. The wrapper creates a browser `Request` but does not call `arrayBuffer()` on its body. For body-capable methods it passes the `Request.body` stream to Go/WASM.
2. Go/WASM creates stream metadata with session ID, request ID, sequence, issue/expiry times, and a random stream nonce. It binds records to the method and exact target.
3. An encrypted `HEAD` record carries content type and permitted application headers. Each `DATA` record carries up to 32 KiB of plaintext and has its own AEAD tag and monotonically increasing record counter. An authenticated `END` record carries the total plaintext byte count.
4. For GET/HEAD, the small complete stream (head plus end) is sent in `X-FH-Envelope`. Other methods use a browser `ReadableStream` backed by an `io.Pipe`; Fetch uses `duplex: "half"` and the stream media type.
5. FH validates session, device, times, method/target binding and replay metadata before dispatch. It reads/authenticates the head, restores application headers, and installs the decoder as the request body reader. It does not expose a data chunk to the handler until that record passes AEAD verification.
6. The FH middleware drains and verifies any unread request tail before it commits the response. A missing/invalid END, truncated record, wrong counter, altered tag, oversized body, or trailing data causes failure. The server uses configured request limits and reserves memory for queued HTTP/2 request chunks.

### V2 response

1. After the application handler returns, FH creates fresh response stream metadata bound to the request ID and sequence.
2. It protects the response status, content type and permitted headers in an encrypted `HEAD` record. The outer response identifies v2 and the stream media type; the response body is encrypted incrementally as `DATA` records followed by an authenticated `END` record.
3. For HEAD and body-forbidden statuses, FH places the small authenticated response stream in `X-FH-Response` instead of a body.
4. Go/WASM checks response marker, media type, session/request/sequence/time binding, and response status binding, then decrypts the head.
5. For a body response, the wrapper returns a standard `Response` whose body is a browser `ReadableStream`. Each browser pull reads and authenticates the next bounded record before enqueueing plaintext. Consumer cancellation closes the underlying reader.

Each delivered data record is authenticated. Completeness is established only when the reader reaches a valid END record and the byte count matches. Code that needs whole-object atomicity should finish consuming the stream and check it completed before acting on the full object.

The whole-body RFC 9421/RFC 9530 signature mode is not currently defined for v2. If either `requireResponseSignature` or `responseSigningPublicKey` is configured, the TypeScript wrapper rejects `streaming: true` before sending a request. Use v1 for that policy.

## Browser API usage

### V1 buffered request

```ts
const response = await secure.fetch("/api/orders", {
  method: "POST",
  headers: {
    "content-type": "application/json",
    authorization: "Bearer <application token>",
  },
  body: JSON.stringify({ orderId: 42 }),
});
const order = await response.json();
```

Application headers such as authorization are inside the encrypted payload. They are still visible to application JS before encryption; the browser transport does not protect the page from itself.

### V2 streamed upload and download

```ts
const secureStream = await createSecureFetch({
  ...baseSecureConfig,
  streaming: true,
});

const upload = new ReadableStream<Uint8Array>({
  async start(controller) {
    controller.enqueue(new TextEncoder().encode("first part\n"));
    controller.enqueue(new TextEncoder().encode("second part\n"));
    controller.close();
  },
});

const response = await secureStream.fetch("/api/import", {
  method: "POST",
  headers: { "content-type": "text/plain" },
  body: upload,
});

if (!response.body) throw new Error("Expected a response stream");
for await (const chunk of response.body) {
  processChunk(chunk); // plaintext arrives only after its record authenticates
}
```

The request body stream is consumed once. Do not combine streaming uploads with automatic retries unless the caller can construct a fresh body stream for every attempt. The secure transport itself sends one request and does not retry it.

### Axios-style client and progress

`createClient(fetchLike)` accepts any Fetch-compatible function. `createSecureClient(config)` builds on the secure Fetch handle. Both offer request/response transforms, interceptors, timeout/cancellation, retry policy and `responseType` handling.

- For v1 buffered operations, upload/download progress events are synthetic start/end notifications and report `synthetic: true`.
- For stream request/response bodies, progress callbacks count bytes actually pulled through the streams and report `synthetic: false`.
- `responseType: "stream"` returns the raw response `ReadableStream`; it does not parse JSON or consume it.
- When reading a stream, progress reflects consumption. If the caller never pulls the response body, download progress cannot advance.

## Server setup and middleware order

`securetransport.Install(app, cfg)` installs routes for device registration and session establishment and mounts the request middleware. Protect API paths and require origin validation. Supply persistent keys and production stores.

An application typically orders middleware as follows:

```text
TLS/connection limits and host validation
security headers and CORS/preflight
normal login session / authentication
FH secure transport
response-signature middleware (v1; after secure transport)
authorization, request validation, business routes
```

Session middleware comes before secure transport when registration or `ValidateSession` needs the normal login session. The response signer must wrap the final v1 ciphertext representation. Do not place response transformations after signing/encryption unless they preserve the wire representation.

The v2 transport is opt-in per request and the server accepts it through the same secure transport middleware. Ensure proxy/CDN layers preserve the stream media type and `X-FH-*` protocol headers, allow streaming request bodies, avoid buffering if low latency matters, and do not rewrite protected responses. For cross-origin use configure CORS to allow `X-FH-Secure`, `X-FH-Envelope`, `X-FH-Stream`, the secure media types, and required credentials; expose `X-FH-Response`, `X-FH-Stream`, and `X-FH-Secure` to browser JS. Exact settings depend on whether browser preflight is used.

## Trust, keys, and deployment

- Keep HTTPS/TLS enabled outside loopback. Application encryption does not hide traffic size/timing, server address, or all outer HTTP metadata.
- Keep the persistent server private key in a secret manager or KMS. The WASM binary contains neither the server private key nor its public pin.
- The WASM binary contains no server key. Non-loopback clients require a runtime X25519 public key and key ID supplied by the host application. If response signatures are required, supply the Ed25519 verification key and key ID through the same trusted configuration mechanism.
- Treat runtime pin provenance as part of the trust design. A pin fetched from the API over the same unauthenticated channel it is meant to authenticate adds no independent trust. HTTPS authenticates the app/config delivery origin; use a separately signed manifest, native wrapper, or managed deployment when that origin is not sufficient.
- Require asset integrity in production and publish the asset manifest through a separately trusted release process. A site that can replace the page, JS loader, WASM, and pins together controls the initial trust decision.
- Use shared atomic replay storage and appropriate shared session/device stores across server replicas. Process-local memory stores are bounded and suitable only for single-process or development use.
- Set application body limits, request/response memory budgets, rate limits, authn/authz, CSRF protections where applicable, and audit callbacks.
- Never embed server private keys, bearer tokens, or signing secrets in WASM. Public pins are not secrets, but this build keeps even those outside the artifact so the host can manage trust and key rotation.
- Rotate server keys with key IDs and an overlap strategy. A single transport instance uses one active static transport key.

### Security layers and limits

| Layer | What it protects | What it does not protect |
| --- | --- | --- |
| Browser HTTPS/Web PKI | Authenticates delivery of the page, JS, WASM, and runtime config to the browser; protects the network connection. | A compromised app server, malicious trusted CA, browser extension, or compromised endpoint. |
| SRI and immutable assets | Detects a WASM/runtime file that differs from the hash trusted by the page; prevents silent CDN/file substitution when the page itself is trusted. | A compromised page that can change both the SRI value and the asset URL. |
| Runtime X25519 pin and authenticated handshake | Binds the secure session to the expected FH server public key and key ID; detects a different API server when the pin came from a separately trusted source. | A pin/config fetched from the same attacker-controlled channel as the server being pinned. |
| Device Ed25519 key in WebCrypto | Proves possession of a registered browser device key without exporting its private key. | XSS or same-origin code that can invoke the signing bridge; browser/OS compromise. |
| AES-GCM transport (v1/v2) | Confidentiality and integrity for protected headers and bodies between the client and the trusted FH server. | Traffic timing/size, server address, plaintext at either application endpoint, or a compromised endpoint. |
| Replay, session, origin, and device checks | Rejects stale/replayed messages and devices/sessions that the server has revoked or invalidated. | Application authorization mistakes, weak login, missing CSRF/rate limits, or stale distributed state. |
| Application controls | Authentication, authorization, schema validation, idempotency, audit, transaction policy, abuse limits. | These are application responsibilities; secure transport does not add them automatically. |

There is no self-authenticating WASM binary in a normal browser. The browser must have an external trust path for the page and the runtime pin configuration. Removing a public key from WASM prevents build-time embedding, but does not make a compromised page safe: altered WASM can ignore runtime pins or expose plaintext. Protect the page and artifact delivery, keep secrets server-side, and enforce policy on the server.

## Limits and error behavior

- V1 buffers a whole body, bounded by protocol and browser limits. Its outer response signature also requires the complete ciphertext.
- V2 is bounded by configured `MaxBody` and fixed-size encrypted records; Go/WASM keeps only bounded chunks in flight. FH request/response memory budgets cover framework buffering paths such as queued HTTP/2 request data and buffered response transforms.
- GET/HEAD v2 request metadata is carried in a header and must remain small enough for browser, proxy, and server header limits.
- Fetch redirects are rejected because redirects change the request target and can leak a protected request to a different destination.
- Browser cancellation stops Fetch; on the server, an interrupted stream cannot produce a valid authenticated end record. FH aborts partial responses instead of sending a successful terminator after a callback failure.
- A response body is single-consumption. Cancellation or early close means the caller has not verified stream completion.
- The custom protocol has unit/integration tests and bounded parsing, but it has not had an independent cryptographic audit.

## Build and verify

From the repository root, `make wasm` builds TinyGo/WASM and TypeScript artifacts, copies the patched TinyGo runtime, creates SHA-256/SRI values, and synchronizes the example bundle. The build needs TinyGo and Binaryen (`wasm-opt`). TinyGo 0.41.x supports Go 1.19–1.26; put a Go 1.26 toolchain on `PATH` for the WASM build even when the main repository uses a newer Go toolchain.

```sh
make wasm
go test ./pkg/securetransport ./mw/securetransport ./mw/proxy
go test -race ./pkg/securetransport ./mw/securetransport
```

Serve `.wasm` as `application/wasm`. Check `wasm/dist/asset-manifest.json` for the artifact hashes and SRI values. `wasm/dist/securefetch.wasm` is generated and ignored by Git; a release pipeline must archive/publish the generated binary with the matching manifest.

## Summary

The browser establishes a device-bound encrypted session, then sends each API operation through native Fetch with an authenticated envelope (v1) or authenticated records (v2). FH verifies trust, binding, expiry, device status and replay state before exposing the request to application middleware. It encrypts the application response, and the browser authenticates and restores it as a normal Fetch `Response`. V1 supports whole-response signatures and buffers bodies; v2 streams bounded chunks with backpressure and per-record authentication, but currently excludes the whole-body response-signature policy.
