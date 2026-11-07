# Secure WASM request/response example

This example demonstrates the combined `mw/securetransport` plus RFC 9421/RFC 9530 browser flow. The WASM client generates a fresh signature nonce internally and carries `Accept-Signature` inside the encrypted request. The server encrypts the logical response and then signs the actual ciphertext HTTP representation. WASM verifies that signature before attempting AES-GCM decryption.

## Run locally

From the repository root:

```sh
go run ./examples/secure_wasm
```

Open `http://localhost:8080`, `http://127.0.0.1:8080`, or `http://0.0.0.0:8080` and use the development credentials `demo` / `demo`. All three development origins are explicitly allowlisted and the selected request origin is used consistently for login, encrypted transport, runtime configuration, and RFC 9421 absolute-URI binding. Unlisted hosts remain rejected.

The example is self-contained: `examples/secure_wasm/wasm` includes `securefetch.wasm`, the matching Go runtime, JavaScript modules, declarations, hashes, and integrity manifest. Run `make wasm` only when regenerating those checked-in assets after changing the WASM client.

Generate a persistent X25519 server key:

```sh
go run ./examples/secure_wasm -generate-key
```

Generate a persistent Ed25519 response-signing key:

```sh
go run ./examples/secure_wasm -generate-signing-key
```

The loopback example permits an ephemeral key and ephemeral session-cookie secret so it is easy to run. Both are rejected when `FH_EXAMPLE_ORIGIN` uses HTTPS unless they are explicitly configured.

## Production configuration

```sh
export FH_EXAMPLE_ADDR=':8443'
export FH_EXAMPLE_ORIGIN='https://app.example.com'
export FH_SECURE_SERVER_KEY_FILE='/run/secrets/fh-transport-key'
export FH_RESPONSE_SIGNING_PRIVATE_KEY_FILE='/run/secrets/fh-response-signing-key'
export FH_RESPONSE_SIGNING_KEY_ID='secure-wasm-response-2026-01'
export FH_EXAMPLE_SESSION_SECRET_FILE='/run/secrets/fh-session-secret'
export FH_EXAMPLE_USER='<bootstrap login user>'
export FH_EXAMPLE_PASSWORD_FILE='/run/secrets/fh-bootstrap-password'
```

Terminate TLS 1.3 at a correctly configured reverse proxy or replace `Listen` with `ListenTLS`. Store the transport key and session secret in a secret manager or KMS; never commit either value. Replace the demonstration login, in-memory stores, and transfer handler with real authentication, distributed atomic replay storage, encrypted session-key storage, authorization, idempotency, and transactional persistence.

The WASM binary contains no server public key or private key. The example supplies the public X25519 transport pin and response-signing key at runtime from `/secure-config.json` after login. This is appropriate when the browser trusts the HTTPS application origin that serves both the config and the WASM. If the app and API have separate trust domains, deliver the API pins through an independently authenticated application configuration channel. A key returned by the API being pinned is not independent proof of that API's identity.

The runtime pin authenticates the server handshake; it does not authenticate the WASM bytes. Protect the application page, JS loader, and WASM artifact with HTTPS, a restrictive CSP, immutable asset names and SRI values. Where the application origin itself is not a sufficient trust anchor, authenticate the asset manifest through a separately signed release, native wrapper, managed browser extension, or equivalent channel. A compromised page can replace the WASM, its runtime configuration, and the SRI reference together; browser WASM cannot defend itself from that.

Check `secure.sessionInfo().trustMode`: production with runtime supplied pins reports `runtime-pinned`; unpinned loopback development reports `loopback-development`.

## Security flow

1. `/auth/login` creates a signed, `HttpOnly`, `SameSite=Strict` web session and regenerates its identifier.
2. `/secure-config.json` requires that session and issues a random, 90-second, one-time registration grant bound to the user and web-session ID.
3. The browser creates a non-extractable Ed25519 device key in WebCrypto/IndexedDB.
4. Device registration consumes the one-time grant and binds the device public key to the authenticated principal.
5. The device signs a ClientHello containing its ID, a fresh X25519 key, timestamps, build ID, and random nonce.
6. The server combines its pinned persistent X25519 key with a fresh ephemeral key and proves possession through the authenticated handshake transcript.
7. HKDF derives separate AES-256-GCM request and response keys.
8. Each request authenticates the method, exact path/query, session ID, random request ID, sequence, timestamps, expiry, and nonce. Application headers and body are encrypted.
9. WASM adds a fresh RFC 9421 nonce and signature profile request inside the encrypted request headers; page script cannot inject or reuse it.
10. Secure transport encrypts/authenticates the logical response, including status, session ID, matching request ID and sequence, timestamps, expiry, headers, and exact body.
11. RFC 9421 signs the resulting ciphertext, outer status/content type, original method/target URI, key ID, timestamps, and nonce. WASM verifies RFC 9530 `Content-Digest` and Ed25519 before decryption, then verifies AES-256-GCM and request/session bindings before exposing plaintext.

## Important boundary

This protects traffic against a proxy that can alter bytes but cannot change the trusted client or obtain the server key. It does not make a hostile browser or rooted device trustworthy. Code running in the page can observe plaintext before encryption or after verification, and a proxy able to replace the HTML, JavaScript, WASM, and their integrity metadata can replace the verifier itself.

The example's one-time registration grant limits CSRF and replay, but it is not hardware attestation. A proxy that can steal the authenticated cookie and bootstrap grant may race device registration. High-value deployments should require a WebAuthn/attested-device step-up or administratively pre-register the device public key.

HTTPS remains mandatory. Browser-enforced headers and `Set-Cookie` cannot be restored by WASM and therefore depend on TLS, secure cookie attributes, and server-side session validation. The server must remain authoritative for authentication, authorization, balances, roles, transaction state, and success/failure outcomes.
