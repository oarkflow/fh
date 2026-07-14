# Secure WASM request/response example

This example demonstrates the complete `mw/securetransport` browser flow. It does not send a nonce as an untrusted standalone header. The WASM client generates a fresh AEAD nonce, request ID, and monotonically increasing sequence for every request; the encrypted response is authenticated and bound to that exact request ID and sequence.

## Run locally

From the repository root:

```sh
make wasm
go run ./examples/secure_wasm
```

Open `http://127.0.0.1:8080` and use the development credentials `demo` / `demo`.

Generate a persistent X25519 server key:

```sh
go run ./examples/secure_wasm -generate-key
```

The loopback example permits an ephemeral key and ephemeral session-cookie secret so it is easy to run. Both are rejected when `FH_EXAMPLE_ORIGIN` uses HTTPS unless they are explicitly configured.

## Production configuration

```sh
export FH_EXAMPLE_ADDR=':8443'
export FH_EXAMPLE_ORIGIN='https://app.example.com'
export FH_SECURE_SERVER_KEY='<base64url 32-byte X25519 private key>'
export FH_EXAMPLE_SESSION_SECRET='<base64url random value of at least 32 bytes>'
export FH_EXAMPLE_USER='<bootstrap login user>'
export FH_EXAMPLE_PASSWORD='<bootstrap login password>'
```

Terminate TLS 1.3 at a correctly configured reverse proxy or replace `Listen` with `ListenTLS`. Store the transport key and session secret in a secret manager or KMS; never commit either value. Replace the demonstration login, in-memory stores, and transfer handler with real authentication, distributed atomic replay storage, encrypted session-key storage, authorization, idempotency, and transactional persistence.

For a real application, pin the server public key in a trusted client build or separately signed configuration. The example obtains it from `/secure-config.json` to remain runnable; a pin obtained over the same potentially intercepted connection is not an independent trust anchor.

## Security flow

1. `/auth/login` creates a signed, `HttpOnly`, `SameSite=Strict` web session and regenerates its identifier.
2. `/secure-config.json` requires that session and issues a random, 90-second, one-time registration grant bound to the user and web-session ID.
3. The browser creates a non-extractable Ed25519 device key in WebCrypto/IndexedDB.
4. Device registration consumes the one-time grant and binds the device public key to the authenticated principal.
5. The device signs a ClientHello containing its ID, a fresh X25519 key, timestamps, build ID, and random nonce.
6. The server combines its pinned persistent X25519 key with a fresh ephemeral key and proves possession through the authenticated handshake transcript.
7. HKDF derives separate AES-256-GCM request and response keys.
8. Each request authenticates the method, exact path/query, session ID, random request ID, sequence, timestamps, expiry, and nonce. Application headers and body are encrypted.
9. Each response authenticates its status, session ID, matching request ID and sequence, timestamps, expiry, nonce, headers, and exact body bytes. Missing, replayed, expired, oversized, or altered responses fail closed before application code receives them.

## Important boundary

This protects traffic against a proxy that can alter bytes but cannot change the trusted client or obtain the server key. It does not make a hostile browser or rooted device trustworthy. Code running in the page can observe plaintext before encryption or after verification, and a proxy able to replace the HTML, JavaScript, WASM, and their integrity metadata can replace the verifier itself.

The example's one-time registration grant limits CSRF and replay, but it is not hardware attestation. A proxy that can steal the authenticated cookie and bootstrap grant may race device registration. High-value deployments should require a WebAuthn/attested-device step-up or administratively pre-register the device public key.

HTTPS remains mandatory. Browser-enforced headers and `Set-Cookie` cannot be restored by WASM and therefore depend on TLS, secure cookie attributes, and server-side session validation. The server must remain authoritative for authentication, authorization, balances, roles, transaction state, and success/failure outcomes.
