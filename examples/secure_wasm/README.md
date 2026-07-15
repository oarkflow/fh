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
export FH_SECURE_SERVER_KEY='<base64url 32-byte X25519 private key>'
export FH_RESPONSE_SIGNING_PRIVATE_KEY='<base64url Ed25519 private seed>'
export FH_RESPONSE_SIGNING_KEY_ID='secure-wasm-response-2026-01'
export FH_EXAMPLE_SESSION_SECRET='<base64url random value of at least 32 bytes>'
export FH_EXAMPLE_USER='<bootstrap login user>'
export FH_EXAMPLE_PASSWORD='<bootstrap login password>'
```

Terminate TLS 1.3 at a correctly configured reverse proxy or replace `Listen` with `ListenTLS`. Store the transport key and session secret in a secret manager or KMS; never commit either value. Replace the demonstration login, in-memory stores, and transfer handler with real authentication, distributed atomic replay storage, encrypted session-key storage, authorization, idempotency, and transactional persistence.

For a real application, pin both the X25519 transport public key and Ed25519 response-signing public key in a trusted client build or separately signed configuration. The example obtains them from `/secure-config.json` to remain runnable; pins obtained over the same potentially intercepted connection are not independent trust anchors.

## Trusted production build

Production WASM refuses to initialize without a complete build-time trust bundle. After configuring the two private keys and production origin, print the corresponding public build variables:

```sh
export FH_EXAMPLE_ORIGIN='https://app.example.com'
export FH_SECURE_SERVER_KEY='<private X25519 key from the secret manager>'
export FH_SECURE_SERVER_KEY_ID='secure-wasm-example-v1'
export FH_RESPONSE_SIGNING_PRIVATE_KEY='<private Ed25519 seed from the secret manager>'
export FH_RESPONSE_SIGNING_KEY_ID='secure-wasm-response-2026-01'

go run ./examples/secure_wasm -print-wasm-trust
```

Supply all five printed public values to the build:

```sh
make wasm \
  WASM_TRUSTED_ORIGIN='https://app.example.com' \
  WASM_TRUSTED_TRANSPORT_KEY='<printed X25519 public key>' \
  WASM_TRUSTED_TRANSPORT_KEY_ID='secure-wasm-example-v1' \
  WASM_TRUSTED_RESPONSE_KEY='<printed Ed25519 public key>' \
  WASM_TRUSTED_RESPONSE_KEY_ID='secure-wasm-response-2026-01'
```

The production client rejects an absent or partial embedded bundle, a different runtime origin, substituted keys or key IDs, missing signatures, altered ciphertext, and negotiation downgrade. `secure.sessionInfo().trustMode` must report `embedded`.

Sign the generated release and pin `examples/secure_wasm/wasm/securefetch.wasm` using the generated SRI value from an independently trusted application release, native wrapper, managed browser extension, TUF/Sigstore metadata, or equivalent deployment channel. Serving the WASM hash and WASM bytes through the same intercepted first connection does not establish trust.

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
