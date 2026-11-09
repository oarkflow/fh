# Secure WASM client

This directory contains the browser half of FH secure transport. It adds
application-layer authenticated encryption and signed-response verification on
top of HTTPS. It is not a TLS replacement.

## Quick local registration

1. From the application root, run `./run.sh`.
2. Open `http://localhost:8080`.
3. Sign in with username `admin` and password
   `change-me-in-development`.
4. The page automatically requests a one-use grant, creates the browser device
   key, registers it, and establishes the secure session.
5. Select **Load protected profile** to verify encrypted `/api/me` access.

No manual browser registration request is required. The development keys are
ephemeral, so after a server restart reload the page and sign in again. If local
state becomes inconsistent, clear cookies and IndexedDB for
`localhost:8080`, then retry.

These credentials and the default operations token are for loopback development
only. The complete initial setup, configuration table, route list, and
production checklist are in the application [`README.md`](../../README.md).


## Files

| File | Purpose |
|---|---|
| `securefetch.wasm` | TinyGo/WASM cryptographic protocol implementation |
| `wasm_exec.js` | Patched TinyGo runtime glue matching `securefetch.wasm` |
| `secure-fetch.js` | Secure Fetch facade, trust validation, SRI loading |
| `storage.js` | IndexedDB bridge for non-extractable browser device keys |
| `client.js` | Axios-style client (interceptors, retries, timeouts) built on `secure-fetch.js` or plain Fetch |
| `errors.js` | `FHError`, `isFHError`, `isCancel` used by `client.js` |
| `body.js` | Shared body/response helpers used by `secure-fetch.js` and `client.js` |
| `index.js` | Public JavaScript module entrypoint |
| `asset-manifest.json` | SHA-256 and SRI values for the distributable assets |
| `README.md` | Build, deployment, rotation, and troubleshooting guide |

The server refuses to start if the manifest lacks integrity entries for
`securefetch.wasm` or `wasm_exec.js`. The browser loader verifies those
values before using the runtime.

## Axios-style client

`secure-fetch.js`'s `createSecureFetch`/`secure.fetch` is the low-level,
Fetch-compatible primitive. `client.js` adds `createClient`/`createSecureClient`
on top of it -- axios-style ergonomics for any Fetch-compatible function
(native `fetch`, or the secure transport above): instances with shared
defaults, request/response interceptors, automatic JSON transforms,
query-param serialization, `timeout`, retries, and a unified `FHError`
(`error.response.status`/`error.response.data`) instead of manually checking
`response.ok`.

```js
import { createSecureClient, isFHError } from "/wasm/index.js";

const api = await createSecureClient(secure); // wraps an existing secure.fetch handle

api.interceptors.request.use((config) => {
  console.log(`-> ${config.method} ${config.url}`);
  return config;
});

try {
  const { data } = await api.get("/api/me");
  await api.post("/api/transfer", { to, amount }, { retries: 2, timeout: 5000 });
} catch (error) {
  if (isFHError(error)) console.error(error.code, error.response?.status, error.response?.data);
}
```

`onUploadProgress`/`onDownloadProgress` are supported for shape parity with
axios, but every event is a synthetic 0%/100% start/end pair
(`event.synthetic === true`) -- the transport buffers whole request/response
bodies rather than streaming them, so true incremental progress isn't
available yet.

## Development and production trust

The checked-in bundle is intended for loopback development. Development may use
ephemeral server transport and response-signing keys, so restarting the server
invalidates browser sessions and changes the runtime pins.

A non-loopback production client fails closed unless all five public trust
values are embedded into `securefetch.wasm` at build time:

- `WASM_TRUSTED_ORIGIN`
- `WASM_TRUSTED_TRANSPORT_KEY`
- `WASM_TRUSTED_TRANSPORT_KEY_ID`
- `WASM_TRUSTED_RESPONSE_KEY`
- `WASM_TRUSTED_RESPONSE_KEY_ID`

Runtime bootstrap values may repeat this bundle but cannot replace or downgrade
it. No private key belongs in WASM, JavaScript, HTML, or the asset manifest.

## Generate server keys and public pins

From the application root:

```sh
make keys
```

The command prints:

- `APP_SECURE_TRANSPORT_KEY`: private X25519 server transport key.
- `WASM_TRUSTED_TRANSPORT_KEY`: matching public X25519 pin.
- `APP_RESPONSE_SIGNING_PRIVATE_KEY`: private Ed25519 response-signing seed.
- `WASM_TRUSTED_RESPONSE_KEY`: matching public Ed25519 pin.

Treat the `APP_*` values as secrets. Store them in a secret manager and expose
them to the server using `APP_SECURE_TRANSPORT_KEY_FILE` and
`APP_RESPONSE_SIGNING_PRIVATE_KEY_FILE`. The `WASM_TRUSTED_*` key values are
public pins but must still be distributed with integrity.

## Build a trusted production bundle

Requires [TinyGo](https://tinygo.org) and Binaryen's `wasm-opt` (`brew install
tinygo binaryen`). TinyGo pins a maximum supported Go version below this
project's go.mod floor (0.41.x supports Go 1.19-1.26; go.mod requires >=
1.26.5), so `go` on PATH usually needs to be a matching 1.26.x for this build
specifically. If `make wasm` reports "requires go version 1.19 through 1.26",
install one side by side and put it first on PATH:

```sh
go install golang.org/dl/go1.26.5@latest && go1.26.5 download
PATH="$(go1.26.5 env GOROOT)/bin:$PATH"
```

Use the external browser origin and key IDs that exactly match the server:

```sh
export WASM_TRUSTED_ORIGIN='https://app.example.com'
export WASM_TRUSTED_TRANSPORT_KEY='<public X25519 value>'
export WASM_TRUSTED_TRANSPORT_KEY_ID='app-transport-v1'
export WASM_TRUSTED_RESPONSE_KEY='<public Ed25519 value>'
export WASM_TRUSTED_RESPONSE_KEY_ID='app-response-v1'
make wasm
```

The build:

1. Compiles `github.com/oarkflow/fh/wasm/cmd/securefetch` with
   `tinygo build -target wasm -no-debug` -- roughly 90% smaller than an
   equivalent `go build` output for this binary (~600KB instead of ~5.4MB),
   because it never links the parts of the Go runtime this client doesn't use.
2. Injects the five public trust values through Go linker variables.
3. Copies the patched `wasm_exec.js` checked in next to `cmd/securefetch` in
   the `github.com/oarkflow/fh` module -- TinyGo's own bundled runtime glue
   does not work as-is for this binary; see that file's header comment.
4. Regenerates `asset-manifest.json` with SHA-256/SRI metadata.

Use the exact `wasm_exec.js` this build copies for `securefetch.wasm`. Neither
TinyGo's stock runtime glue nor a plain Go `wasm_exec.js` will instantiate
this binary.

## Browser/server flow

1. The browser logs in through same-origin `POST /auth/login`.
2. `POST /secure-config.json` returns public pins, SRI values, and a short-lived
   one-use registration grant bound to the authenticated web session.
3. WebCrypto creates a non-extractable Ed25519 device key in IndexedDB.
4. The device registers through `/__fh/secure/v1/device/register`.
5. Client and server establish an ephemeral X25519 session and derive independent
   AES-256-GCM request/response keys with HKDF-SHA-256.
6. Calls to `/api/*` bind the method, exact target, session, sequence, request
   ID, timestamps, expiry, and nonce as authenticated data.
7. The server encrypts the logical response and signs the outer ciphertext using
   RFC 9421/9530 metadata.
8. The client verifies the signature before decrypting and exposing the response.

Replays, expired envelopes, invalid sequences, cross-site registration,
unprotected API calls, session/principal mismatches, and unexpected key IDs are
rejected.

## Serving requirements

- Use HTTPS/TLS 1.3 outside loopback development.
- Serve every file from the trusted application origin unless CORS and CSP are
  deliberately configured for a separate immutable asset origin.
- Serve `.wasm` as `application/wasm`.
- Keep CSP restricted to `'self'`; the WASM runtime requires `'wasm-unsafe-eval'`.
- Do not enable redirects for encrypted Fetch requests because redirects break
  exact target binding and can forward credentials unexpectedly.
- Publish the final artifact hash through a separately authenticated release
  channel, such as signed release metadata or a managed native application.
- Purge/CDN-version assets atomically so the WASM binary, runtime glue,
  JavaScript facade, server keys, key IDs, and manifest always describe one
  release.

## Key rotation

Use a new explicit transport/signing key ID for every rotation. Build a new WASM
artifact containing the new public pins, deploy it with the matching server
keys, and keep the previous application version available only for the planned
overlap window. The current transport has one active server key, so parallel
application versions are the safe overlap mechanism.

Revoking a device must also remove its active secure sessions. Multi-instance
deployments require a shared durable device registry and shared atomic TTL stores
for sessions, replay records, and signature nonces.

## Browser security boundary

WASM is tamper-resistant, not tamperproof. A compromised same-origin page,
extension, browser, endpoint, operating system, server, or build pipeline can
observe plaintext before encryption or after decryption, or invoke legitimate
device-key operations. Continue enforcing authentication, authorization,
validation, rate limits, audit policy, and transaction rules on the server.

Never embed:

- X25519 or Ed25519 private keys.
- Session/cookie secrets.
- Passwords or bearer tokens.
- Database credentials.
- KMS/HSM credentials.
- Client-reported authorization or transaction state.

## Troubleshooting

| Error | Likely cause |
|---|---|
| `FH_SECURE_REQUIRED` | The API was called with normal Fetch/curl instead of the secure client |
| `DEVICE_REGISTRATION_FORBIDDEN` | Missing, expired, replayed, or session-mismatched registration grant |
| `SESSION_BINDING_FAILED` | Web login principal no longer matches the secure session principal |
| `HTTP_SIGNATURE_REQUIRED` | Client did not negotiate the configured response-signing key |
| Embedded trust/origin mismatch | WASM build values do not match `APP_ORIGIN`, public keys, or key IDs |
| SRI/integrity failure | Asset bytes and `asset-manifest.json` came from different builds |
| Runtime initialization failure (e.g. "function import requires a callable") | `wasm_exec.js` is not the patched copy checked in next to `cmd/securefetch` -- TinyGo's stock runtime glue and a plain Go `wasm_exec.js` both fail to instantiate this binary |
| Sessions fail after restart | Development ephemeral keys changed; configure persistent keys |
| Production startup fails | HTTPS origin or required secret/key configuration is missing |

For the server configuration, route inventory, operational endpoints, scaling
requirements, and deployment checklist, see the application-level
[`README.md`](../../README.md).
