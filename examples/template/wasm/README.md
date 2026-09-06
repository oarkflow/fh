# @oarkflow/fh-secure-fetch

Go-WASM Fetch-compatible client for `github.com/oarkflow/fh/mw/securetransport`.

Build from the repository root:

```bash
make wasm
```

Import the generated ES module:

```js
import { createSecureFetch } from "/wasm/index.js";

const secure = await createSecureFetch({
  baseURL: location.origin,
  pinnedServerKey: "<base64url-server-public-key>",
  pinnedServerKeyID: "api-transport-2026-01",
  responseSigningPublicKey: "<base64url-ed25519-public-key>",
  responseSigningKeyID: "api-response-2026-01",
  requireResponseSignature: true,
  requireEmbeddedTrust: true,
  wasmURL: "/wasm/securefetch.wasm",
  wasmExecURL: "/wasm/wasm_exec.js",
  wasmIntegrity: "sha256-<from-asset-manifest>",
  wasmExecIntegrity: "sha256-<from-asset-manifest>",
  requireAssetIntegrity: true,
});

const response = await secure.fetch("/api/profile");
console.log(await response.json());
```

The package requires WebAssembly, WebCrypto, IndexedDB, a browser Window, and a secure context. Loopback HTTP is accepted for local development. Every non-loopback execution requires a complete origin/X25519/Ed25519 trust bundle embedded by the Makefile; runtime configuration cannot substitute those pins. When `requireResponseSignature` is enabled it generates signature nonces inside WASM, verifies signed ciphertext before decryption, and fails closed on downgrade or tampering. It intentionally rejects redirects, `no-cors`/navigation requests, non-canonical base URLs and prefixes, and destinations outside `baseURL`.

`make wasm` emits `dist/asset-manifest.json` with SHA-256 SRI values for the Go WASM binary and runtime.

See `docs/secure-wasm-transport.md` for the protocol, server setup, threat model, and production checklist.
