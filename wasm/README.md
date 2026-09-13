# @oarkflow/fh-secure-fetch

TinyGo/WASM Fetch-compatible client for `github.com/oarkflow/fh/mw/securetransport`.

Build from the repository root (requires [TinyGo](https://tinygo.org) and
[Binaryen](https://github.com/WebAssembly/binaryen)'s `wasm-opt`, e.g. `brew
install tinygo binaryen`):

```bash
make wasm
```

TinyGo's `-no-debug -opt=z` build (plus its own internal `wasm-opt` pass) is
roughly 90% smaller than the equivalent stock `go build` output for this
binary -- ~600KB instead of ~5.4MB -- because it doesn't link the parts of
the Go runtime this client never uses. `cmd/securefetch/wasm_exec.js` is a
patched copy of TinyGo's own runtime glue; see that file's header comment
before regenerating it from a newer TinyGo release, and never substitute
TinyGo's stock `targets/wasm_exec.js` or a plain Go `wasm_exec.js` for it --
neither will instantiate this specific binary.

TinyGo also pins a maximum supported Go version below what this module's
go.mod requires (0.41.x supports Go 1.19-1.26; go.mod requires >= 1.26.5), so
`go` on PATH usually needs to be a matching 1.26.x for this build specifically
-- not whatever newer toolchain `go build`/`go test` use elsewhere in this
repo. If `tinygo build` reports "requires go version 1.19 through 1.26",
install one side by side and put it first on PATH:

```bash
go install golang.org/dl/go1.26.5@latest && go1.26.5 download
PATH="$(go1.26.5 env GOROOT)/bin:$PATH" make wasm
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

`make wasm` emits `dist/asset-manifest.json` with SHA-256 SRI values for the WASM binary and runtime.

See `docs/secure-wasm-transport.md` for the protocol, server setup, threat model, and production checklist.
