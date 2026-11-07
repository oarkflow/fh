# Production app WASM client

The WASM client contains protocol code only. It contains no server public key,
server private key, response signing key, or application secret. The browser
host supplies public trust pins at startup from the authenticated
`/secure-config.json` endpoint.

For the full browser-to-server flow, trust boundaries, streaming behavior, and
security limitations, see [`wasm/WASM.md`](../../../../wasm/WASM.md) and the
[secure WASM deployment guide](../../../../docs/secure-wasm-transport.md).

## Runtime configuration

After a user logs in, the server returns a one-use registration grant, the
X25519 transport public key and key ID, the Ed25519 response verification
public key and key ID, and WASM/runtime SRI values. The frontend passes that
configuration to `createSecureFetch`. The server pins are public data. Private
transport and signing keys stay in the server's secret manager.

The runtime pin authenticates the FH handshake only when its source is trusted.
This example serves the page and config from the same HTTPS origin, so browser
Web PKI authenticates their delivery. If the application and API have separate
trust domains, deliver the pin from a separately authenticated config source.
A pin fetched from an untrusted API endpoint cannot independently authenticate
that endpoint.

The browser verifies the WASM and runtime files against SRI values from the
config before executing them. SRI prevents asset substitution when the page
and its SRI values are trusted. A compromised page can replace the SRI value,
asset, and config together; protect the page with HTTPS, a strict CSP, and a
trusted build/deployment pipeline.

## Build

From the production-app directory, run:

```sh
make wasm
```

The build compiles the module without trust-key linker flags, copies the
matching patched TinyGo runtime, and regenerates the asset manifest. TinyGo
0.41.x requires a Go 1.19–1.26 toolchain on `PATH`; see the parent application
README for setup. Serve `.wasm` as `application/wasm`.

The deployment flow is:

1. `make keys` generates private server keys and corresponding public values.
2. Store only the private values in the server secret manager.
3. Build and publish the WASM/JS artifacts and their SRI manifest. No server
   public key is passed to TinyGo or written into the WASM file.
4. After login, `/secure-config.json` returns runtime public pins and the
   matching asset SRI values over HTTPS.
5. The browser initializes the client and reports `trustMode: "runtime-pinned"`.

`make keys` prints public and private material together. Handle its output as
sensitive because it includes private keys, and retain only the public values
for the authenticated runtime configuration.
