# Secure WASM Example

This example serves a browser page that uses the FH session middleware for login, then uses the FH secure WASM client to call protected `/api/*` routes. It demonstrates session-cookie authentication, session-authorized device registration, session creation, pinned server keys, encrypted request and response bodies, encrypted application headers, and session revocation.

## Prerequisites

- Go with WASM support.
- TypeScript available as `tsc`, or install the `wasm/` package dependencies.
- A generated FH secure transport server key.
- A random FH session cookie secret.

Build the WASM client and asset manifest:

```sh
make wasm
```

Generate a server key:

```sh
go run ./examples/secure_wasm -generate-key
```

Generate a cookie secret:

```sh
openssl rand -base64 32 | tr '+/' '-_' | tr -d '='
```

Run the example:

```sh
export FH_SECURE_SERVER_KEY="<server-key>"
export FH_SESSION_SECRET="<cookie-secret>"
go run ./examples/secure_wasm
```

Open either:

- `http://localhost:8080`
- `http://127.0.0.1:8080`

The example allows both loopback origins by default. To set explicit origins:

```sh
export FH_APP_ORIGIN="https://app.example.com,https://admin.example.com"
```

## Files

- `main.go`: FH server, session middleware, secure transport configuration, session-authorized device registration, static assets, and demo API routes.
- `public/index.html`: login and secure API UI.
- `public/app.js`: logs in with the normal session cookie, loads `/secure-config.json`, initializes `createSecureFetch`, and calls protected APIs.
- `public/style.css`: page styling.
- `wasm/dist/`: generated WASM runtime, JS wrapper, and integrity manifest created by `make wasm`.

## How The Flow Works

1. `GET /` serves the login UI.
2. `POST /auth/login` authenticates `demo` / `demo`, stores the user in `mw/session`, and regenerates the session ID.
3. The browser loads `/secure-config.json` with the same session cookie. Unauthenticated clients get `401`.
4. The WASM client creates or loads a browser-held device key.
5. `POST /__fh/secure/v1/device/register` registers the device only if the normal FH session is logged in. The secure transport principal is the session user ID.
6. `POST /__fh/secure/v1/session` creates an encrypted session after the device signs the client hello.
7. `secure.fetch("/api/profile")` and `secure.fetch("/api/echo")` send encrypted application requests with `credentials: "same-origin"`, so the same FH session cookie is present.
8. `ValidateSession` and the API handlers verify that the secure transport principal matches the logged-in session user.
9. Responses are decrypted back into normal `Response` objects in the browser.

## Plain Object Bodies

The demo intentionally sends a plain object:

```js
await secure.fetch("/api/echo", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: { message: "hello from Go WASM", at: new Date().toISOString() },
});
```

The secure fetch wrapper converts non-`BodyInit` values to JSON and sets `content-type: application/json` if the caller did not set one. Native body types such as `FormData`, `Blob`, `ArrayBuffer`, typed arrays, strings, `URLSearchParams`, and streams pass through as normal Fetch bodies. The secure transport encrypts bytes, so the backend can receive JSON, text, files, or binary data.

The `/api/echo` handler shows this by returning:

- `content_type`
- byte length
- parsed `json` when the body is valid JSON
- `text` when the body is UTF-8
- `base64` when the body is binary

## Troubleshooting 401 On Session Creation

If the browser reports:

```text
POST /__fh/secure/v1/session
401 Unauthorized (from service worker)
```

check these items:

- The page origin must match the `baseURL` returned by `/secure-config.json`.
- `FH_APP_ORIGIN`, when set, must include the exact scheme, host, and port used in the browser.
- Log in first. `/secure-config.json`, device registration, and protected APIs require the normal FH session cookie.
- If the browser has a stored device that the server no longer knows about, the WASM client clears it, re-registers with the current logged-in session, and retries session creation once. This commonly happens in development because the example uses in-memory stores.
- Clear the stored device if you changed the server key or revocation state and the automatic retry cannot use the current login session.
- Rebuild with `make wasm` after changing files under `wasm/src` or `wasm/cmd/securefetch`.

This example also supports same-origin service-worker paths that omit the `Origin` header on control POSTs. The middleware accepts that case only when `Sec-Fetch-Site: same-origin` is present and the request `Host` matches an allowed origin.

## Security Notes

- Keep `FH_SECURE_SERVER_KEY` stable and secret.
- Keep `FH_SESSION_SECRET` stable, random, and at least 32 bytes.
- Serve production deployments over HTTPS.
- Keep `requireAssetIntegrity: true` and serve `wasm/dist/asset-manifest.json` values from a trusted bootstrap endpoint.
- Replace the demo `demo` / `demo` login with your real authentication flow.
- Replace in-memory stores with production stores before running more than one process.

## Verification

```sh
go test ./pkg/securetransport ./mw/securetransport
make wasm
```
