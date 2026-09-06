# {{APP_NAME}}

Secure, runnable `fh` application with JSON APIs, SPL-rendered pages, session and bearer authentication, CSRF protection, RBAC, SQLite persistence, health checks, OpenAPI, structured logging, and secure-fetch WASM assets.

## Run locally

The bootstrap password is deliberately absent from source control. Supply one only for this local example:

```sh
APP_BOOTSTRAP_PASSWORD='change-this-local-password' go run ./cmd/api
```

SQLite is enabled by default and creates the ignored `app.db`. Set `APP_DATABASE_ENABLED=false` to use the concurrency-safe in-memory repository.

Useful routes:

- `GET /healthz` and `GET /readyz`
- `GET /v1/example` and typed `POST /v1/example`
- `GET /v1/auth/csrf`, `POST /v1/auth/login`, `GET /v1/auth/me`, and `POST /v1/auth/logout`
- `GET /v1/protected`, guarded by the `developer` RBAC role
- `GET /`, `GET /form`, and CSRF-protected `POST /form`
- `GET /openapi.json`, plus versioned WASM assets under `/wasm/`

## Authentication example

Login and logout are CSRF protected because they create or mutate a browser session. First obtain the double-submit token and cookies, then log in:

```sh
curl -c cookies.txt http://localhost:8080/v1/auth/csrf
curl -b cookies.txt -c cookies.txt \
  -H 'Content-Type: application/json' \
  -H 'X-CSRF-Token: <token-from-previous-response>' \
  -d '{"username":"admin","password":"change-this-local-password"}' \
  http://localhost:8080/v1/auth/login
curl -b cookies.txt http://localhost:8080/v1/auth/me
curl -b cookies.txt http://localhost:8080/v1/protected
```

The login response also contains an `authz.TokenPair`; its access token works as `Authorization: Bearer <access_token>`. Passwords are verified through `authz` Argon2id, login regenerates the session ID, session data remains server-side, and `authz.Engine` denies permissions by default. Replace `Credentials`, `BearerAuthenticator`, or `SessionAuthenticator` with your identity-provider adapter without changing handlers.

## Configuration and production

`config.bcl` uses BCL environment interpolation. Explicit `APP_*` environment values remain the deployment source of truth, and secrets can be read from mounted files through `*_FILE` variables listed in `.env.example`.

Production startup fails unless all of the following are true:

- `APP_AUTH_CONFIGURED=true` after a real authentication adapter is wired
- `APP_AUTH_JWT_SECRET_FILE` points to at least 32 bytes
- `APP_SESSION_SECRET_FILE` points to at least 32 bytes
- development authentication is disabled and allowed hosts are explicit

Cookies are `Secure`, `HttpOnly`, and `SameSite=Lax` in production. CORS is not installed by default. Request IDs, host checks, body/header limits, timeouts, rate limits, security headers, panic recovery, dependency readiness, graceful shutdown, and RFC-style API problems are installed in `internal/app`.

Persistence is behind the repository interface. The included `squealx` SQLite adapter runs an idempotent migration and implements health, insert, and list operations; the in-memory adapter is suitable for tests and local ephemeral runs. Add PostgreSQL/MySQL adapters beside it and keep transaction boundaries in the service layer.

HTML uses `github.com/oarkflow/template` with the SPL engine and `${Name}` expressions—not Go `html/template` syntax. Structured application logs use `github.com/oarkflow/zlog` through the `fh-contrib` adapter.

## Secure WASM client

The template includes the secure-fetch TypeScript client, IndexedDB device-key storage, Go WASM bridge, integrity manifest tooling, and `/wasm` static handling. Build it with `make wasm`. Server-side `mw/securetransport` and `mw/httpsignature` remain disabled until persistent encryption/signing keys are configured; enabling encrypted routes without durable trusted keys would be unsafe.

## Verify

```sh
make test
make vet
make race
make build
```

`internal/app/app_test.go` exercises health/readiness, anonymous denial, CSRF login, session identity, RBAC through session and bearer token, real SQLite CRUD, browser-form CSRF submission, logout, and post-logout denial as one stateful flow.
