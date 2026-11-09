# github.com/oarkflow/fh/examples/production-app

A production-oriented FH server starter with authenticated sessions, RBAC,
bounded request handling, operational endpoints, SQLite persistence, the FH
secure WASM transport, and a structured, componentized browser frontend
("FH Control Center", built with [`@oarkflow/lithe`](https://www.npmjs.com/package/@oarkflow/lithe) -
see `web/frontend/README.md`). The application is runnable on loopback for
development and fails closed when configured for production without its
required secrets.

## Quick start

`fh-init` creates `.env` from `.env.example` with mode `0600`. A later
`fh-init -force` refreshes generated files but never overwrites `.env`.

```sh
go mod tidy
make frontend   # builds web/public/app.js + app.css from web/frontend/src (needs Node 22.6+)
./run.sh
```

`web/public/` is build output, not checked in - the server has nothing to
serve at `/assets/*` until `make frontend` has run at least once.

Open `http://localhost:8080`. The development credentials are in `.env`; change
them before using the example beyond local testing.

Prefer not to do that by hand the first time? Regenerate with `fh-init
-module github.com/oarkflow/fh/examples/production-app -dir . -force -verify`: it re-scaffolds, builds the
server, boots it on a spare loopback port, walks through the exact HTTP
surface the browser depends on (assets, the WASM transport bundle, and the
full login/secure-bootstrap/logout lifecycle), and opens it in your browser
for a live look - see [`-verify`](#fh-init--verify) below.

The server loads `.env` itself on startup (via `github.com/oarkflow/config`),
so `go run ./cmd/server` works directly — `./run.sh` is a thin convenience
wrapper, not a requirement. Real environment variables always take priority
over `.env`, so orchestrated deployments that inject secrets directly (Docker,
systemd, Kubernetes) behave the same whether or not a `.env` file is present.

## First-run setup

### Development defaults

A newly generated project is immediately runnable with these loopback-only
values:

| Setting | Default |
|---|---|
| Application URL | `http://localhost:8080` (bound to loopback) |
| Username | `admin` |
| Password | `change-me-in-development` |
| Operations bearer token | `change-me-ops-token` |
| Database | `data/app.db` |
| Transport key | Ephemeral; regenerated whenever the server starts |
| Response-signing key | Ephemeral; regenerated whenever the server starts |

These values are public development conveniences, not production secrets.
The server binds to `127.0.0.1` by default. To share it on a trusted LAN,
set `APP_ADDR` to the intended interface and set `APP_ORIGIN` to an explicit
development origin; avoid wildcard interfaces on untrusted networks.

### Start and sign in

1. Run `go mod tidy` once.
2. Run `make frontend` once (needs Node 22.6+) to build `web/public/app.js`
   and `app.css` - they are build output, not checked in, so the server has
   nothing to serve at `/assets/*` until this has run at least once.
3. Review `.env`, especially `APP_ORIGIN`, the login credentials, and the
   operations token.
4. Start the server with `./run.sh`, `make run`, or `go run ./cmd/server`.
5. Open `http://localhost:8080`.
6. Enter `admin` and `change-me-in-development`, then select **Sign in**.
7. After secure registration completes, select **Load profile** on the
   **Overview** panel to call `GET /api/me` through encrypted Fetch, use
   **Secure API** to send an encrypted `POST /api/echo`, or check
   **Activity** for a log of what the client has done.

The sign-in form is replaced by the dashboard once authenticated, and
**Sign out** moves to the top bar. Reloading the page checks
`GET /auth/session`, restores an existing web login, and creates a fresh
secure transport session. Signing out revokes the active secure session
first, destroys the web session cookie, and returns to the sign-in form.

The warning about ephemeral cryptographic keys is expected in development.
Restarting the server changes those keys and invalidates existing secure
transport sessions.

### Device registration

There is no separate manual registration command for the browser. The
frontend's secure API client (`web/frontend/src/lib/secure-client.ts`,
compiled into `web/public/app.js` - see `web/frontend/README.md`) performs
the complete sequence after login:

1. `POST /auth/login` validates the development credentials, regenerates the
   web session ID, and sets the signed HttpOnly cookie.
2. `POST /secure-config.json` issues a random 90-second, one-use registration
   grant bound to that authenticated principal and exact web session.
3. `createSecureFetch(...)` verifies asset integrity, loads the WASM runtime,
   and asks WebCrypto to create a non-extractable Ed25519 device key in
   IndexedDB.
4. The secure client submits its signed registration to
   `POST /__fh/secure/v1/device/register` with the one-use grant.
5. It establishes an ephemeral encrypted session through
   `POST /__fh/secure/v1/session`.
6. Subsequent `secure.fetch('/api/...')` requests are encrypted, replay
   protected, bound to the login principal, and require signed responses.

A failed grant binding consumes the grant, and a grant cannot be replayed.
Reload the page and sign in again to obtain a fresh one. Do not call the device
registration endpoint with plain curl: a valid request includes browser-held
device keys and protocol signatures.

To reset a development browser completely, stop the server, clear site data for
`localhost:8080` (including IndexedDB and cookies), restart it, and sign in
again.

### `fh-init -verify`

`fh-init` can do the checks in this section for you, against a real,
temporarily-running instance of the generated app:

```sh
fh-init -module github.com/oarkflow/fh/examples/production-app -dir . -force -verify
```

It builds the frontend (`npm ci && npm run build`, producing
`web/public/app.js` + `app.css`) and the server, boots the server on a free
loopback port using the credentials in `.env`, works through the same
checklist below (plus the static assets and WASM transport bundle every page
load needs), and - unless `-no-browser` is also given - opens it in your
default browser for a live look before shutting the instance back down. A
missing network connection, Node.js, or Go toolchain only prints a warning
and leaves the exit code at `0` (the scaffold itself is still complete);
pass `-require-verify` to make that fatal instead. See `fh-init -h` for every
flag.

### Basic verification

Public liveness:

```sh
curl -i http://localhost:8080/healthz
```

Protected readiness and metrics:

```sh
set -a
. ./.env
set +a
curl -H "Authorization: Bearer $APP_OPERATIONS_TOKEN" http://localhost:8080/_ops/ready
curl -H "Authorization: Bearer $APP_OPERATIONS_TOKEN" 'http://localhost:8080/_ops/metrics?format=prometheus'
```

Expected security checks:

- `GET /healthz` returns `200`.
- `GET /_ops/metrics` without the bearer token returns `401`.
- A normal request to `/api/me` returns `426 FH_SECURE_REQUIRED`.
- The browser's encrypted request to `/api/me` succeeds only after login,
  device registration, and secure-session establishment.
- Login, logout, and secure bootstrap reject cross-site requests.


Useful commands:

```sh
make run       # start the server (equivalent to `go run ./cmd/server`)
make test      # run application tests
make check     # run all application checks
make keys      # generate transport/signing private keys and matching public pins
make wasm      # create a production-pinned WASM artifact (requires trust variables)
make frontend  # build web/public/app.js + app.css from web/frontend/src (required before first run; needs Node 22.6+)
```

## Project structure

```text
.
├── cmd/server/main.go             server assembly, middleware, keys, lifecycle
├── internal/auth/                 login, principal extraction, RBAC bridge, grants
├── internal/config/                validated environment and secret-file loading
├── internal/database/              squealx/SQLite store, migration, health check
├── internal/httpapi/               web, auth, bootstrap, health, and API routes
├── web/templates/                  secure SPL server-rendered templates
├── web/public/                     compiled browser JS/CSS (build output - run `make frontend`; not checked in)
├── web/frontend/                   FH Control Center: the @oarkflow/lithe frontend source
│   └── src/{lib,state,components,features}   secure client, state, reusable UI, panels
├── web/wasm/                       secure Fetch runtime and integrity manifest
├── policy.authz                    file-backed authorization policy
├── .env.example                    documented development configuration
├── Makefile                        test, key, run, frontend, and WASM build targets
└── run.sh                          optional convenience wrapper for `go run ./cmd/server`
```

## Feature and source guide

### What the starter provides

- **Browser control center:** sign-in, session restore, profile loading, secure
  API console, activity log, and session status. The API includes note CRUD
  endpoints for callers built on the secure client.
- **Authentication:** one configured starter account, rotating server-side
  session IDs at login, HttpOnly/SameSite cookies, logout, and session expiry.
- **Authorization:** route-level RBAC from `policy.authz` plus per-note owner
  checks, so users can only change their own records unless policy grants an
  administrative role.
- **Encrypted API transport:** `/api/*` requires the WASM client, registered
  device, authenticated session, replay protection, and a signed response.
- **Abuse detection:** TCPGuard evaluates requests, failed logins, and note
  creation velocity against `security/policy/tcpguard.bcl`.
- **Operations:** public liveness and bearer-protected readiness, health,
  runtime, route, and metrics endpoints.
- **Persistence:** SQLite-backed users and notes with connection pool limits,
  foreign keys, and database readiness checks.
- **Deployment controls:** strict origin/host checks, CSP and browser security
  headers, rate/body/time limits, TLS early-data rejection, audit logging,
  graceful shutdown, production secret requirements, and pinned WASM trust.

The starter login is a demonstration credential flow. Replace it with your
identity provider, add application-specific validation and authorization, and
review deployment settings before handling real user data.

### Change guide: where should I make a change?

Use this section as the task-oriented entry point. Most changes cross more than
one layer: route behavior belongs in `internal/httpapi/routes.go.tmpl`, policy
belongs in `policy.authz.tmpl` or `security/policy/tcpguard.bcl.tmpl`, persisted
state belongs in `internal/database/database.go.tmpl`, and browser behavior
belongs in the separately versioned `web/frontend` project. Update all layers
that enforce the same rule; hiding a button in the browser is never access
control.

#### 1. How do I change authentication or add an identity provider?

For the demonstration username/password flow, start at
`internal/httpapi/routes.go.tmpl` (`POST /auth/login`) and
`internal/auth/auth.go.tmpl` (`DecodeLogin`, `Login`, `Principal`). `DecodeLogin`
owns the request shape; `Login` compares the configured demo credential,
loads the account's role, stores the identity/role in the session, and rotates
the session ID. `Principal` reconstructs the FH principal from that session.
`internal/config/config.go.tmpl` loads the demo credentials and session secret.

For OIDC, WebAuthn, or another real identity provider, replace the credential
validation in `Login` with provider callback/token validation, but keep the
same downstream contract: establish a server-side session only after provider
validation, rotate its ID, store a stable internal subject ID and roles, and
derive `fh.Principal` server-side. Add CSRF/state/nonce and callback-origin
checks appropriate to the provider. Do not accept roles, user IDs, or an
“authenticated” flag supplied by the browser. Update the frontend auth calls
and add tests for valid, invalid, expired, replayed, and cross-site flows.

For login/session/logout behavior, edit the matching handlers in
`Register`: `/auth/session` reports current server session state, `/auth/login`
validates same origin and credentials, `/auth/logout` destroys the session,
and `/secure-config.json` issues the secure transport bootstrap grant. Preserve
cookie flags, no-store behavior, grant binding, and secure-session revocation
when changing logout or bootstrap.

#### 2. How do I add or change application functionality?

Add the HTTP endpoint and request contract in `Register` or a focused helper
near `registerNoteRoutes` in `internal/httpapi/routes.go.tmpl`. Decide the
method, path, accepted content type, maximum body size, validation rules,
response shape, and error codes first. Business endpoints should normally be
under `/api/*`, so secure transport, authentication, and route authorization
apply. Add database calls through a repository in `internal/database`, and
add abuse signals through `Guard.EmitBusinessEvent` when the operation can be
automated or has a meaningful rate/volume risk.

Update `policy.authz.tmpl` for the route permission, then add row-level checks
inside the handler when authorization depends on a database record's owner or
state. Update `web/frontend` API types/client/state/panel if the browser uses
the new capability. Document the route in the Routes table, and add handler,
authorization, validation, persistence, and browser integration tests.

For notes specifically, `registerNoteRoutes` is the CRUD example: it scopes
list reads by user, bounds pagination, checks owner policy after loading a row,
validates titles, and emits a creation-velocity event. Reuse its patterns, not
its note-specific schema or policy names.

#### 3. How do I change the database, schema, or persistence engine?

Edit `internal/database/database.go.tmpl`: add or change model structs and
repositories, then update `migrate` and indexes in the same change. `Open` and
`OpenWithPool` own connection setup; `ensureSQLiteDirectory` handles local
SQLite file paths; `Ping` feeds readiness. Add a versioned migration system
before evolving deployed schemas: the starter's `CREATE TABLE IF NOT EXISTS`
statements only create the initial tables and do not migrate existing columns.

When adding a table, define primary/foreign keys, nullability, uniqueness,
indexes, ownership boundaries, and deletion behavior. Use parameterized
repository operations, pass request contexts through queries, bound connection
pools, and avoid automatic retries for non-idempotent writes. Update
`APP_DATABASE_URL` handling/documentation and the deployment backup/restore
instructions if the backend changes. Add migration, transaction, constraint,
concurrency, and readiness tests. A database schema change often also needs
updates to authz resource attributes and API response types.

#### 4. How do I add monitoring, metrics, logs, tracing, or health checks?

The middleware stack is assembled in `cmd/server/main.go.tmpl`. Request IDs,
structured access logs, metrics, and audit logging are installed there. The
operations routes are registered there too: `/_ops/*` routes are protected by
`operationAuth`, while `/_ops/metrics` uses the FH metrics handler. Add a
dependency/readiness probe with `app.AddHealthCheck` after the dependency is
initialized. Keep liveness cheap and independent from downstream dependencies;
readiness should report whether the instance can safely receive traffic.

Configure output in the relevant adapter/config rather than logging secrets
from handlers. Use request IDs for correlation, emit actionable errors without
credentials or bodies, and keep metrics labels low-cardinality (never label by
raw user ID, URL query, or token). Protect operational routes at both the
bearer-token and network layers. If exporting traces or metrics to a vendor,
add explicit configuration, shutdown/flush behavior, and tests for exporter
failure so observability outages do not take down business requests.

#### 5. How do I change roles, permissions, or resource ownership?

Route-level permissions live in `policy.authz.tmpl`. `PolicyMiddleware` in
`internal/auth/auth.go.tmpl` adapts each protected `/api` request into an authz
subject/action/resource decision. `internal/httpapi/routes.go.tmpl` attaches
that middleware to the `/api` group. Add a policy rule for each new action and
resource; default to deny and verify that unrelated roles remain denied.

Object ownership is checked after loading the record with `AuthorizeOwned`.
Use this for update/delete/read-by-ID where a caller may only access their own
row. Do not trust an owner ID from request JSON; compare against the persisted
owner. Role assignment comes from `Store.EnsureUser` and the role stored in the
server session. For real multi-user provisioning, define who can grant roles,
how changes invalidate active sessions, and how administrator actions are
audited. Add positive and negative tests for every role/action/ownership
combination; frontend navigation is only presentation.

#### 6. How do I change security headers, CORS, host/origin checks, or cookies?

The FH security/CORS/fetch-metadata/early-data/session middleware and cookie
options are in `cmd/server/main.go.tmpl`. Allowed hosts and origins are
computed in `internal/config/config.go.tmpl` by `parseOriginPolicy` and shared
with CORS, secure transport, and response signing. Login, logout, and bootstrap
also call `sameOrigin` in `internal/httpapi/routes.go.tmpl`. Change these
together when adding a hostname or deployment origin. Review CSP whenever
adding a script, style, image, API, or third-party origin; do not loosen it to
`*` or inline scripts just to make a frontend build work.

Session cookie name, Secure, HttpOnly, SameSite, Path, and lifetime are set
when the session manager is created in `main`. Production uses a `__Host-`
cookie and HTTPS origin. Preserve the production requirements in `Load`; do
not make unsafe development defaults apply outside explicit development.

#### 7. How do I change encrypted API transport or browser trust?

The secure transport is installed in `main`, and `/api/*` is selected by its
`Protect` callback. Registration and session binding callbacks require the
one-use grant, authenticated principal, and matching web session. Response
signing is installed after secure transport so the outer ciphertext is
signed. `/secure-config.json` in `Register` returns public pins and integrity
values; `loadManifest` checks the checked-in WASM manifest at startup.

Change the protocol only together with the matching FH WASM client/runtime,
its API docs, generated artifact, manifest hashes, key IDs, rotation plan, and
browser integration tests. Never put a private key in the frontend. Production
must use persistent server private keys and a WASM artifact built with matching
embedded public trust anchors. TLS remains required.

#### 8. How do I tune abuse prevention or add a detection rule?

Detection policy is in `security/policy/tcpguard.bcl.tmpl`; engine setup and
event adapters are in `internal/security/guard.go.tmpl`. The global middleware
uses `skipStaticAndOps` to omit static/framework paths. Failed and successful
login events are emitted by the login route, while business actions call
`EmitBusinessEvent`. Add facts/rules in the policy file and emit the matching
event at the point where the server has trusted context. Decide whether a
decision blocks the action, how clients see the response, and which structured
event fields operators need. Keep anomaly detection separate from
authorization: a low-risk score must never grant permission.

#### 9. How do I change environment variables, secrets, or deployment behavior?

Add fields/defaults/validation to `Config` and `Load` in
`internal/config/config.go.tmpl`, add the documented value to
`.env.example.tmpl`, and update the Configuration table here. Secret values
should use the framework secret provider's `_FILE` option where supported;
the loader gives process environment values precedence over `.env`. Keep
production validation fail-closed and test malformed, missing, and boundary
values. `loadDotEnv` is only a local convenience and must not override
orchestrator-injected environment variables.

For deployment changes, update the Production deployment checklist and
Multi-instance deployments sections too. The in-memory session, rate limit,
secure session, replay, nonce, and grant stores are single-process; horizontal
scaling requires shared atomic stores and a durable device registry.

#### 10. How do I change the browser interface?

The frontend is a separate `lithe-boilerplate` checkout cloned by `fh-init`
into generated projects; it is not authored by these `.tmpl` files. Follow
`web/frontend/README.md` for component, state, API-client, and function details.
Protected calls must go through `src/lib/secure-client.ts`; do not use plain
Fetch for `/api/*`. After a frontend change, rebuild the single-file assets,
run its secure-fetch check, and use `fh-init -verify` or browser integration
tests to verify login, secure bootstrap, API behavior, and logout.

#### 11. What should I update for every change?

Update the implementation, its configuration/policy/docs, and tests together.
Run `gofmt`, `go test ./...`, `go vet ./...`, the frontend secure-fetch check,
and the runnable `fh-init -verify` workflow when the relevant toolchains are
available. Check both allowed and denied behavior. If a change modifies
generated template files, regenerate `examples/production-app`, review the
diff, and verify the generated example before publishing the template.

### Function-by-function reference

This index covers every declared function in the Go template files, including
the small policy-boundary tests. It describes what each function does and
where to extend it; the source remains authoritative for exact signatures.

#### `cmd/server/main.go.tmpl`

| Function | Behavior and when to change it |
|---|---|
| `main` | Parses the two key-generation flags, loads validated configuration, initializes logging/database/authz/TCPGuard/SPL, configures FH limits and middleware, creates sessions, installs secure transport and response signing, registers application and operations routes, then starts graceful listening. Add global middleware and dependencies here in deliberate order. |
| `transportKey` | Decodes the configured X25519 private key; permits an ephemeral key only in development and rejects its absence in production. Change only with secure transport key format/rotation changes. |
| `signingKey` | Decodes the configured Ed25519 private seed or creates an ephemeral development signing pair; returns the public/private pair and whether it is ephemeral. |
| `operationAuth` | Returns middleware that accepts only a matching non-empty bearer token using constant-time comparison; otherwise returns 401. Keep every operations route behind it. |
| `skipStaticAndOps` | Excludes home, liveness, static assets, WASM, and operations paths from TCPGuard business-request analysis. Add exclusions only for framework/static paths, not business endpoints. |
| `constantEqual` | Hashes both strings with SHA-256 and compares fixed-size digests in constant time. Used for token/principal binding comparisons. |
| `hstsAge` | Returns one-year HSTS in production and zero in development. |
| `generateTransportKey` | Generates and prints the encoded private X25519 key and its public WASM trust value. Treat the private output as a secret. |
| `generateSigningKey` | Generates and prints the encoded Ed25519 private seed and public WASM pin. Treat the private output as a secret. |

#### `internal/config/config.go.tmpl` and `internal/config/dotenv.go.tmpl`

| Function | Behavior and when to change it |
|---|---|
| `Load` | Loads `.env`, determines production mode, resolves origin/host policy, loads direct or file-backed secrets, enforces production-only key/password/token requirements, validates time/body/rate limits, and assembles `Config`. Add every new setting here and document/test it. |
| `positiveInt` | Parses a positive integer from the environment or uses its fallback; rejects invalid and non-positive values with a variable-specific error. |
| `env` | Returns a trimmed non-empty process environment value or the supplied fallback. |
| `parseOriginPolicy` | Parses `:port`, host:port, or absolute origins; enforces HTTPS outside loopback development and always in production; derives canonical origin, allowed hosts, and allowed origins. Change alongside CORS, host validation, secure transport, and frontend deployment settings. |
| `developmentLANHosts` | Collects the machine hostname and assigned unicast IPs, plus the wildcard bind address, for explicit `:port` LAN development mode. It is not used for production. |
| `uniqueStrings` | Trims, lowercases, removes empty/duplicate values, and sorts origin/host policy entries deterministically. |
| `loadDotEnv` | Loads local variables through the config env provider, leaving already-set process variables untouched and tolerating a missing file. Keep this precedence for container/orchestrator deployments. |

#### `internal/auth/auth.go.tmpl` and `internal/auth/grants.go.tmpl`

| Function | Behavior and when to change it |
|---|---|
| `Login` | Constant-time compares the configured demo username/password, upserts/loads the account role through `RoleStore`, stores identity and role in the session, then regenerates the session ID. Replace the credential check for a real identity provider while preserving session rotation and server-derived roles. |
| `Principal` | Reads the user ID and roles from the session, normalizes decoded role values, defaults missing roles to least-privileged `viewer`, constructs and sets an FH principal. |
| `Require` | Rejects requests without a principal with 401; otherwise continues middleware. |
| `PolicyMiddleware` | Builds FH authz middleware that derives subject from the principal, action from HTTP method, resource from route, and environment from request context; uses default deny/error handlers. |
| `AuthorizeOwned` | Evaluates an authz decision with the loaded resource ID and trusted database owner ID, returning 500 on engine error or 403 on denial. Call after loading a row for per-object authorization. |
| `DecodeLogin` | Parses login JSON and requires non-empty username/password; returns a typed 400 error for malformed or incomplete input. |
| `rolesFromSession` | Converts either `[]string` or JSON-restored `[]any` to non-empty role strings; returns nil for unsupported session data. |
| `secretEqual` | Compares two secrets using fixed-size SHA-256 digests and constant-time comparison. |
| `NewGrantStore` | Allocates the in-memory registration grant map and returns the store. Replace storage for multi-instance deployment with a shared atomic TTL store. |
| `Issue` | Generates a 256-bit random grant, stores only its SHA-256 hash bound to principal and session for 90 seconds, expires stale entries, and enforces a 10,000-entry cap. |
| `Consume` | Decodes a grant, hashes and clears its raw bytes, atomically removes the stored entry, then checks expiry/principal/session. Any attempt burns the token, including invalid binding attempts. |

#### `internal/httpapi/routes.go.tmpl`

| Function | Behavior and when to change it |
|---|---|
| `Register` | Validates the WASM manifest, mounts static assets, installs home/health/auth/login/logout/bootstrap handlers, creates the authenticated RBAC API group, registers `/me`, `/echo`, and note routes. Add endpoints and request contracts here or split them into focused route files. |
| `registerNoteRoutes` | Implements owner-scoped paginated list, validated create, owner/admin update, and owner/admin delete. It demonstrates repository access, route policy, ABAC ownership, bounded inputs, and TCPGuard business events. |
| `notesPageLimit` | Parses `limit`, defaults to 25, and caps at 100 to bound database work. |
| `notesPage` | Parses `page` and defaults malformed or non-positive values to page 1. |
| `newID` | Generates a random 128-bit identifier and hex-encodes it for note records. |
| `renderIndex` | Adds a response transform that requires `</body>` and inserts one fixed same-origin module script after secure SPL rendering; caller/request input cannot alter the loader. |
| `loadManifest` | Reads and decodes the WASM asset manifest; fails startup if securefetch or wasm_exec integrity values are missing. |
| `sameOrigin` | Requires an Origin header, rejects Fetch Metadata cross-site requests, requires Origin host to match Host, and matches the configured allow-list. Used by state-changing browser endpoints. |
| `requestOrigin` | Returns a normalized request Origin when present or the configured canonical origin as fallback for secure bootstrap. |

#### `internal/database/database.go.tmpl`

| Function | Behavior and when to change it |
|---|---|
| `DefaultPoolConfig` | Returns conservative pool bounds and connection lifetimes suitable for one SQLite-backed process. Tune with the production database and measured concurrency. |
| `Open` | Opens the configured DSN using the default pool settings. |
| `OpenWithPool` | Ensures a local SQLite parent directory exists, opens the SQLite driver, applies pool limits, runs initial schema creation, and returns typed user/note repositories; closes the DB on setup errors. |
| `migrate` | Creates the starter users/notes tables and owner/time index if absent. Add versioned migrations before changing a deployed schema. |
| `ensureSQLiteDirectory` | Creates parent directories for relative file-backed SQLite DSNs; leaves memory and absolute paths alone. |
| `(*Store).Close` | Closes the underlying SQL database and returns any close error. |
| `(*Store).Ping` | Performs a context-aware database ping for health/readiness checks. |
| `(*Store).EnsureUser` | Retries a transient user lookup, returns an existing row if found, otherwise inserts the initial user/role and returns it. It is starter account provisioning, not a complete user-management flow. |
| `(*Store).RoleForUser` | Retries the user lookup and returns the current stored role, wrapping lookup errors with the user ID. |

#### `internal/security/guard.go.tmpl`

| Function | Behavior and when to change it |
|---|---|
| `New` | Loads the BCL policy bundle, selects development/production response policy, creates in-memory TCPGuard state/metrics, disables unused GeoIP enrichment, and returns the adapter. |
| `(*Guard).Middleware` | Optionally skips a request, adapts FH context, evaluates request policies, adds decision headers/logs, continues allowed traffic, or writes the configured enforcement response. |
| `(*Guard).EmitAuthEvent` | Builds identity context for a login event, evaluates and logs it, and reports whether a deny/block decision should stop the login response. |
| `(*Guard).EmitBusinessEvent` | Builds business action/amount context, evaluates a `function.invoked` event, logs it, and reports whether the business operation should be blocked. |
| `(*Guard).setDecisionHeaders` | Adds decision effect and, when present, severity and trace ID headers to the response. |
| `(*Guard).log` | No-ops without a callback; otherwise forwards event name, request ID, decision effect/severity, and risk score to the configured structured logger. |
| `toHTTPRequest` | Adapts FH method, URL, body, host, remote IP, and headers into a synthetic `net/http` request expected by TCPGuard's context builder. |

#### Template test functions

| Function | Behavior |
|---|---|
| `TestGrantIsOneShotAndSessionBound` | Verifies wrong-session use burns a grant, correct-session use succeeds once, and replay fails. Extend it for expiry, capacity, and malformed token cases when grant behavior changes. |
| `TestLoopbackOriginPolicyAcceptsBothBrowserNames` | Verifies localhost, IPv4 loopback, and IPv6 loopback hosts/origins are admitted for loopback development origins. |
| `TestPortOnlyOriginPolicyEnablesLANDevelopment` | Verifies explicit `:port` mode includes development LAN hosts, canonicalizes to localhost, and is rejected in production. |

### Generated file guide

| Path | Purpose |
|---|---|
| `go.mod`, `go.sum` | Go module identity and pinned server dependencies. |
| `.env.example`, `.env` | Documented defaults and private local settings; `.env` is created with mode `0600` and ignored by Git. |
| `.gitignore` | Excludes secrets, database files, frontend build output, and local binaries. |
| `Makefile` | `run`, `test`/`check`, key generation, frontend build, secure-fetch lint, and production WASM build tasks. |
| `run.sh` | Loads no extra configuration; changes to project root and runs the server. |
| `cmd/server/main.go` | Executable entry point and middleware/lifecycle assembly. |
| `internal/auth/` | Session login, principal extraction, authorization bridges, and one-use bootstrap grants. |
| `internal/config/` | Environment, dotenv, secrets, and origin policy validation. |
| `internal/database/` | SQLite schema, bounded connection pool, and typed user/note repository. |
| `internal/httpapi/routes.go` | HTTP route handlers, browser bootstrap, API examples, and asset manifest checks. |
| `internal/security/guard.go` | TCPGuard integration for request and business-event abuse detection. |
| `policy.authz` | Declarative role and note-owner authorization policy. |
| `security/policy/tcpguard.bcl` | Declarative credential abuse, scanning, and business-velocity detection rules. |
| `templates/index.html`, `web/templates/index.html` | SPL source for the browser shell (kept in both compatibility locations). |
| `index.html` | Static frontend entrypoint used by Lithe during development/build. |
| `web/frontend/` | Separately maintained Lithe frontend cloned by `fh-init`; consult its README for component, state, and frontend-function details. The security check ensures protected requests use `src/lib/secure-client.ts`. |
| `web/wasm/secure-fetch.js` | Browser API for trust-checked WASM secure transport. |
| `web/wasm/client.js` | Optional Axios-style request client and interceptors over a supplied transport. |
| `web/wasm/index.js`, `body.js`, `errors.js`, `storage.js` | WASM package exports, body handling, typed protocol errors, and device-key storage. |
| `web/wasm/securefetch.wasm`, `wasm_exec.js` | Secure protocol implementation and its matching TinyGo runtime glue. |
| `web/wasm/asset-manifest.json` | SHA-256 integrity values checked before browser loading. |
| `web/wasm/README.md` | Browser transport API, build, key, rotation, and troubleshooting guide. |
| `internal/auth/grants_test.go`, `internal/config/origin_test.go` | Tests for one-shot grant binding and origin-policy boundaries. |
| `web/public/`, `web/frontend/node_modules/`, `data/` | Generated/local output; created as needed and not committed. |

The `.tmpl` suffix belongs to the source repository. `fh-init` removes that
suffix, substitutes `github.com/oarkflow/fh/examples/production-app`, and writes the rendered project files.

## Security stack and ordering

The server enables the following layers in deliberate order:

1. FH production protocol settings, Host allow-list, body/header/connection
   limits, handler deadlines, resource admission, panic recovery, and graceful
   shutdown.
2. Server-generated request IDs, structured request logs, RED metrics, and
   warning-or-higher security audit events.
3. TLS early-data rejection and Fetch Metadata isolation.
4. CSP, clickjacking, MIME-sniffing, referrer, permissions, COOP/CORP/COEP, and
   production-only HSTS headers.
5. Exact-origin credentialed CORS, global body limits, cooperative handler
   timeouts, per-IP rate limiting, and signed sessions.
6. FH encrypted WASM transport for `/api/*`, session/device binding, one-time
   registration grants, replay defenses, and security-event logging.
7. RFC 9421 Ed25519 signatures over the outer encrypted response.
8. Authentication, file-backed RBAC, endpoint contracts, validation, and the
   business handler.

Public HTML, assets, health, and operations endpoints do not create sessions.
Authentication state is held in an HttpOnly, SameSite=Strict cookie; production
uses the `Secure` `__Host-` cookie form. Login regenerates the session ID.

SPL stays in secure mode, which rejects script elements in rendered templates.
After SPL validates the page, the route appends one constant same-origin module
loader for `/assets/app.js`; neither template data nor request input can alter
that tag, and CSP restricts scripts to the application origin.

## Routes

| Method | Path | Access | Purpose |
|---|---|---|---|
| `GET` | `/` | Public | Render the SPL application page |
| `GET` | `/assets/*` | Public | Static browser assets |
| `GET` | `/wasm/*` | Public | WASM transport bundle and loader |
| `GET` | `/healthz` | Public | Minimal liveness response |
| `GET` | `/auth/session` | Current browser session | Report whether the browser is authenticated |
| `POST` | `/auth/login` | Same origin | Authenticate and regenerate session |
| `POST` | `/auth/logout` | Same origin | Destroy the web session |
| `POST` | `/secure-config.json` | Authenticated, same origin | Issue a one-use device grant and public pins |
| `POST` | `/__fh/secure/v1/device/register` | Authorized grant | Register the browser device |
| `POST` | `/__fh/secure/v1/session` | Registered device | Establish encrypted transport session |
| `GET` | `/api/me` | Encrypted + authenticated + RBAC | Return bound principal information |
| `POST` | `/api/echo` | Encrypted + authenticated + RBAC | Validated JSON example |
| `GET` | `/api/notes?page=&limit=` | Encrypted + authenticated + RBAC | List the caller's notes with pagination |
| `POST` | `/api/notes` | Encrypted + authenticated + RBAC | Create a note owned by the caller |
| `PUT` | `/api/notes/:id` | Encrypted + authenticated + owner policy | Update an owned note (or one allowed by admin policy) |
| `DELETE` | `/api/notes/:id` | Encrypted + authenticated + owner policy | Delete an owned note (or one allowed by admin policy) |
| `GET` | `/_ops/metrics` | Operations bearer token | JSON/Prometheus request metrics |
| `GET` | `/_ops/health`, `/_ops/live`, `/_ops/ready` | Operations bearer token | Detailed health/readiness |
| `GET` | `/_ops/runtime`, `/_ops/routes` | Operations bearer token | Runtime and route diagnostics |

All `/api/*` requests must use the secure WASM client. A normal Fetch/curl
request is rejected with `426 FH_SECURE_REQUIRED`.

## Configuration

The server loads `.env` from the working directory on startup (a missing file
is not an error). Real environment variables always take priority over `.env`,
so orchestrated environments can inject the same variables directly — with no
`.env` file present at all — and start `go run ./cmd/server` or the compiled
binary unchanged.

| Variable | Development default | Description |
|---|---|---|
| `APP_ENV` | `development` | Only this exact value enables development concessions; every other value is production |
| `APP_ADDR` | `127.0.0.1:8080` | Listen address; loopback by default |
| `APP_ORIGIN` | `http://localhost:8080` | Browser-origin policy; production requires one explicit absolute HTTPS origin |
| `APP_DATABASE_URL` | `file:data/app.db?...` | SQLite DSN |
| `APP_SESSION_SECRET` / `_FILE` | development value | Base64url value decoding to at least 32 random bytes |
| `APP_LOGIN_USER` | `admin` | Starter login identity |
| `APP_LOGIN_PASSWORD` / `_FILE` | development value | Starter password; replace with a real identity provider for production |
| `APP_OPERATIONS_TOKEN` / `_FILE` | development value | Bearer token protecting `/_ops/*` |
| `APP_SECURE_TRANSPORT_KEY` / `_FILE` | empty | Base64url X25519 server private key |
| `APP_SECURE_TRANSPORT_KEY_ID` | `app-transport-v1` | Transport key/version identifier |
| `APP_RESPONSE_SIGNING_PRIVATE_KEY` / `_FILE` | empty | Base64url Ed25519 response-signing seed |
| `APP_RESPONSE_SIGNING_KEY_ID` | `app-response-v1` | Response key/version identifier |
| `APP_REQUEST_TIMEOUT` | `15s` | Handler execution budget |
| `APP_MAX_BODY_BYTES` | `1048576` | Global request body limit |
| `APP_RATE_LIMIT` | `120` | Requests per client IP per minute |

Development origin policy is intentionally explicit:

- `APP_ORIGIN=http://localhost:8080` accepts `localhost`, `127.0.0.1`, and
  IPv6 loopback only.
- `APP_ORIGIN=:8080` is an opt-in convenience that accepts loopback, the
  machine hostname, and assigned LAN IP addresses on port 8080.
- `APP_ORIGIN=localhost:8080` and `APP_ORIGIN=127.0.0.1:8080` accept both
  loopback spellings (and IPv6 loopback), but do not enable LAN hosts.
- An explicit non-loopback HTTP hostname is rejected. Production accepts only
  an explicit absolute HTTPS origin.

The same resolved policy is used for host validation, credentialed CORS,
login/logout origin checks, secure transport, response signatures, and the
WASM bootstrap base URL.

Secret loading prefers the `_FILE` variant, suitable for mounted Kubernetes,
Docker, or systemd credentials. Never commit `.env`; it is ignored by Git.

## Key generation

Generate the X25519 transport key, Ed25519 signing seed, and corresponding
public WASM pins:

```sh
make keys
```

The output contains private material. Put the `APP_*` private values in your
secret manager and retain the `WASM_TRUSTED_*` public values for the WASM build.
Generate independent session and operations secrets, for example:

```sh
openssl rand -base64 32 | tr '+/' '-_' | tr -d '='
```

Use distinct values for each purpose and environment. Rotate keys using new
explicit key IDs and a controlled deployment overlap.

## Secure WASM production build

WASM encryption is additional application-layer protection; it does not replace
TLS, authentication, authorization, validation, or endpoint policy. For a
production build, provide the complete immutable public trust bundle:

```sh
export WASM_TRUSTED_ORIGIN='https://app.example.com'
export WASM_TRUSTED_TRANSPORT_KEY='<X25519 public value from make keys>'
export WASM_TRUSTED_TRANSPORT_KEY_ID='app-transport-v1'
export WASM_TRUSTED_RESPONSE_KEY='<Ed25519 public value from make keys>'
export WASM_TRUSTED_RESPONSE_KEY_ID='app-response-v1'
make wasm
```

This embeds the origin and both public keys/key IDs in `securefetch.wasm` and
regenerates `asset-manifest.json`. The server validates the manifest at startup;
the browser verifies WASM and runtime SRI before use. Publish the artifact hash
through an independently authenticated release channel. Never embed private
keys, session secrets, passwords, database credentials, or bearer tokens in
JavaScript or WASM.

The checked-in WASM bundle is for loopback development. Production refuses a
runtime-only trust configuration: the trust anchors must be compiled into the
artifact.

## Database

Startup creates the SQLite directory and an initial `users` table, then exposes
it through a typed `squealx` repository. Readiness includes a bounded database
ping. Replace the starter migration with a versioned migration system before
evolving a real schema, and use a production database appropriate to the
deployment topology.

## Operations

Supply the bearer token to inspect protected endpoints:

```sh
curl -H "Authorization: Bearer $APP_OPERATIONS_TOKEN" http://localhost:8080/_ops/ready
curl -H "Authorization: Bearer $APP_OPERATIONS_TOKEN" 'http://localhost:8080/_ops/metrics?format=prometheus'
```

Keep `/_ops/*` private at the network layer as defense in depth. The route list,
runtime data, readiness errors, and metrics reveal operational details.

## Production deployment checklist

- Set `APP_ENV=production` and an HTTPS `APP_ORIGIN`.
- Store all private values in a secret manager and expose them with `_FILE`.
- Use persistent transport/signing keys; ephemeral keys are development-only.
- Build the WASM artifact with all five matching `WASM_TRUSTED_*` values.
- Terminate TLS 1.3 at FH or a correctly configured trusted reverse proxy.
- Replace the demonstration password login with OIDC/WebAuthn or your identity
  system, and implement application-specific roles and authorization rules.
- Restrict `/_ops/*` at both the bearer-token and network layers.
- Back up the database, run versioned migrations, and configure monitoring.
- Review CSP/CORS/Host policies when adding origins or third-party assets.
- Run unit, integration, race, vulnerability, and browser tests in CI.
- Arrange an independent review before protecting high-value or regulated data.

## Multi-instance deployments

The starter’s session, rate-limit, secure-transport session, replay, nonce, and
device stores are process-local. Before horizontal scaling, provide shared
atomic TTL-capable stores (for example Redis/Valkey) and a durable device
registry. Encrypt secure-session key material with KMS/HSM-backed keys at rest.
Use a shared database and define key rotation, revocation, and failover policy.

## Testing

```sh
make check
go test -race ./...
go vet ./...
```

The included grant test verifies that registration grants are one-shot and
session-bound. Add tests for every business authorization rule, request schema,
database transaction, and failure path. Browser integration tests should verify
SRI loading, embedded trust, device registration, encrypted requests, signed
responses, logout/revocation, replay rejection, and key rotation.

## Security boundaries

Secure WASM protects the application protocol from passive/intercepting network
observers even beyond TLS, but plaintext still exists at application boundaries.
A compromised page, extension, browser, endpoint, build pipeline, server, or
operating system can access it. Keep dependencies patched, minimize same-origin
scripts, maintain CSP, audit sensitive actions, and never trust client-reported
authorization or transaction success.
