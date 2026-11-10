# FH Control Center (frontend)

The browser application served by this project, built with
[`@oarkflow/lithe`](https://www.npmjs.com/package/@oarkflow/lithe) - a
zero-dependency, fine-grained-reactive TypeScript frontend framework. It
renders into the `#app` element of `web/templates/index.html`, which the Go
server owns; this directory only produces `web/public/app.js` and
`web/public/app.css`.

Neither file is checked in - `web/public/` is build output. Run `npm install
&& npm run build` here (or `make frontend` from the project root) before the
server has anything to serve at `/assets/*`, and again after changing
anything under `src/`.

## Layout

```text
src/
  index.tsx              mounts <App/> into #app, imports the stylesheet
  app.tsx                composition root: checking / signed-out / signed-in
  styles/app.css          the whole design system (tokens, layout, components)
  lib/
    types.ts              shared types + the ApiError shape every call throws
    http.ts                one JSON fetch helper for the plain /auth/* endpoints
    wasm-bridge.ts         the one safe dynamic import of /wasm/index.js
    secure-client.ts       the reusable, structured API client (see below)
    reactive.ts            resolve() helper for components accepting either
                            a signal, a getter, or a plain value as a prop
  state/session.ts          the app's client state: a handful of signals plus
                            the actions (signIn, signOut, loadProfile, ...)
                            that mutate them - components only ever import
                            from here, never from lib/secure-client.ts directly
  components/
    ui/                    generic, reusable pieces: Button, Card, Badge,
                            Alert, Spinner, StatTile, CodeBlock, ThemeToggle
    layout/                AppShell, Sidebar, Topbar - the page frame
  features/
    auth/LoginPanel.tsx     the sign-in form
    dashboard/              ProfileCard, SessionCard, ApiConsole, ActivityLog,
                            and DashboardPanels, which swaps between them
```

## Extending it

- **A new encrypted endpoint**: add one function to `secureApi` in
  `lib/secure-client.ts`, one action in `state/session.ts`, call it from a
  panel (or a new one under `features/`).
- **A new panel**: add an entry to `NAV_ITEMS` in
  `components/layout/Sidebar.tsx`, a `Panel` id in `state/session.ts`, and a
  `<Show>` branch in `features/dashboard/DashboardPanels.tsx`.
- **Real, navigable routes** (as opposed to the three panels this starter
  ships, which are just a `Show`-driven view state - there is only one
  logical page here): swap `app.tsx`'s phase `Show` blocks for
  `@oarkflow/lithe/router`'s `createRouter`/`Link`/`Outlet`. The router
  supports nested/parallel routes, typed loaders, and lazy route components;
  see the package README's "Complete router example".

## Frontend function guide

### App flow and state

| Function or state | File | Responsibility |
|---|---|---|
| `App` | `src/app.tsx` | Starts session restoration and renders checking, sign-in, or authenticated dashboard phases. |
| `restoreSession` | `src/state/session.ts` | Reads the web session, establishes secure transport when logged in, and records restore status. |
| `signIn` | `src/state/session.ts` | Authenticates through the same-origin login API, then creates the secure device session. |
| `signOut` | `src/state/session.ts` | Revokes secure transport, destroys the web session, and clears local profile state. |
| `loadProfile` | `src/state/session.ts` | Loads `/api/me` through the encrypted client. |
| `sendEcho` | `src/state/session.ts` | Sends the typed sample payload through encrypted `POST /api/echo`. |
| `selectPanel` | `src/state/session.ts` | Changes the active dashboard panel. |
| `record` | `src/state/session.ts` | Adds a bounded activity entry for user-visible actions and errors. |
| `phase`, `userID`, `profile`, `lastEcho`, `secureInfo`, `activity`, `formError`, `busy`, `activePanel` | `src/state/session.ts` | Reactive application state; components subscribe to these signals. `isAuthenticated` and `isChecking` are derived computed values. |

### HTTP and secure transport

| Function or API | File | Responsibility |
|---|---|---|
| `requestJSON` | `src/lib/http.ts` | Performs same-origin JSON calls for login/session/bootstrap endpoints. |
| `parseJSONResponse` | `src/lib/http.ts` | Parses responses, returns validated data, and turns HTTP or shape errors into `ApiError`. |
| `authApi.session`, `authApi.login`, `authApi.logout`, `authApi.secureConfig` | `src/lib/secure-client.ts` | Plain same-origin authentication and secure-bootstrap requests. |
| `ensureSecureSession` | `src/lib/secure-client.ts` | Loads the trusted WASM bridge, registers the device, and opens an encrypted session. |
| `secureSessionInfo`, `hasSecureSession` | `src/lib/secure-client.ts` | Expose safe session status to the UI. |
| `revokeSecureSession` | `src/lib/secure-client.ts` | Revokes the active secure transport session during logout. |
| `secureApi.me`, `secureApi.echo` | `src/lib/secure-client.ts` | Typed encrypted API calls; protected calls must be added here rather than using plain Fetch. |
| `deviceLabel` | `src/lib/secure-client.ts` | Produces a non-secret browser label for device registration. |
| `loadWasmBridge` | `src/lib/wasm-bridge.ts` | Dynamically imports the unbundled, integrity-checked `/wasm/index.js` API. |
| `resolve` | `src/lib/reactive.ts` | Normalizes a signal, getter, or plain value passed to a reusable component. |
| `describeError`, `isRecord`, `isStringArray`, `isSessionStatus`, `isSecureBootstrapConfig`, `isProfileResponse`, `isEchoResponse`, `isLoginResponse`, `isEmptyResponse` | `src/lib/types.ts` | Formats errors and validates untrusted JSON shapes before data reaches application state. |

### Components and event handlers

| Function | File | Responsibility |
|---|---|---|
| `LoginPanel`, `handleSubmit` | `src/features/auth/LoginPanel.tsx` | Presents the sign-in form and submits credentials to `signIn`. |
| `DashboardPanels` | `src/features/dashboard/DashboardPanels.tsx` | Selects the overview, secure API, or activity panel. |
| `ProfileCard` | `src/features/dashboard/ProfileCard.tsx` | Shows the authenticated profile and triggers profile loading. |
| `SessionCard` | `src/features/dashboard/SessionCard.tsx` | Shows secure-session status and transport details safe for display. |
| `ApiConsole`, `submit` | `src/features/dashboard/ApiConsole.tsx` | Sends the sample encrypted request and displays its response. |
| `ActivityLog`, `timeLabel` | `src/features/dashboard/ActivityLog.tsx` | Displays recent actions and formats their timestamps. |
| `AppShell`, `Sidebar`, `Topbar` | `src/components/layout/` | Composes the signed-in page frame, panel navigation, and session controls. |
| `Button`, `Card`, `Badge`, `Alert`, `Spinner`, `StatTile`, `CodeBlock` | `src/components/ui/` | Reusable presentation components with typed props. |
| `ThemeEffect`, `ThemeToggle`, `preferredTheme`, `toggle` | `src/components/ui/ThemeToggle.tsx` | Loads and persists the light/dark preference and toggles the theme. |

Response validators are deliberate: secure transport authenticates response
origin and integrity, while the frontend validators separately enforce each
JSON shape before the UI trusts it. Activity entries are diagnostic UI state;
security audit records belong on the server.

## Commands

```sh
npm install        # fetches @oarkflow/lithe from the registry (needs network)
npm run dev         # lithe dev . - local dev server with HMR
npm run build       # lithe build . --bundle=single --out=../public
npm run check        # structural lint (islands, a11y, reachability, ...)
npm run typecheck    # Lithe's own semantic TypeScript assignability check
```

`npm run build` emits exactly `../public/app.js` and `../public/app.css` -
single-file bundle mode, chosen deliberately: the server's SPL template
renders in secure mode, which strips every `<script>` element from HTML and
then appends exactly one constant, application-owned
`<script src="/assets/app.js" type="module">` tag on the response path (see
`internal/httpapi/routes.go`, `renderIndex`). Chunked or unbundled output
would need multiple script/module-preload tags, which that security model
does not allow. Don't switch `--bundle` away from `single` without updating
`renderIndex` and its CSP to match.

## Why the WASM loader looks unusual

`lib/wasm-bridge.ts` loads `/wasm/index.js` with `import(\`/wasm/index.js\`)`
- a template literal, not a string literal. That file is intentionally
outside this build (see `web/wasm/README.md`): it is unbundled,
integrity-checked at both ends, and the server refuses to start if
`web/wasm/asset-manifest.json` is missing its hash. Lithe's single-file
bundler rewrites a string-literal `import('/wasm/index.js')` into a lookup
against its own internal module registry, which has no entry for a path
outside `src/`, and that lookup would fail silently at runtime. A
template-literal specifier isn't string-literal syntax to that rewriter, so
it passes through unchanged as a genuine native dynamic import. Keep it that
way if you touch this file.
