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
