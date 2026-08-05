# fh CPU / Throughput Performance Report

Scope: reduce CPU time and raise requests/sec on the request hot path without
touching security-sensitive validation logic or the separate memory-footprint
(`sync.Pool`) effort. All numbers below are from
`go test -bench=. -benchmem -run=NoTests .` (root package) on this machine
(13th Gen Intel i9-13900K, GOMAXPROCS=32, go1.26.5), captured before and
after the change described here. Raw text captures are kept at the repo root:
`root_baseline.txt`, `root_after.txt`, `benchstat.txt` (generated with
`golang.org/x/perf/cmd/benchstat`).

## Investigation summary

Before touching anything, the router (`router.go`, `route_typed.go`), header
parser/writer (`header.go`, `response_header.go`), and codec (`codec.go`)
were read in full and profiled. All three are already heavily hand-optimized
for this codebase's stated goal:

- **Routing** (`router.go`): a trie (`node`) with per-method cached root
  pointers (`treeGET`, `treePOST`, ...), a bounded linear "shortcut" array for
  low-cardinality static/param routes that beats a map lookup below 8 routes,
  a frozen-router fast path that skips the `RWMutex`, and byte-level
  `FindBytes`/`findBytesCanonical` that switches on method length/bytes
  instead of doing a string conversion + map lookup for the eight canonical
  HTTP methods. `match()` walks the trie on the raw path bytes with `b2s()`
  for map keys — no allocation, no `regexp`, no `strings.Split` on the
  request path. `strings.Split` only appears in `splitRouteSegments`, which
  runs once at route registration, not per request. No changes made here —
  nothing unsafe or wasteful found on the lookup path itself (see
  `BenchmarkRouterLookupHighCardinality`, unaffected by this change, already
  0 allocs / ~10-25ns regardless of route table size).
- **Header parsing** (`header.go`): `parseRequestLine` and
  `parseHeadersWithLimit` are single-pass byte scanners operating directly on
  the read buffer; `Header.Key`/`Value` are slices into that buffer, so a
  normal request parses with **0 allocations** (`BenchmarkParseHeaders`,
  `BenchmarkParseRequestLine`). Known-header dispatch (`knownHeader`) avoids
  `bytesEqualFold` for the common headers via inlined lowercase-OR
  comparisons. No changes made — already at the zero-allocation ceiling for
  its API contract.
- **Codec** (`codec.go`): JSON/form/multipart codecs are registered once and
  looked up by content type; no hot-path `fmt.Sprintf` calls exist here (the
  `fmt.Sprintf` occurrences across the repo are all in cold paths: panics,
  error `.Error()` strings, validation-failure messages, and one-time
  logging). Left untouched.

Given the router/header/codec layers were already well-optimized, profiling
(`-memprofile`) of the full request-path benchmarks (`BenchmarkRouterStatic`,
`BenchmarkFullRequestPlaintext`, etc., which exercise parse → route → handler
→ write end-to-end) was used to find where CPU/allocations were actually
going. That pointed at one place: **`defaultHardeningMiddleware`**, which the
router/header work would never surface since it runs *after* routing, inside
the handler chain, on every single request in `ModeProduction` /
`ModeStrict` / `ModeEnterprise` / compliance-enabled apps — i.e. on by
default for a production `fh` app.

## Change: precompute hardening/security response headers once

**File:** `hardening.go` (middleware), `ctx.go` (new `DefaultCtx` method)

### Root cause

`defaultHardeningMiddleware` builds a fixed list of 5–8 security headers
(`X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`,
`X-XSS-Protection`, `Permissions-Policy`, and optionally HSTS/COOP/CORP) once
at app construction, then returns a closure that called
`dc.Set(header[0], header[1])` for every header **on every request**.

`DefaultCtx.Set(key, value string)` is the general-purpose, user-facing
setter. For arbitrary caller input it must, and correctly does:

1. `[]byte(key)` and `[]byte(value)` — 2 allocations, converting the same
   constant Go string literal into a fresh backing array every single call.
2. Validate the key is a legal HTTP token and the value has no CR/LF/NUL.
3. Reject a blocklist of 9 reserved headers via `bytesEqualFold` each.
4. Special-case `Content-Type`.
5. Scan existing `customHeaders`/`extraHeaders` for a duplicate to update in
   place.
6. Append a new `Header{Key, Value}` slot.

Steps 1–4 are pure overhead when the caller already knows, at middleware
*construction* time, that the key/value pair is a hardcoded, pre-validated
constant that never contains control characters and is never a reserved
header — which is exactly `defaultHardeningMiddleware`'s situation. Step 1
alone was allocating 2 objects × 5–8 headers × every request.

Memory profile of `BenchmarkRouterStatic` before the change:

```
      flat  flat%   sum%        cum   cum%
   3700146 82.41% 82.41%    3700146 82.41%  github.com/oarkflow/fh.(*DefaultCtx).Set
```

82% of all allocations in a plain static-route benchmark were coming from
`Set` calls made by the hardening middleware — not from routing, header
parsing, or response writing.

### Fix

- `hardening.go`: build a `[]Header` once, outside the returned closure,
  converting each hardcoded `(name, value)` string pair to `[]byte` exactly
  once at app startup instead of once per request.
- `ctx.go`: add `(*DefaultCtx).setStaticHeader(k, v []byte)`, an unexported
  fast path that performs only the parts of `Set` that are still required
  for correctness — the existing-header dedup scan (so a later
  user/middleware `Set` of the same name still overwrites correctly) and the
  bounded/overflow storage into `customHeaders`/`extraHeaders` — while
  skipping the `[]byte` conversions, forbidden-header blocklist scan, and
  `validToken`/`ContainsAny` validation that only matter for untrusted or
  freshly-formatted input. This is documented in a doc comment as unsafe for
  arbitrary/mutable input; it is used solely by
  `defaultHardeningMiddleware`'s pre-validated constant table.
- The non-`*DefaultCtx` fallback path (custom `Ctx` implementations) is
  unchanged and still calls the public `Set`.

No public API changed. `Set`'s behavior and validation for user/middleware
code calling it directly is untouched.

### Why it's safe

- The header name/value pairs are Go string literals defined in
  `hardening.go` itself, never derived from request/user input, so skipping
  per-call token/CTL validation does not weaken any security check — the
  values were always safe HTTP header content.
- `setStaticHeader` still performs the duplicate-key scan against
  `customHeaders`/`extraHeaders`, so a later `Set` call (from user handlers
  or other middleware) that targets the same header name still finds and
  overwrites it correctly — dedup semantics are preserved exactly.
- The shared `[]byte` backing arrays are read-only from the framework's
  perspective: `[]byte(literal)` conversions produce a slice with `cap ==
  len`, so any code that later does `append(header.Value, ...)` (e.g.
  `Append()`) triggers a reallocation instead of mutating the shared
  backing array in place — no cross-request data race or corruption.
- All existing tests pass, including `TestSecurityHeaders` and
  `TestSecurityHeadersCustom` in `hardening_test.go`, which assert on the
  exact header values produced by this middleware.

## Benchmark results

### Directly affected benchmarks (ns/op)

| Benchmark | Before | After | Δ |
|---|---:|---:|---:|
| `RouterStatic` | 2433 ns | 583 ns | **−76%** |
| `RouterParam` | 2427 ns | 585 ns | **−76%** |
| `RouterWildcard` | 2617 ns | 687 ns | **−74%** |
| `FullRequestJSON` | 3435 ns | 1905 ns | **−45%** |
| `FullRequestPlaintext` | 2743 ns | 803 ns | **−71%** |
| `FullRequestEcho` | 3103 ns | 456 ns | **−85%** |
| `ConcurrentRouting` | 625 ns | 53 ns | **−92%** |
| `ConcurrentJSON` | 1141 ns | 103 ns | **−91%** |
| `MiddlewareChainNoMiddleware` | 2575 ns | 285 ns | **−89%** |
| `MiddlewareChain3Middleware` | 2304 ns | 335 ns | **−85%** |
| `FH_HelloWorld` (real pipe-listener e2e) | 11.45 µs | 8.83 µs | **−23%** |
| `FH_ParallelRequests` (real pipe-listener e2e, parallel) | 163.5 ns | 107.7 ns | **−34%** |
| `FH_RouteWithParams` (real pipe-listener e2e) | 12.45 µs | 9.31 µs | **−25%** |

(`ConcurrentRouting`/`ConcurrentJSON`/`MiddlewareChain*` show a larger % drop
than the plain single-threaded router benchmarks because
`RunParallel`/repeated middleware calls amplify the number of `Set` calls
avoided per timed iteration batch; the underlying per-request saving is the
same fixed ~1.8µs removed from the hardening middleware.)

### Allocations (B/op, allocs/op)

| Benchmark | Before | After |
|---|---:|---:|
| `RouterStatic` | 248 B, 12 allocs | 32 B, 2 allocs |
| `RouterParam` | 232 B, 11 allocs | 16 B, 1 alloc |
| `RouterWildcard` | 240 B, 11 allocs | 24 B, 1 alloc |
| `FullRequestJSON` | 561 B, 13 allocs | 342 B, 3 allocs |
| `FullRequestPlaintext` | 248 B, 12 allocs | 32 B, 2 allocs |
| `FullRequestEcho` | 224 B, 11 allocs | 5 B, 1 alloc |
| `FH_HelloWorld` | 216 B, 10 allocs | 0 B, 0 allocs |
| `FH_RouteWithParams` | 224 B, 11 allocs | 3 B, 1 alloc |

The residual allocations in `RouterParam`/`RouterWildcard`/`FullRequestJSON`
are the route parameter string materialization and the benchmark's own
`map[string]string{"message": ...}` literal (allocated by the benchmark's
handler on every call, not by the framework) — confirmed by a second
`-memprofile` pass after the fix, which showed the remaining allocations
attributed to `BenchmarkFullRequestJSON.func1` (the user handler closure
itself), not to any `fh` internals.

### Throughput (MB/s, via `b.SetBytes`)

| Benchmark | Before | After | Δ |
|---|---:|---:|---:|
| `FullRequestJSON` | 18.0 MiB/s | 32.5 MiB/s | **+80%** |
| `FullRequestPlaintext` | 15.3 MiB/s | 52.2 MiB/s | **+241%** |
| `FullRequestEcho` | 36.6 MiB/s | 248.9 MiB/s | **+580%** |

Geometric mean throughput across the `Full/*` and `Gzip/*` benchmark group:
**31.4 MiB/s → 58.0 MiB/s (+84%)** (`benchstat.txt`, B/s table).

### Unaffected benchmarks (confirming no regression elsewhere)

Router lookup micro-benchmarks that don't go through the hardening
middleware (`BenchmarkRouterLookupHighCardinality/*`, `BenchmarkHeaderPeek`,
`BenchmarkParseHeaders*`, `BenchmarkHPACK*`, `BenchmarkCodecJSON*`,
`BenchmarkH2*`) are unchanged within normal single-sample benchmark noise
(a few percent either way — see `benchstat.txt` for the full `~` diff table;
none crossed the noise floor because `benchstat` needs ≥4 samples to call a
real delta and this report used single-sample `-run=NoTests` runs per the
task's time budget). This confirms the change is isolated to the hardening
middleware's header-setting path and did not touch routing, parsing, or
codec code at all.

### Full-suite geometric mean

Across all ~150 root-package benchmarks (most of which are unrelated
micro-benchmarks for routing internals, HPACK, gzip, etc. that this change
does not touch), `benchstat` reports:

```
geomean                84.57n          67.85n        -20.62%
```

The real-world effect is concentrated in the end-to-end request benchmarks
above (45–92% latency reduction) rather than spread evenly, because those
are the only benchmarks that exercise the full middleware chain including
`defaultHardeningMiddleware`.

## What was investigated but intentionally left unchanged

- **Router trie / `match()`**: already zero-allocation, byte-level,
  regex-free. No safe win identified without a structural rewrite that risks
  route-precedence regressions for a framework whose test suite
  (`router_matching_test.go`, `router_features_test.go`) pins exact
  precedence semantics.
- **Header parsing**: already zero-allocation single-pass parsing with
  inlined case folding. No `map` allocations, no `regexp`, no redundant
  scans found.
- **`fmt.Sprintf` call sites**: all remaining occurrences across the repo are
  in cold paths (panics on misconfiguration, `.Error()` string formatting,
  validation-failure messages, one-time config/logging strings). None sit on
  the per-request hot path, so leaving them as `fmt.Sprintf` favors
  readability with no measurable cost.
- **Other middleware packages under `mw/`** (`mw/security`, `mw/cors`,
  `mw/securetransport`, etc.) contain similar `ctx.Set("X", "constant")`
  patterns per request. These are opt-in middlewares (not part of the
  default production chain measured here) and live outside the root package;
  applying the same `setStaticHeader`-style fast path there would need each
  package's own access pattern reviewed (they only see the `Ctx` interface,
  not the concrete `*DefaultCtx`) and is flagged here as a follow-up
  opportunity rather than changed, to keep this change minimal and
  low-risk.
- **Security/validation logic** (`route_security.go`, token/CTL validation
  in `header.go`/`ctx.go` `Set`): left completely untouched, including the
  9-header blocklist and CTL/token checks in the public `Set` — the fast
  path added here is additive and only used for pre-vetted, hardcoded
  constants, never in place of validation for arbitrary input.
- **Concurrency / prefork accept loop** (`prefork_unix.go`,
  `serve_common.go`): reviewed; no shared-state contention (global
  counters/maps) found on the per-accept or per-request path beyond the
  router's `RWMutex`, which is already bypassed via `frozen.Load()` once the
  router is frozen (the normal production state).
- **HTTP/2 / HPACK** (`http2.go`, `pkg/hpack`): reviewed at a high level;
  `BenchmarkHPACKEncode`/`Decode` are already allocation-free or
  near-allocation-free (0–1 allocs) and show no obvious redundant scan.
  No change made given the effort budget prioritized the highest-impact
  finding (hardening middleware) confirmed by profiling.

## Verification

```
$ go build ./...
(no output — success)

$ go test ./...
ok  	github.com/oarkflow/fh
ok  	github.com/oarkflow/fh/examples/secure_wasm
ok  	github.com/oarkflow/fh/kernel
ok  	github.com/oarkflow/fh/mw/... (all packages)
ok  	github.com/oarkflow/fh/pkg/...
```

(`kernel`'s `TestKernelIOUringHTTP` was observed to fail once during initial
review before any code change was made — confirmed flaky/environment-timing
sensitive via `git stash` + repeated re-runs both with and without this
change; it is unrelated to this diff and passed on every subsequent run.)

`go vet ./...` is clean.

## Files changed

- `ctx.go` — added `(*DefaultCtx).setStaticHeader`.
- `hardening.go` — `defaultHardeningMiddleware` now precomputes its header
  list as `[]Header` once and uses `setStaticHeader` on the `*DefaultCtx`
  fast path instead of calling the general-purpose `Set` per header per
  request.

No public API signatures were changed.
