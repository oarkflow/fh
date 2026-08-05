# fh Security Hardening Audit — 2026-08-05

## Scope and method

This audit covered the root package (`app.go`, `header.go`, `chunked.go`,
`codec.go`, `request_body.go`, `cookie.go`, `client_ip.go`, `tls_config.go`,
`hardening.go`, `errors.go`, `redaction.go`, `websocket.go`, `http2*.go`,
`fs.go`, `files.go`) and the `mw/` middleware set most relevant to the six
focus areas in the brief (smuggling/parsing, DoS resistance, TLS, auth/cookie
safety, memory-safety in parsers, weak crypto), plus `pkg/httpsignature`,
`pkg/securetransport`, `pkg/websocket`, and `pkg/storage/sql`.

Method: manual read-through of each file against the specific attack class
(request smuggling ambiguity, slowloris, decompression bombs, cipher/version
downgrade, XFF spoofing, integer overflow, timing side channels, weak RNG),
cross-checked against the existing hardening test suite
(`hardening_test.go`, `security_regression_test.go`,
`standards_hardening_test.go`, `cookie_security_test.go`, `route_security.go`,
`fuzz_test.go`, `h2_fuzz_test.go`, `h2spec_test.go`) to see what invariants
were already pinned down, followed by targeted greps for known-bad patterns
(`math/rand` outside jitter/backoff, `md5`/`sha1` outside protocol-mandated
uses, `InsecureSkipVerify`, `fmt.Sprintf` building SQL from untrusted input,
unbounded `make([]byte, n)` from attacker-controlled `n`, etc.).

## Headline finding

**This codebase already carries a mature, dedicated security-hardening layer**
(see commit `fdccb46 "fix security gaps"` and the accumulated
`hardening.go` / `route_security.go` / `*_security_test.go` /
`*_hardening_test.go` files). Working through the six priority areas in the
brief one at a time:

1. **Request smuggling** — `header.go`'s `parseHeadersWithLimit` already
   rejects: obsolete line-folding (RFC 9112 §5.5), LF-only line endings,
   duplicate `Host`, duplicate conflicting `Content-Length`, more than one
   `Transfer-Encoding` header, `Transfer-Encoding` combined with
   `Content-Length`, stacked transfer-codings (`gzip, chunked`), and missing
   `Host` on HTTP/1.1. `chunked.go` enforces a max chunk-size-line length,
   bounds the accumulated body against `maxBody`, and rejects malformed
   chunk trailers (including trailers that try to smuggle `Content-Length`/
   `Transfer-Encoding`/`Host`). HTTP/2 has CONTINUATION-flood protection
   (`maxContinuationFrames = 64` in `http2.go`), matching the 2024
   CVE-2024-27316-class mitigation.
2. **DoS resistance** — `app.go` arms a single absolute read deadline for
   header accumulation (not reset per byte), which specifically defeats a
   slowloris-style trickle attack; `MaxRequestBodySize` (default 4 MiB) is
   enforced in the core read loop independent of whether `mw/bodylimit` is
   mounted; `mw/decompress` bounds both absolute expanded size and expansion
   ratio to stop zip-bomb-style attacks; `hardening.go` adds an optional
   in-flight-request/goroutine/heap-based load shedder.
3. **TLS** — `tls_config.go` defaults to TLS 1.3, refuses `MinVersion` below
   TLS 1.2, defaults to X25519/P-256/P-384 curves, and requires a client CA
   pool before enabling any client-cert verification mode.
4. **Header/cookie/auth safety** — `cookie.go` enforces `__Secure-`/`__Host-`
   prefix rules, forces `Secure` when `SameSite=None`, and validates
   Path/Domain against CRLF injection. `mw/basicauth`, `mw/apikey`,
   `mw/csrf`, and `mw/session` all use `crypto/subtle`/`hmac.Equal` for
   credential comparisons and `crypto/rand` for token generation, and
   `mw/basicauth` runs a dummy-hash comparison on both "user not found" and
   "user disabled" to avoid a timing oracle for username enumeration.
   `mw/realip` fails closed (does not honor `X-Forwarded-For`/`Forwarded`)
   unless the immediate peer is in a configured trusted-proxy CIDR list, and
   walks the forwarding chain right-to-left so only trusted hops can assert
   the next IP left of them.
5. **Parser memory-safety** — `parseContentLength`/`parseHex` in
   `header.go`/`chunked.go` check for overflow before multiplying rather than
   after; `app.go`'s connection loop and panic points are wrapped in
   `recover()` with details logged only when `Debug`/`LogInternal` is set
   (never echoed to the client).
6. **Weak crypto/RNG** — the only `math/rand` usage outside tests is
   `httpclient.go`'s retry-backoff jitter, which is not security-sensitive.
   All token/nonce/salt generation found (`mw/csrf`, `mw/apikey`,
   `mw/basicauth` PBKDF2 salts, `mw/session`, `mw/security` CSP nonces,
   `mw/requestid`) uses `crypto/rand`. The only `sha1` usage
   (`pkg/websocket/websocket.go`) is the RFC 6455-mandated
   `Sec-WebSocket-Accept` computation, not a security primitive.

Given this baseline, the actionable, previously-unaddressed gap this audit
found and fixed is below.

## Vulnerability found and fixed

### V1 — `RedactMap` did not recurse into slices, so secrets nested in arrays leaked into audit/log output

- **File / location**: `redaction.go`, `Redactor.RedactMap` (used by
  `audit.go:187` to scrub `AuditEvent.Metadata` before it is written to the
  audit log/journal).
- **Class**: Information disclosure (sensitive-data leakage into logs),
  adjacent to focus area 4 (auth/cookie/PII safety) and the compliance/audit
  subsystem's PII-redaction guarantee.
- **Root cause**: `RedactMap` only special-cased `map[string]any` and
  `string` values; a `[]any` or `[]map[string]any` value was returned
  unmodified via the `default` branch. Any audit metadata shaped like
  `{"users": [{"username":"alice","password":"hunter2"}, ...]}` — a very
  common shape for "list of records affected by this action" metadata — had
  its nested `password`/`token`/`api_key`/etc. fields written to the audit
  log or compliance journal in cleartext, defeating the purpose of the
  redaction layer entirely for that (common) shape of input.
- **Fix**: extracted a `redactValue` helper that recurses into `map[string]any`,
  `[]any`, and `[]map[string]any`, so nested secrets at any depth composed of
  maps and slices are now redacted the same as top-level ones. Non-container,
  non-string values are still passed through unchanged (as before) to avoid
  mutating unrelated data (numbers, bools, structs the caller may still want
  raw).
- **Diff**: `redaction.go` (`Redactor.RedactMap` / new `Redactor.redactValue`).
- **Test evidence**: added `redaction_test.go` —
  `TestRedactMapNestedSlices` builds metadata with a `[]any` of
  `map[string]any` (mixed `password`/`api_key` fields) and a `[]map[string]any`
  with a `token` field, and asserts every sensitive nested field is replaced
  while non-sensitive fields and top-level strings are preserved.
  `TestRedactMapNilAndSensitiveKey` covers the pre-existing nil/case-insensitive
  behavior wasn't broken by the refactor.

  ```
  $ go test . -run TestRedact -v
  === RUN   TestRedactMapNestedSlices
  --- PASS: TestRedactMapNestedSlices (0.00s)
  === RUN   TestRedactMapNilAndSensitiveKey
  --- PASS: TestRedactMapNilAndSensitiveKey (0.00s)
  PASS
  ok  	github.com/oarkflow/fh	(cached)
  ```

## Test-coverage gaps closed (defense-in-depth, no bug found but no regression guard existed)

Two packages central to focus areas 3 and 4 had zero test files
(`go test ./...` reported `[no test files]` for both), meaning a future
change could silently weaken their secure defaults with nothing catching it
in CI:

### `mw/security` (security response headers / CSP nonce middleware)

Added `mw/security/security_test.go`:
- `TestDefaultHeadersAreSecureByDefault` — pins that `New()` called with no
  config (the easy-to-reach "I forgot to configure it" path) still emits
  `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`,
  `Referrer-Policy: no-referrer`, and the two `Cross-Origin-*` isolation
  headers.
- `TestCSPNonceIsUniquePerRequest` — asserts two requests get two distinct
  CSP nonces (guards against a future refactor that caches/precomputes the
  nonce and defeats the whole point of CSP nonces).

### `mw/mtls` (client-certificate authentication middleware)

Added `mw/mtls/mtls_test.go`:
- `TestRequiredRejectsPlaintextConnection` — `Config{Required: true}` over a
  plaintext (non-TLS) connection must fail closed (401), not be treated as
  "no TLS state means the check doesn't apply."
- `TestOptionalAllowsPlaintextConnection` — confirms `Required: false` is
  still a true opt-in and doesn't accidentally become globally fail-closed.
- `TestAllowedEmptyListPermitsAny` / `TestAllowedIsCaseInsensitiveAndTrimmed`
  — pin the `allowed()` allowlist helper's exact-match (not substring),
  case-insensitive, whitespace-trimming semantics, since a substring-match
  regression there (`allowed("evil.example.com", []string{"example.com"})`)
  would silently widen who is allowed to authenticate.

```
$ go test ./mw/security/... ./mw/mtls/... -v
--- PASS: TestDefaultHeadersAreSecureByDefault (0.01s)
--- PASS: TestCSPNonceIsUniquePerRequest (0.01s)
ok  	github.com/oarkflow/fh/mw/security	0.024s
--- PASS: TestRequiredRejectsPlaintextConnection (0.01s)
--- PASS: TestOptionalAllowsPlaintextConnection (0.01s)
--- PASS: TestAllowedEmptyListPermitsAny (0.00s)
--- PASS: TestAllowedIsCaseInsensitiveAndTrimmed (0.00s)
ok  	github.com/oarkflow/fh/mw/mtls	0.024s
```

Full-repo test suite and build/vet after all changes are in the final
report section below.

## NOT FIXED / needs a human decision

- **`mw/mtls` real-certificate integration coverage**: the new tests exercise
  the fail-closed/allowlist logic over plaintext connections (which is where
  the actual security-relevant branch — "no verified chain present" — lives),
  but do not spin up a real TLS listener with a client certificate chain to
  exercise the `VerifiedChains`/`PeerCertificates`/`AllowUnverified` branches
  end-to-end. Building a throwaway CA + leaf + client cert chain in-test is
  straightforward but nontrivial, and the existing `pkg/securetransport`
  package already has that kind of TLS fixture machinery; wiring `mw/mtls`
  tests to reuse it is a reasonable follow-up but was out of scope for the
  time budget here. No known bug motivates this — it's a coverage gap, not a
  vulnerability.
- **`mw/compress`, `mw/bodylimit`, `mw/idempotency`, `mw/requesthash`,
  `mw/policy`, `mw/logger`, `mw/metrics`, `mw/pprof`, `mw/privacy` have no
  test files.** I read all of them; none showed a security defect worth a
  targeted fix, but "no defect found on a read-through" is weaker evidence
  than a test suite. Recommend a follow-up pass to backfill tests package by
  package rather than doing it wholesale here, since several (`mw/pprof`)
  are debug-only surfaces where the main security property ("don't mount
  this in production without an authz gate") is a documentation concern, not
  something a unit test enforces.
- **CSRF double-submit token is never rotated after authentication
  (`mw/csrf/csrf.go`)**: the token is generated once per (missing-cookie)
  request and reused for the cookie's lifetime; it is not regenerated on
  login/privilege change. This is a defensible design choice for a
  stateless double-submit-cookie CSRF scheme (rotating it would require
  session-scoped storage, which the middleware deliberately avoids to stay
  stateless), but it does mean a token fixed before authentication remains
  valid after. Flagging for a human decision because "fix" here means a
  design change (session-bound tokens), not a bug fix, and the brief asked
  me to keep changes minimal and backward compatible.
- **`RedactMap`/`redactValue` does not recurse into arbitrary `[]T` slice
  types via reflection** (only `[]any` and `[]map[string]any`, the two
  shapes `encoding/json`-decoded data actually produces). A caller who hands
  `RedactMap` a `[]CustomStruct` typed slice (rather than JSON-decoded
  `map[string]any`/`[]any`) will not have it redacted. Given `RedactMap`'s
  only current call site (`audit.go`) works on JSON-shaped metadata, this
  covers the real-world input; a reflection-based general fallback was left
  out to avoid adding `reflect` to a hot path and to keep the fix minimal,
  but should be reconsidered if `RedactMap` grows new callers with
  strongly-typed input.

## Final verification

```
$ go build ./...
(no output — success)

$ go vet ./...
(no output — success)

$ go test ./...
ok  	github.com/oarkflow/fh	7.003s
ok  	github.com/oarkflow/fh/examples/secure_wasm	0.008s
ok  	github.com/oarkflow/fh/kernel	0.009s
ok  	github.com/oarkflow/fh/mw/acceptquery	0.005s
ok  	github.com/oarkflow/fh/mw/admin	0.007s
ok  	github.com/oarkflow/fh/mw/apikey	0.055s
ok  	github.com/oarkflow/fh/mw/backpressure	0.004s
ok  	github.com/oarkflow/fh/mw/basicauth	0.076s
ok  	github.com/oarkflow/fh/mw/bulkhead	0.007s
ok  	github.com/oarkflow/fh/mw/cache	0.149s
ok  	github.com/oarkflow/fh/mw/circuitbreaker	0.012s
ok  	github.com/oarkflow/fh/mw/coalesce	0.127s
ok  	github.com/oarkflow/fh/mw/contentdigest	0.007s
ok  	github.com/oarkflow/fh/mw/cors	0.167s
ok  	github.com/oarkflow/fh/mw/csrf	0.005s
ok  	github.com/oarkflow/fh/mw/decompress	0.005s
ok  	github.com/oarkflow/fh/mw/etag	0.004s
ok  	github.com/oarkflow/fh/mw/hostguard	0.005s
ok  	github.com/oarkflow/fh/mw/httpsignature	0.097s
ok  	github.com/oarkflow/fh/mw/ipreputation	0.023s
ok  	github.com/oarkflow/fh/mw/ipthrottle	0.024s
ok  	github.com/oarkflow/fh/mw/ipwhitelist	0.166s
ok  	github.com/oarkflow/fh/mw/loadshed	0.005s
ok  	github.com/oarkflow/fh/mw/maintenance	0.005s
ok  	github.com/oarkflow/fh/mw/mtls	0.025s
ok  	github.com/oarkflow/fh/mw/proxy	0.208s
ok  	github.com/oarkflow/fh/mw/ratelimiter	0.021s
ok  	github.com/oarkflow/fh/mw/realip	0.003s
ok  	github.com/oarkflow/fh/mw/replay	0.047s
ok  	github.com/oarkflow/fh/mw/retrybudget	0.185s
ok  	github.com/oarkflow/fh/mw/securetransport	0.061s
ok  	github.com/oarkflow/fh/mw/security	0.027s
ok  	github.com/oarkflow/fh/mw/session	0.009s
ok  	github.com/oarkflow/fh/mw/signature	0.057s
ok  	github.com/oarkflow/fh/mw/slidingwindow	0.078s
ok  	github.com/oarkflow/fh/mw/slowloris	0.002s
ok  	github.com/oarkflow/fh/mw/smartcache	0.244s
ok  	github.com/oarkflow/fh/mw/tenant	0.044s
ok  	github.com/oarkflow/fh/mw/timestamp	0.089s
ok  	github.com/oarkflow/fh/mw/tracing	0.003s
ok  	github.com/oarkflow/fh/mw/webhook	0.110s
ok  	github.com/oarkflow/fh/mw/workflow	0.131s
ok  	github.com/oarkflow/fh/pkg/cluster	(cached)
ok  	github.com/oarkflow/fh/pkg/config	0.002s
ok  	github.com/oarkflow/fh/pkg/hpack	(cached)
ok  	github.com/oarkflow/fh/pkg/httpsignature	(cached)
ok  	github.com/oarkflow/fh/pkg/securetransport	(cached)
ok  	github.com/oarkflow/fh/pkg/storage/kv	(cached)
ok  	github.com/oarkflow/fh/pkg/storage/memory	0.002s
ok  	github.com/oarkflow/fh/pkg/storage/sql	0.002s
ok  	github.com/oarkflow/fh/pkg/websocket	0.003s
```

(all remaining listed packages report `?  ... [no test files]`, none `FAIL`.)

## Files changed

```
 redaction.go                    | 36 ++++++++++++++++++++++++++++--------
 redaction_test.go                | new file
 mw/security/security_test.go     | new file
 mw/mtls/mtls_test.go             | new file
```
