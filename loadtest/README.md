# fh load and failure tests

This module implements the "Load and failure testing" checklist from
[`../docs/production-readiness.md`](../docs/production-readiness.md) as real,
automated Go tests instead of a manual pre-release checklist. It is a
**separate Go module** (its own `go.mod`, requiring the root `fh` module via
a relative `replace github.com/oarkflow/fh => ../`) so that:

- `go test ./...` at the repo root never runs, times, or flakes on these
  tests — they simply aren't part of that module;
- these tests can still freely import and drive real `*fh.App` instances,
  real listeners, real TLS, and real goroutines/timers, because Go modules
  can depend on sibling modules in the same repo via `replace`.

Every test here drives a **real** `fh.App` over a **real** `net.Listener` /
`net.Dial` / TLS handshake / HTTP client. Nothing in this module fakes the
mechanism it's testing (e.g. no test calls a timeout's internal logic
directly instead of actually waiting for a real deadline) — the "load and
failure" in the name is a real load and a real, injected failure.

## Running the short (default, CI-bound) suite

```sh
cd loadtest
go test ./...
```

This runs in well under a minute (whole-suite runtime observed around
12-15s on a laptop; `TestSlowlorisNoBytes` is the single slowest sub-test at
~2s because it has to wait out a real header-read timeout). Every test uses
short, deterministic, generously-bounded-but-finite timeouts, so a genuine
regression (a hang, a leak, a missed timeout) fails the test rather than
making the suite slow.

`go vet ./...` and `gofmt -l .` are clean inside this module.

**A note on running this suite in a tight loop.** These tests dial a lot of
real loopback TCP/TLS connections — that's the point, they're exercising
real network behavior. Each short-lived connection this suite closes
proactively uses `SO_LINGER=0` (RST-close, skipping `TIME_WAIT`) specifically
so the suite doesn't pressure the host's ephemeral port range even under
repeated runs. Under an artificial stress test of re-running the *entire
suite* back-to-back many times within the same couple of minutes on one
machine, occasional transient `"can't assign requested address"` dial
failures in the TLS flood test were still observed (verified during
development of this module) — that is this suite's own prior runs still
draining `TIME_WAIT` on a small, shared local ephemeral port range, not a
bug in fh or in these tests. It did not reproduce in a single normal run, or
in runs spaced a few seconds apart, which is how both a real CI job and an
ordinary local `go test ./...` behave. If you deliberately hammer this
module in a tight retry loop and see a `can't assign requested address`
dial error, that's what's happening; a plain rerun clears it.

## Running the long soak

Concern #1 (24-72 hour keep-alive and connection-churn soak) is implemented
as **one harness, parameterized by duration**, not two separate tests: the
same `TestSoakKeepAliveAndConnectionChurn` in `soak_test.go` runs for a few
seconds by default and for a much longer, configurable duration when you
opt in:

```sh
cd loadtest
FH_LOADTEST_SOAK=1 FH_LOADTEST_SOAK_DURATION=72h \
  go test -run TestSoakKeepAliveAndConnectionChurn -timeout 0 -v ./...
```

- `FH_LOADTEST_SOAK=1` switches the test from its short default (3s) to the
  long-soak path.
- `FH_LOADTEST_SOAK_DURATION` (a Go duration string, e.g. `2h`, `24h`, `72h`)
  sets how long that long-soak path actually runs; it defaults to `2h` if
  `FH_LOADTEST_SOAK=1` is set but the duration variable isn't.
- `-timeout 0` disables `go test`'s own test-binary timeout (default 10m),
  which would otherwise kill a multi-hour run partway through — required
  for anything longer than a few minutes.
- `-run TestSoakKeepAliveAndConnectionChurn` scopes the run to just the soak
  test; the other tests in this module don't have a long-duration mode
  (see "Coverage gaps" for why) and there's no reason to make a CI operator
  wait for them too.

Wire this into a nightly/manual CI job, not the normal per-commit job. A
real 24-72 hour run cannot happen inside an interactive coding session or a
normal CI job timeout — see "Coverage gaps" below for the explicit statement
of what this repository can and cannot exercise on that front.

## What each file tests

| File | Concern # | What it does |
|---|---|---|
| `helpers_test.go` | — | Shared test infrastructure: starting a real `*fh.App` on a real listener (`startApp`/`startTLSApp`), generating a fresh self-signed ECDSA certificate per TLS test (never a checked-in cert file), raw HTTP/1.1 request helpers, goroutine/heap sampling, and the `FH_LOADTEST_SOAK`/`FH_LOADTEST_SOAK_DURATION` parameterization (`soakDuration`). |
| `rlimit_unix.go` / `rlimit_windows.go` | #3 | Reads the process's real `RLIMIT_NOFILE` via `syscall.Getrlimit` on Unix (a stub returning a conservative constant on Windows, which has no direct equivalent), so descriptor-pressure tests can scale their connection counts safely below the real OS ceiling instead of guessing a fixed number. |
| `soak_test.go` | #1 | `TestSoakKeepAliveAndConnectionChurn`: a pool of real keep-alive `net/http` clients issuing requests continuously over reused connections, plus a separate pool of connect-request-disconnect churn workers, for a bounded duration (parameterized, see above). Asserts a bounded request error rate, bounded goroutine growth after the run settles, and no clear monotonic heap-growth trend across periodic samples. |
| `slowclients_test.go` | #2 | Five sub-tests, each a raw `net.Dial` client against an app with short, explicit timeouts: trickling request headers one byte at a time slower than `ReadHeaderTimeout`; a slowloris connection that sends nothing at all; an aborted upload that announces a `Content-Length` then closes mid-body; a body that stalls forever without closing; and a legitimate request sent then half-closed (`TCPConn.CloseWrite`) while still reading the response. Asserts the server enforces its timeouts (drops the connection, doesn't hang) for the adversarial cases, and completes the request normally for the legitimate half-close case, then that the server is still healthy afterward. |
| `fdlimits_test.go` | #3 | Drives real concurrent connections up to a configured `MaxConnections` (and, separately, `MaxConnectionsPerIP`) ceiling — sized well below the process's real `RLIMIT_NOFILE` (never actually exhausting OS descriptors) — and asserts connections beyond the ceiling are rejected cleanly (the TCP connection is closed, not hung or silently admitted), and that the server accepts connections again once load drops below the ceiling. |
| `memorypressure_test.go` | #4 | Configures `MaxInFlightRequests`, `MaxGoroutines`, and `MaxHeapBytes` (`hardening.go`'s `defaultResourceGuardMiddleware`) low and sends load designed to trip each. Asserts the server sheds load with `503` rather than crashing/OOMing, and that `MaxInFlightRequests`/`MaxGoroutines` recover to serving `200`s again once the burst passes. `MaxHeapBytes` recovery is demonstrated structurally rather than by forcing the GC — see "Coverage gaps". |
| `overload_test.go` | #6 | Mounts `mw/bulkhead` and, separately, `mw/loadshed` (the two purpose-built overload-control middlewares, as distinct from the built-in `MaxInFlightRequests` ceiling already covered in `memorypressure_test.go`) with a low concurrency ceiling and pushes a burst past it. Asserts clean rejection beyond the ceiling and full recovery once the burst passes (the semaphore/counter isn't left stuck). |
| `tlsflood_test.go` | #5 | Serves real TLS with a freshly generated self-signed certificate. Floods it with connections that open the TCP socket and never send a single TLS byte, concurrently with several connections that perform a real, legitimate handshake and HTTP request. Asserts `TLSHandshakeTimeout` actually drops the stalled connections (not held forever) and that legitimate concurrent handshakes still succeed while the flood is in progress. |
| `shutdown_test.go` | #7 | Four sub-tests covering graceful shutdown under live traffic: an in-flight HTTP/1.1 request, an in-flight real HTTP/2 request (via `golang.org/x/net/http2.Transport` over real TLS with ALPN), an active Server-Sent Events stream, and an active WebSocket connection (a small hand-rolled RFC 6455 client — fh has no client-side WebSocket helper — doing a real masked-frame handshake and echo round-trip against `pkg/websocket`). Asserts in-flight request/stream traffic completes rather than being cut off, new connections are rejected once shutdown begins, and `ShutdownWithTimeout` always returns within a bounded margin of its timeout — including for the WebSocket case, where fh's real contract is "force-close within the timeout," not "wait for the client to leave," since a long-lived full-duplex connection has no natural completion point. |
| `sharedstore_test.go` | #8 | A `flakyProvider`/`flakyStore` wrapping a real `kv.MemoryProvider`, wired in through the documented `fh.WithSharedState` + `app.MustStateStore` pattern from `docs/shared-state.md`, feeding `mw/ratelimiter` and (separately) `mw/replay`. Can simulate a hard outage (every store operation errors) or added latency on demand. Asserts — per what `ratelimiter.go`/`replay.go` actually do with a store error (return it up the middleware chain, which fh's default error handler turns into a 5xx) — that both middlewares **fail closed**, not open, on a real outage, that latency shows up as request latency rather than errors or a hang, and that both recover cleanly once the simulated outage ends. |
| `reliability_test.go` | #9 | Four sub-tests using `fh.OpenFileQueueStorage`/`fh.NewDurableQueue`/`fh.WithReliability` (the real reliability/queue API in `reliability.go`, `outbox_inbox.go`): a job claimed then abandoned (simulating a crash) is recovered back to pending — not lost, not duplicated — after closing and reopening storage against the same on-disk directory; a job whose handler always fails is retried exactly `MaxAttempts` times and then lands in the `"failed"` state (the DLQ), with no further silent retries; two genuinely concurrent HTTP requests sharing one `Idempotency-Key` and body produce exactly one handler side effect (reading `reliability.go`'s actual `Begin`/`Complete` design shows the loser of a real race gets an in-flight-conflict response, not an instant replay — this test asserts that real invariant, not an idealized one, and separately proves the documented replay guarantee once both requests have finished); and `mw/replay`'s store, filled to a configured `MaxEntries` cap, rejects further distinct entries rather than growing unbounded. |

## Coverage gaps

Being direct about what this harness does **not** prove, rather than
quietly shipping a test that doesn't actually exercise its claimed failure
mode:

- **A genuine 24-72 hour wall-clock soak has not been run as part of this
  work**, and could not be — it cannot fit inside an interactive session or
  a bounded CI job. What exists is a real harness (`TestSoakKeepAliveAndConnectionChurn`)
  that is mechanically capable of running that long unattended (see "Running
  the long soak" above) and has been spot-checked at a short override
  duration to confirm the parameterization, sampling, and assertions all
  work correctly. Whether fh actually holds up for 72 real hours of
  keep-alive/churn traffic is a claim only an actual 72-hour run — ideally
  against a separately-run server binary, not an in-process test — can
  support, and that run has not happened here.
- **Split-brain and multi-node failover (the second half of concern #8) are
  not exercised.** This repository's built-in `kv.Provider` implementations
  (`MemoryProvider`, `FileProvider`) are both explicitly process-local/
  single-host by design (see `docs/shared-state.md`). A real split-brain
  scenario needs an actual distributed backend (Redis Cluster, etcd,
  PostgreSQL with replication) and a real network partition between nodes,
  which is infrastructure this repository doesn't provide and this test
  module doesn't stand up. What *is* tested — outage, added latency, and
  recovery against a fault-injecting store wrapper, and the resulting
  fail-closed behavior in `mw/ratelimiter`/`mw/replay` — is the single-store
  half of concern #8, not the multi-node half.
- **Descriptor exhaustion is tested well below the real OS ceiling, on
  purpose.** Per the task's own constraint (and ordinary CI hygiene),
  `fdlimits_test.go` scales its connection count to a safe fraction of the
  process's actual `RLIMIT_NOFILE` and only exercises fh's own configured
  `MaxConnections`/`MaxConnectionsPerIP` ceilings. It does not prove
  behavior when the OS itself starts refusing `accept()`/`socket()` calls
  (`EMFILE`/`ENFILE`) — that would require actually exhausting descriptors,
  which is unsafe to do in a shared CI environment.
- **`MaxHeapBytes` recovery is demonstrated structurally, not by forcing
  Go's garbage collector.** `TestMaxHeapBytesSheds` proves an unreachably low
  heap ceiling sheds every request (not crash/hang), then shows a
  *separately configured* app with no ceiling serves normally — it does not
  (and, deterministically, cannot) prove that a live process's heap usage
  falls back under an artificially tiny ceiling within a bounded test.
- **The TLS handshake "flood" and the descriptor/connection ceilings are
  sized for CI speed (tens of concurrent connections), not lab-scale load.**
  They prove the configured timeouts/ceilings are real and enforced under
  concurrency, not resilience to an internet-scale DDoS, which is a
  network-layer concern beyond what a same-machine Go test can simulate.
- **Kernel transport paths (`cfg.Kernel`: io_uring/epoll-native/XDP),
  prefork, and the secure WASM/browser transport are entirely out of scope
  for this module.** Every test here goes through the standard
  `net.Listen`/`App.Serve`/`App.ServeTLS` path. `docs/production-readiness.md`
  calls out kernel-transport and secure-transport validation as their own,
  separate gates with their own platform/hardware dependencies (actual
  kernel/NIC/driver combinations, an independent cryptographic audit) — that
  work does not belong in a portable, CI-run Go test module and isn't
  attempted here.
- **Goroutine-count assertions are inherently process-wide.**
  `runtime.NumGoroutine()` (what `MaxGoroutines` samples, and what the soak
  test checks for leaks) counts every goroutine in the process, including
  this test module's own HTTP clients, since server and test client share
  one process here. Thresholds are chosen with deliberately generous
  headroom so the checks are meaningful without being flaky, but a
  dedicated goroutine-leak investigation against a separately-run server
  process would be more precise than anything an in-process test can offer.
- **The idempotency race test asserts the safe invariant, not a specific
  timing outcome.** `reliability.go`'s `Begin`/`Complete` split means the
  loser of a genuine concurrent race sees an in-flight-conflict response
  (409 by default), not an instant cached replay — the replay only appears
  for a request that arrives *after* the winner's `Complete` call. The test
  in `reliability_test.go` accepts either outcome for each racer (since real
  OS scheduling decides which happens) and separately, deterministically
  proves the steady-state replay guarantee once both requests have finished.
- **`TestTLSManyConcurrentLegitimateHandshakesSucceed` can fail under
  rapid, repeated back-to-back full-suite reruns on one already-loaded
  machine** (empirically: reliable in isolation and under normally-spaced
  runs; degraded — e.g. 11-17/30 handshakes succeeding instead of 30/30 —
  after several immediate repeats on a host also running other CPU-heavy
  work). This is not a dial-level failure `dialRetry`/`waitDialable` can
  absorb (`netstat` showed no ephemeral-port/TIME_WAIT pressure when it was
  reproduced) — it looks like genuine CPU/scheduling contention delaying
  real TLS handshake completion within the client's timeout, which a
  same-machine Go test can't fully insulate itself from. A single run, or
  runs spaced a few seconds apart — the realistic CI pattern — were
  reliable in testing. If this shows up in CI, it's worth first checking
  whether the runner was under unrelated concurrent load rather than
  assuming a regression.
