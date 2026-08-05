# fh vs Fiber vs fasthttp — Validated Comparison

**Date:** 2026-08-05
**fh commit:** `248676b733f5d049b7e794212c8bff3822706c24` (2026-07-31, includes the merged security-hardening / CPU / memory-optimization changes)

This report re-validates the numbers already produced by the existing `benchmarks/` harness (`benchmarks/main.go`, `benchmarks/servers/go/*`) at fh's current HEAD, and adds process-level resource sampling (RSS/CPU) that the harness did not previously measure. No fh source (root package or `mw/`) was modified to produce these numbers — this is a pure measurement pass.

## Methodology

### Machine / toolchain

| | |
|---|---|
| CPU | 13th Gen Intel(R) Core(TM) i9-13900K |
| Cores | 32 (`nproc`) |
| RAM | 62 GiB total, ~49 GiB available at run time (`free -h`) |
| OS | Linux 6.8.0-79-generic x86_64 (Ubuntu, `uname -a`) |
| Go | go1.26.5 linux/amd64 |
| fasthttp | `github.com/valyala/fasthttp v1.72.0` |
| Fiber | `github.com/gofiber/fiber/v3 v3.4.0` (v3, chosen because it is Fiber's current line; noted so it isn't confused with v2 numbers elsewhere) |
| Load generator | [bombardier](https://github.com/codesenberg/bombardier) (already vendored at `~/go/bin/bombardier` by the existing harness) for all scenarios except the `/methods/*` matrix, which uses the harness's own persistent raw-HTTP/1.1 driver because bombardier rejects CONNECT/TRACE/QUERY |
| fh dependency isolation | fh, Fiber, and fasthttp only exist as dependencies inside `benchmarks/servers/go/go.mod` (a separate module with a `replace github.com/oarkflow/fh => ../../../`). The root `fh` module's `go.mod` was **not** touched — it has no fiber/fasthttp dependency. |

### Server configuration (apples-to-apples)

All three servers in `benchmarks/servers/go/{fh,fiber,fasthttp}/main.go` implement the identical scenario set: static plaintext GET, JSON GET, route-param GET, query-string GET, POST body echo (JSON parse+reserialize), a 100-element JSON array GET, and the full HTTP method matrix (GET/HEAD/POST/PUT/PATCH/DELETE/OPTIONS/CONNECT/TRACE/QUERY). All three use a 16 KiB read buffer, 4 MiB body limit, no Date/Server header, and no request/write/idle timeouts, so none of them wins or loses on I/O-buffer or timeout knobs. The fh server explicitly runs with `fh.WithDisableHTTP2(true)` and `fh.WithDisablePanicRecovery(true)` to match Fiber's and fasthttp's bare (no built-in panic-recovery, HTTP/1.1-only) configurations for this benchmark — this makes the throughput comparison fair, but it also means these numbers do **not** reflect fh running with its default safety net (panic recovery) or with HTTP/2 enabled. No logger/CORS/recover middleware chain was added on top for any server in this pass (the harness intentionally benchmarks bare handlers on all three; adding a middleware chain equally to all three would be a reasonable follow-up but wasn't done here).

A preflight gate (already part of `main.go`) fails the whole run if any two servers return non-identical bodies or content-types for a scenario, so every number below reflects genuinely equivalent responses, not accidentally-easier work by one server.

The main comparison ran with `-d 5 -c 100 -n 3` (5-second samples, 100 connections, 3 rotating rounds, median RPS reported), pinned via `taskset` so the servers (cores 0–15) and the load driver (cores 16–31) don't fight for the same cores.

### Framework parity caveats (read before trusting the table)

- **fasthttp is not a `net/http`-compatible server.** It has no built-in router, middleware system, HTTP/2 support, or `net/http` handler interface — the fasthttp "server" here is a hand-written `switch` on path/method. It is the fastest possible baseline for this workload precisely because it does the least work. fh and Fiber both provide a real router, middleware chain, and richer `Ctx` API on top of comparable throughput.
- **HTTP/2 is disabled on fh for this run.** fh supports HTTP/2; Fiber v3 and fasthttp here are HTTP/1.1-only for these benchmarks. Turning fh's HTTP/2 on would not directly apply here since bombardier speaks HTTP/1.1, but it's a capability gap in fasthttp/Fiber(v3 config used) worth noting the other direction.
- **fh's panic-recovery middleware was disabled** to match Fiber/fasthttp's out-of-the-box panic behavior (fasthttp has none; Fiber's is opt-in). Running fh with recovery enabled will show a small (single-digit percent, based on the router/middleware micro-benchmarks below) throughput cost that is not reflected in the head-to-head table.
- The `/methods/query` scenario is a QUERY HTTP method — Fiber v3 rejects registering a custom `QUERY` method, so Fiber legitimately errors on that one row (100/100 requests errored, 0 req/s) while fh and fasthttp complete it. This is a feature gap, not a bug in the harness.

## Results — throughput & latency (bombardier / raw-HTTP driver, 100 conns, median of 3×5s rounds)

| Scenario | Method | fh RPS | Fiber RPS | fasthttp RPS | fh P50/P99 (ms) | Fiber P50/P99 (ms) | fasthttp P50/P99 (ms) |
|---|---|---:|---:|---:|---:|---:|---:|
| Plaintext | GET | 1,231,433 | 1,193,168 | 1,221,217 | 0.067 / 0.251 | 0.067 / 0.273 | 0.067 / 0.255 |
| JSON | GET | 1,209,651 | 1,189,147 | 1,216,289 | 0.066 / 0.279 | 0.070 / 0.288 | 0.068 / 0.268 |
| Params | GET | 1,232,473 | 1,241,653 | 1,250,381 | 0.066 / 0.254 | 0.066 / 0.250 | 0.065 / 0.255 |
| Query string | GET | 1,187,881 | 1,166,116 | 1,190,810 | 0.068 / 0.281 | 0.070 / 0.303 | 0.071 / 0.276 |
| Echo (JSON body) | POST | 1,129,184 | 1,011,078 | 1,089,651 | 0.073 / 0.296 | 0.079 / 0.379 | 0.075 / 0.338 |
| Users array (100 objs) | GET | **601,377** | 501,124 | 511,938 | 0.133 / 0.559 | 0.139 / 0.978 | 0.139 / 0.918 |
| Method GET | GET | 1,483,506 | 1,418,768 | 1,426,127 | 0.058 / 0.180 | 0.061 / 0.190 | 0.060 / 0.186 |
| Method HEAD | HEAD | 1,489,411 | 1,409,726 | 1,392,253 | 0.058 / 0.180 | 0.061 / 0.191 | 0.060 / 0.188 |
| Method POST | POST | 1,491,552 | 1,403,608 | 1,431,334 | 0.058 / 0.179 | 0.061 / 0.192 | 0.060 / 0.187 |
| Method PUT | PUT | 1,486,823 | 1,403,946 | 1,435,562 | 0.057 / 0.181 | 0.061 / 0.192 | 0.060 / 0.187 |
| Method PATCH | PATCH | 1,465,988 | 1,398,394 | 1,433,267 | 0.058 / 0.182 | 0.061 / 0.193 | 0.060 / 0.186 |
| Method DELETE | DELETE | 1,483,406 | 1,407,062 | 1,433,008 | 0.058 / 0.180 | 0.061 / 0.190 | 0.060 / 0.186 |
| Method OPTIONS | OPTIONS | 1,482,751 | 1,395,255 | 1,432,655 | 0.058 / 0.182 | 0.061 / 0.189 | 0.060 / 0.187 |
| Method CONNECT | CONNECT | 1,488,742 | 1,407,018 | 1,426,817 | 0.058 / 0.180 | 0.061 / 0.189 | 0.060 / 0.190 |
| Method TRACE | TRACE | 1,455,693 | 1,401,003 | 1,428,010 | 0.059 / 0.186 | 0.061 / 0.192 | 0.060 / 0.188 |
| Method QUERY | QUERY | 1,480,382 | **0 (unsupported)** | 1,397,651 | 0.058 / 0.182 | n/a | 0.061 / 0.196 |

fh finished first (or tied within noise) in every scenario on this machine, mirroring the July 2026 run recorded in `benchmarks/RESULTS.md`, and margins are generally tighter now (all three servers sit within ~5% of each other on the simple GET scenarios) than that older run reported, most likely because this is a much bigger/faster 32-core machine where raw dispatch overhead matters less and Go's scheduler/GC dominate more evenly across all three. The one scenario with real separation is **Users array** (real JSON serialization of a 100-element slice), where fh leads fasthttp/Fiber by ~17–20%, and **Echo**, where fh leads by ~4–12%. Full raw output: `benchmarks/results/bench_20260805_211216.json` and the round-by-round console log is reproducible via `bash run.sh` (see Reproducing below).

## Results — resource usage (RSS / CPU)

Measured directly via `/proc/<pid>/status` (VmRSS) and `ps -o %cpu` sampled once per second, using freshly built binaries of each server (`go build` under `benchmarks/servers/go/{fh,fiber,fasthttp}`), independent of the bombardier-driven harness above.

### Idle

| Server | RSS, 0 connections | RSS, 1000 idle keep-alive connections | CPU% at 1000 idle conns |
|---|---:|---:|---:|
| fh | 8.4 MB | 29.2 MB | 3.3% |
| Fiber | 9.0 MB | 27.6 MB | 3.2% |
| fasthttp | 6.9 MB | 25.1 MB | 3.8% |

All three add roughly 18–21 MB of RSS to hold 1000 idle TCP connections open — essentially indistinguishable given the noise at this scale (a few MB either way is within run-to-run variance for a `go build` binary's heap).

### Under load (100 connections, 15s sustained load against `/json`, sampled every 1s)

| Server | Avg RSS during load | Peak RSS during load | CPU% at end of 15s window | Achieved RPS (same run) |
|---|---:|---:|---:|---:|
| fh | 29.4 MB | 31.8 MB | ~607% (≈6 cores) | 969,473 |
| Fiber | 29.9 MB | 31.1 MB | ~650% (≈6.5 cores) | 938,669 |
| fasthttp | 27.4 MB | 28.8 MB | ~645% (≈6.5 cores) | 935,731 |

Caveat on the CPU% column: `ps -o %cpu` reports a time-decayed average of CPU-seconds consumed over the process's wall-clock lifetime, not an instantaneous snapshot, and it can exceed 100% for a multi-threaded/multi-goroutine process (the server has access to cores 0–15, i.e. up to 1600%). Treat these as directionally comparable (all three processes were sampled with the identical protocol and timing) rather than precise absolute figures. fasthttp and Fiber's fractionally higher CPU% for slightly lower throughput than fh is consistent with fh doing marginally less work per request in this configuration, but the gap is small enough (≈6–8%) that it should not be over-read.

RSS differences between the three servers are minor (a few MB) at both idle and under load; none of the three shows runaway memory growth over the sampled windows. Raw per-second CSVs are not committed (temporary artifacts of this run) but the commands to reproduce them are in "Reproducing" below.

## Go-level micro-benchmarks (root `fh` module, same commit)

Run via `go test -bench=. -benchmem -run=NoTestsMatchThis .` in the repository root (no gofiber/fasthttp involved — these are fh-only, in-process, allocation-precise numbers; they exist for developers tracking fh's own regressions, not for cross-framework comparison). Full log: 2877 lines / 215.8s; selected highlights:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| RouterStatic | 269.8 | 32 | 2 |
| RouterParam | 267.9 | 16 | 1 |
| RouterWildcard | 322.2 | 24 | 1 |
| RouterLookupHighCardinality/Static/4096 routes | 16.80 | 0 | 0 |
| RouterLookupHighCardinality/Param/4096 routes | 39.55 | 0 | 0 |
| HeaderPeek | 4.318 | 0 | 0 |
| CtxAcquireRelease | 22.26 | 0 | 0 |
| CtxJSON | 146.8 | 0 | 0 |
| CtxSendString | 36.66 | 0 | 0 |
| FullRequestPlaintext (parse+route+respond) | 225.0 | 32 | 2 |
| FullRequestJSON | 403.9 | 342 | 3 |
| FullRequestEcho | 323.5 | 5 | 1 |
| MiddlewareChainNoMiddleware | 202.8 | 5 | 1 |
| MiddlewareChain3Middleware | 213.7 | 5 | 1 |
| GzipCompression | 131,944 (≈132µs) | 1,207,076 | 19 |
| GzipDecompression | 6,316 (≈6.3µs) | 41,405 | 6 |
| HPACKEncode / Decode | 37.95 / 23.08 | 0 / 112 | 0 / 1 |

The router and Ctx paths remain zero-allocation on hot lookups (static/param routing, header peek, buffer pool acquire/release), and a 3-middleware chain adds ~11ns (~5%) over no middleware with no extra allocations — consistent with fh's design intent of near-zero middleware tax.

## Summary — where fh wins, loses, or ties

**Wins:**
- fh led (or tied within run-to-run noise) on all 16 head-to-head scenarios against Fiber v3 and fasthttp on this 32-core machine.
- The clearest win is real JSON array serialization (Users scenario): fh is ~17–20% ahead of both Fiber and fasthttp, and also has visibly tighter tail latency there (P99 0.56ms vs 0.92–0.98ms) — this is the workload that exercises actual work per request rather than dispatch overhead, so it's the most meaningful result in the set.
- fh is the only one of the three that supports the non-standard QUERY HTTP method the way fasthttp does; Fiber v3 cannot register it at all.
- fh's router/Ctx micro-benchmarks confirm zero-allocation hot paths and a negligible (~5%) middleware tax, matching the throughput story from the load tests.

**Ties (essentially, within noise):**
- Plaintext, JSON, Params, Query, and every simple method-dispatch scenario cluster within roughly 2–6% of each other across all three servers. On this hardware, dispatch overhead is small enough relative to raw syscall/TCP cost that none of the three has a decisive structural edge for trivial handlers.
- Idle and under-load RSS are indistinguishable across the three (single-digit-MB differences) at 100–1000 connections; none of the three has a memory efficiency problem relative to the others at this scale.

**Losses / things to watch:**
- fh did not lose any scenario outright in this run, but the fasthttp comparison is somewhat apples-to-oranges in fh's favor structurally: fasthttp has no router, no middleware system, and no HTTP/2, so a lot of what fh spends cycles on (routing, `Ctx` lifecycle, HTTP/2 capability even when disabled here) is work fasthttp simply never does. That fh still keeps pace with (and often beats) fasthttp on this workload is a good sign, but a workload with deep route trees, wildcard matching, or many registered middlewares would be a fairer stress test of that overhead and wasn't included in this pass.
- The numbers here reflect fh with HTTP/2 and panic-recovery **disabled**. Production fh deployments that leave those on will see somewhat lower throughput than shown here; the Go-level benchmarks suggest the middleware/recovery tax itself is small (~5%), but this wasn't independently re-measured with recovery re-enabled in this pass.
- CPU% under load was measured with a coarse, lifetime-averaged metric (`ps %cpu`); a perf/cgroup-based instantaneous measurement would be more rigorous if resource efficiency under load becomes a specific area of investigation.

**Suggestion (not implemented — measurement only, per instructions):** the Users-array (JSON serialization) win suggests fh's JSON encode path is a genuine structural advantage; it would be worth profiling to see if the same technique generalizes to larger payloads, and worth adding a "deep route tree + N middlewares" scenario to this harness to specifically stress the parts of fh that fasthttp doesn't have to pay for at all.

## Reproducing

```bash
cd benchmarks
bash run.sh -d 5 -c 100 -n 3                 # throughput/latency table (fh, fiber, fasthttp)
go test -bench=. -benchmem -run=NoTestsMatchThis .   # from repo root — Go-level micro-benchmarks
```

Resource sampling (RSS/CPU) was done ad hoc for this report by building the three server binaries directly (`go build -o <bin> ./fh|./fiber|./fasthttp` under `benchmarks/servers/go`), starting each, sampling `/proc/<pid>/status` VmRSS and `ps -o %cpu` once per second while opening 1000 idle keep-alive sockets (Python `socket.create_connection` loop) and then while running `bombardier -c 100 -d 15s http://127.0.0.1:<port>/json` — this is not (yet) wired into `run.sh`; it can be added as a follow-up if resource tracking should become a standard part of the harness.
