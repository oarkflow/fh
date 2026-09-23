# REF versus traditional FH: parity benchmark report

## Summary

The earlier REF performance claim came from a mismatched benchmark: it compared a full FH HTTP handler against direct REF engine dispatch. That was not a valid HTTP comparison. Its parallel FH case also reused a consumed request body.

This report compares two routes on the same FH server. Both receive the same JSON, authenticate the same bearer token, calculate the same result, and return identical JSON and status. The REF route additionally builds an immutable invocation, schedules a compiled graph, stores typed facts, and projects the outcome through the REF HTTP adapter.

Results vary by workload. Plain FH is faster for a tiny CPU-bound endpoint. REF is faster where three independent 2 ms waits can overlap. Under 32-client CPU-only loopback load, REF remains much slower; that is a real optimization target and the old blanket speed claim is removed.

## Traditional patterns and REF mechanisms

| Common pattern | Cost or risk | REF mechanism | Tradeoff |
|---|---|---|---|
| Linear middleware and handler sequence | Independent work waits in sequence | Compiled DAG launches independent safe nodes together | Helps when independent I/O dominates; adds scheduling cost for tiny CPU-only work |
| Mutable string-keyed context | Misspelled keys and untyped values fail at runtime | Typed fact keys, declared requirements, compile-time dependency checks | Better dataflow guarantees; facts and scheduler add runtime work |
| Business logic inside HTTP handlers | Reuse by CLI, queue, WebSocket, or gRPC needs transport glue | Typed intents and transport adapters | Adapter creates invocation metadata and projects output; not zero-cost |
| Writes mixed into handler execution | Partial failures and retries need custom recovery | Effect plans and explicit commit/delivery stages | Durability costs time; guarantee depends on configured effect store |
| Handwritten speculative concurrency | Unsafe reads can run before identity/policy checks | Speculation classes and compile-time dependency checks | Only safe operations should be declared speculative |
| Implicit policy state | Access decisions and constraints can be hard to inspect | Explicit decision nodes and deny-dominant gates | More visible security behavior, not a faster replacement for a trivial conditional |

These are tradeoffs, not claims that conventional handlers cannot use goroutines or typed domain layers.

## Apples-to-apples setup

Code: compare_bench_test.go, compare_tcp_load_test.go, and transport/http/adapter.go.

- Both variants use fh.NewFast() and the same FH HTTP server/router.
- Authentication validation and the business operation are shared functions called by both implementations. The REF variant adds only the adapter, immutable invocation, compiled intent graph, typed fact lookup, and outcome projection.
- Both routes use the same method, path, payload, headers, JSON response type, status, HTTP/1 client implementation, keep-alive policy, and warmup count. The parity test checks equivalent decoded response values before timings are collected.
- Same POST path, JSON body, content type, bearer token, fixed request ID, auth identity, operation, JSON response, and 200 status.
- Plain FH does authentication, JSON decode, and response in a traditional handler. REF runs the equivalent auth capability, intent decode, operation, and HTTP response projection.
- Engine compilation and listener startup are outside measured intervals.
- Loopback test: reused Go HTTP client with keep-alive; 100 warm-ups, then 2 seconds at 1 and 32 concurrent clients.
- Overlap test: three independent read-like operations, each modeled by a 2 ms wait. FH waits serially; REF declares them as independent pre-auth-safe capability nodes. Synthetic wait only; it does not model a specific database or service.
- Host: Linux amd64, Intel Core i9-13900K, Go 1.27.1. In-process benchmark ran -cpu=1, 1 second per sample, three samples.

The TCP test is local HTTP/1 loopback, not remote-client, TLS, HTTP/2, database, or production capacity testing. The net.Pipe microbenchmark closes each connection per request, so it is a diagnostic for the complete one-request path and not the headline application throughput result. The TCP load test uses keep-alive and is the representative HTTP result here. Fiber and standalone fasthttp were not benchmarked; using FH for both routes isolates REF application-layer overhead from server-framework differences.

## Results

### CPU-bound endpoint: in-process benchmark

Median of three samples using go test -run '^$' -bench '^(BenchmarkHTTPParity|BenchmarkHTTPDependencyOverlap)$' -benchmem -benchtime=1s -count=3 -cpu=1 ./:

| Variant | Median latency | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| FH traditional | 10.877 µs | 22,698 B | 64 |
| FH with REF | 16.526 µs | 22,094 B | 138 |

REF is about 52% slower here and uses 2.16x allocations. Bytes/op were slightly lower in this harness, but allocation count was higher. This is the current cost when no independent blocking work can be hidden.

### Latency-bound endpoint: in-process benchmark

| Variant | Median latency | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| FH sequential: three 2 ms waits | 6.918 ms | 19,081 B | 64 |
| REF DAG: same waits overlap | 2.515 ms | 22,603 B | 147 |

REF cuts median latency by about 63.6% (2.75x lower latency), because independent waits run concurrently. It uses more temporary allocations.

### Loopback HTTP/1 load: CPU-bound endpoint

| Clients | Variant | Throughput | p50 | p95 | p99 | Errors |
|---:|---|---:|---:|---:|---:|---:|
| 1 | FH traditional | 6,443 req/s | 92 µs | 412 µs | 491 µs | 0 |
| 1 | FH with REF | 3,279 req/s | 227 µs | 586 µs | 690 µs | 0 |
| 32 | FH traditional | 331,055 req/s | 42 µs | 302 µs | 700 µs | 0 |
| 32 | FH with REF | 10,555 req/s | 2.65 ms | 6.87 ms | 9.21 ms | 0 |

This run does not support claiming REF is faster for a trivial request. It exposes a large high-concurrency dispatch/scheduling cost. The adapter allocation fix below does not close that gap.

### Independent 2 ms reads: in-process stress/load

| Clients | Variant | Throughput | p50 | p95 | p99 | Errors |
|---:|---|---:|---:|---:|---:|---:|
| 1 | FH sequential | 143 req/s | 6.97 ms | 7.20 ms | 7.37 ms | 0 |
| 1 | REF overlap | 368 req/s | 2.69 ms | 2.92 ms | 3.37 ms | 0 |
| 16 | FH sequential | 2,003 req/s | 8.01 ms | 8.93 ms | 9.28 ms | 0 |
| 16 | REF overlap | 5,281 req/s | 2.95 ms | 4.22 ms | 4.94 ms | 0 |

REF raised throughput about 2.6x in this I/O-bound case. These load results use net.Pipe, not TCP.

### Follow-up verification (2026-09-23)

Reran the same three-sample CPU benchmark after testing a single-root inline scheduler path. The current multi-root HTTP graph does not qualify for that path, so the experiment did not change the HTTP benchmark and was removed to preserve scheduler behavior. This confirms the existing target remains unresolved; it is not evidence of a speedup.

| Variant | Median latency | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| FH traditional | 12.505 µs | 23,414 B | 64 |
| FH with REF | 17.770 µs | 22,095 B | 138 |

A fresh 2-second TCP loopback run completed with zero failures:

| Clients | Variant | Throughput | p50 | p95 | p99 | Errors |
|---:|---|---:|---:|---:|---:|---:|
| 1 | FH traditional | 6,957 req/s | 92 µs | 397 µs | 497 µs | 0 |
| 1 | FH with REF | 3,125 req/s | 292 µs | 597 µs | 735 µs | 0 |
| 32 | FH traditional | 313,710 req/s | 43 µs | 324 µs | 737 µs | 0 |
| 32 | FH with REF | 10,760 req/s | 2.61 ms | 6.82 ms | 9.14 ms | 0 |

Parity tests and the full go test ./... suite pass. The high-concurrency CPU-bound gap persists. No scheduler optimization is retained because the tested fast path either failed short-circuit semantics for concurrent roots or did not apply to the benchmark plan.

A rerun after sharing the exact auth validator and business function across both variants measured:

| Clients | Variant | Throughput | p50 | p95 | p99 | Errors |
|---:|---|---:|---:|---:|---:|---:|
| 1 | FH traditional | 5,941 req/s | 95 µs | 435 µs | 527 µs | 0 |
| 1 | FH with REF | 3,325 req/s | 247 µs | 615 µs | 745 µs | 0 |
| 32 | FH traditional | 319,052 req/s | 44 µs | 313 µs | 735 µs | 0 |
| 32 | FH with REF | 11,963 req/s | 2.28 ms | 6.23 ms | 8.66 ms | 0 |

This is the current primary apples-to-apples load result: same FH server and HTTP client, persistent TCP connections, equivalent auth and business behavior, and zero failed responses. REF throughput is about 44% lower at one client and 96% lower at 32 clients in this CPU-bound workload.

## Code changes

1. Replaced the invalid direct-dispatch-vs-HTTP benchmark with matched FH traditional-handler and REF-adapter endpoints; both share the same auth validator and application operation.
2. Added exact status/body parity tests for CPU-only and independent-read variants.
3. Added a persistent in-process FH server over net.Pipe; repeated App.Test calls had started and shut down a server per request and were not a valid load driver.
4. Added opt-in TCP load testing with keep-alive, warm-up, concurrency tiers, throughput, and p50/p95/p99 reporting.
5. Added the synthetic I/O-overlap benchmark and load test.
6. Profiled the high-concurrency REF path; the profile showed significant runtime GC and scheduler time.
7. Updated the REF HTTP adapter to pass BodyRaw into immutable NewInput, avoiding a redundant body copy, and to skip parsing an empty query string. Parity tests pass after the change.
8. Removed the old misleading microbenchmark and replaced unsupported speed claims in the REF README.

## Reproduce

Run from the ref module directory:

~~~sh
go test -run '^(TestHTTPParity|TestHTTPDependencyOverlapParity)$' -count=1 ./
go test -run '^$' -bench '^(BenchmarkHTTPParity|BenchmarkHTTPDependencyOverlap)$' -benchmem -benchtime=1s -count=3 -cpu=1 ./
FH_LOAD=1 FH_LOAD_DURATION=2s go test -run '^(TestHTTPParityLoad|TestHTTPDependencyOverlapLoad)$' -count=1 -v ./
FH_TCP_LOAD=1 FH_LOAD_DURATION=5s go test -run '^TestHTTPParityTCPLoad$' -count=1 -v ./
~~~

## Allocation reduction follow-up (2026-09-23)

A direct compiled-engine dispatch benchmark (no HTTP server/client) measured the REF scheduler and intent path at 1,104 B/op and 26 allocs/op before this change. The scheduler no longer allocates an empty effects backing array for effect-free requests, reuses a fixed local ready-node buffer while propagating readiness, and creates the cancellation channel only if a node requests Context.Done(). Concurrent independent nodes still run concurrently; Done() remains cancellation-safe.

| Measurement | Before | After | Change |
|---|---:|---:|---:|
| Direct REF dispatch bytes/op | 1,104 B | 928 B | -176 B (-15.9%) |
| Direct REF dispatch allocs/op | 26 | 23 | -3 (-11.5%) |
| Direct REF dispatch latency | 3.95–4.69 µs | 3.91–4.12 µs | within run variation |
| FH + REF HTTP allocs/op | 138 | 135 | -3 |

Direct benchmark command: go test -run '^$' -bench '^BenchmarkREFDispatch$' -benchmem -benchtime=2s -count=3 -cpu=1 ./. Full REF test suite passes. The remaining allocations include per-node goroutine scheduling, execution contexts, and boxed typed facts; reducing those further needs scheduler/fact-storage changes that preserve cancellation and concurrency semantics.

## Remaining optimization target

REF is an application execution layer, not an HTTP server replacement. FH handles HTTP in both variants; REF adds decoding, metadata capture, graph scheduling, fact storage, and result projection. The small CPU-only endpoint shows this overhead plainly. The next performance work should reduce scheduler allocations and small-plan dispatch cost, then rerun this same keep-alive TCP parity load test. These measurements are a baseline, not a production capacity guarantee.
