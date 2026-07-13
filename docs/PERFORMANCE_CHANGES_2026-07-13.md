# HTTP/1 hot-path changes (2026-07-13)

This revision targets the ordinary `Ctx` API used by the cross-framework suite;
it does not replace benchmark handlers with static/prebuilt responses.

## Changes

- HTTP/1 request contexts and response assembly buffers now belong to the
  connection and are reused throughout keep-alive. HTTP/1 is serial within a
  connection, so this safely removes both context and response-buffer
  `sync.Pool` acquire/release pairs from every request while preserving the
  normal `Ctx` APIs. HTTP/2 continues using independently pooled stream contexts.
- `ModeFast` omits per-request graceful-shutdown activity atomics. The fast
  profile closes its connections when shutdown starts; production, enterprise,
  and strict profiles retain graceful in-flight request tracking.
- Direct 200 responses for `map[string]string`, `map[string]any`, and `fh.Map`
  now encode JSON after reserved header space in the connection buffer. The
  response header is backfilled after encoding, eliminating the JSON-buffer
  pool round trip and the body copy.
- The direct JSON header is assembled inside the connection buffer. This
  removed a compiler-visible 256-byte heap escape; `BenchmarkCtxJSON` moved
  from 256 B / 1 allocation to 0 B / 0 allocations.
- Common small keep-alive text and JSON responses reuse immutable status,
  content-type and content-length header blocks built at process startup.
- ModeFast connection contexts reset logical state without clearing bounded
  header/local slots that can only reference the same connection. Production
  and pooled contexts retain reference-clearing semantics.
- The earlier dispatch/parser experiment was reverted after target-machine
  results showed no improvement.
- Error handling, automatic status, HEAD, keep-alive, dynamic routing, and all
  production-mode shutdown semantics are preserved.

## Verification

Run the complete suite on the target machine:

```bash
cd benchmarks
bash run.sh -c 100 -d 10 -n 5 \
  --server fh --server fiber --server fasthttp
```

Use the median of at least five rotating rounds. A throughput lead cannot be
guaranteed across Go versions, kernels, CPU power states, or benchmark tools;
publish the exact environment with results. The preflight response-equivalence
gate must pass before accepting a run.

## Final sandbox sanity run

Environment: Go 1.26.0, linux/amd64, AMD EPYC 9V74, 100 connections,
Bombardier latency mode, five rotating one-second samples. These short samples
are a regression gate, not publication-grade capacity numbers.

| Scenario | fh median req/s | Fiber | fasthttp | fh vs best peer |
|---|---:|---:|---:|---:|
| Plaintext | 353,834 | 316,836 | 315,338 | +11.7% |
| JSON | 317,960 | 284,617 | 287,511 | +10.6% |
| Params | 322,088 | 308,982 | 319,205 | +0.9% |
| Query | 305,204 | 292,520 | 284,463 | +4.3% |

The direct JSON microbenchmark is 0 B/op and 0 allocs/op. Full `go test ./...`,
`go test -race ./...`, and `go vet ./...` pass under the same toolchain.
