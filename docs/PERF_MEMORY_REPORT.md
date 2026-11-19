# Memory Footprint Reduction Report

Scope: allocation count/size and buffer lifecycle only (per the coordinated split with the
parallel CPU/routing effort). No public API signatures were changed, and no security
validation was weakened.

## Summary of findings

The codebase already applies most of the standard allocation-avoidance techniques
before this pass:

- `ctx.go` pools `DefaultCtx` via `sync.Pool` (`ctxPool`) and additionally special-cases
  HTTP/1 keep-alive connections to reuse one `*DefaultCtx` per connection
  (`acquireHTTP1Ctx`/`connState.ctx`) instead of round-tripping through the pool on every
  request.
- `pool.go` has size-classed `sync.Pool`s for read/scratch buffers (`bufPool512/4K/16K/64K`)
  and a shared response-body buffer pool (`bytesPool`), and `putBytes` already refuses to
  pool a buffer whose capacity grew past `maxPooledBytesCap` (1MB) — the classic
  "one big response permanently inflates the pool" bug is already guarded against for the
  *shared* pool.
- `RequestHeader` parses directly into fixed-size, zero-copy slices (`Header{Key,Value []byte}`
  pointing into the connection's read buffer) — no `map[string]string` per request, no
  per-header allocation.
- `mw/cache`, `mw/smartcache`, and `mw/coalesce` are all already bounded (`MaxEntries` +
  TTL-based eviction loops), so they were not a source of unbounded growth.
- `string(...)`/`[]byte(...)` conversions found in `codec.go` are all necessary copies out
  of a buffer that will be reused by the next request on the same connection (BodyParser
  binding target fields) — converting them to zero-copy would alias reused connection
  memory into long-lived application state, which is a correctness/security bug, not an
  optimization. Left as-is and flagged here rather than "fixed."

The one gap found was in **idle keep-alive connection memory** (mission item 4): two
per-connection buffers can grow to fit an oversized request/response and then stay at
that size for the rest of the connection's life, including while the connection sits
idle waiting for the next pipelined request. This is the same "slice retains huge backing
array" pattern `putBytes` already guards against for the shared pool, just not yet applied
to the two buffers that live directly on `connState` / the HTTP/1 read loop instead of
going through the shared pool.

## Changes made

### 1. `connState.writeBuf` shrink-back (app.go, serveConn)

`state.writeBuf` (the per-connection response assembly buffer, see the
`writeBufPooled` comment on `DefaultCtx`) is deliberately *not* returned to the shared
`bytesPool` on every request — that's what lets a busy keep-alive connection build every
response without a `sync.Pool` round trip. But nothing previously capped how large that
buffer could grow: one large response (a big JSON dump, an echoed upload) permanently
inflated `state.writeBuf`'s backing array, and every subsequent request on that connection
— including while the connection later sits idle — kept that oversized buffer alive.

Fix: right after `releaseCtx(ctx)` in the keep-alive loop, once the ctx has flushed its
final content into `state.writeBuf`, check `cap(state.writeBuf) > maxPooledBytesCap`
(the same 1MB ceiling `putBytes` uses) and reallocate a small (4KB) buffer if so.

```go
if state != nil && cap(state.writeBuf) > maxPooledBytesCap {
    state.writeBuf = make([]byte, 0, 4096)
}
```

**Pool-safety / correctness:** `state.writeBuf` is only ever touched by the single
goroutine serving that connection (HTTP/1 requests on one connection are strictly
serial), so there's no race. The reallocation happens after the buffer's current content
has already been written out to the socket (`writeAll` already completed inside the
response-writing methods before `releaseCtx`), so no in-flight data is lost. Because the
old backing array is simply dropped (not pooled), it carries no risk of leaking one
tenant's data into another connection's buffer the way returning it to a *shared* pool
without clearing could — this is strictly per-connection, so there's no cross-request or
cross-connection data exposure to reason about, only this connection's own now-stale
bytes, which are unreachable and GC'd normally.

### 2. Read buffer (`buf`/`accumulated`) shrink-back (app.go, serveConn)

Same pattern for the *read* side: `buf` starts as a size-classed buffer from `getBuf`
(`rawBuf`), but grows via a plain `make` when a request's header block or body exceeds
the pooled size class (`grown := make([]byte, newCap)` / `grown := make([]byte, messageEnd)`).
That grown array then becomes `buf` for the rest of the connection's keep-alive loop —
including idle time between requests — even after the oversized request that caused the
growth is long gone.

Fix: at the same keep-alive-continuation point, if `cap(buf) > cap(*rawBuf)` (i.e. it grew
past the pooled buffer this connection started with) and any pipelined leftover bytes
still fit inside the original pooled buffer, switch back to `*rawBuf` instead of carrying
the oversized array forward:

```go
if cap(buf) > cap(*rawBuf) {
    var leftoverLen int
    if chunkedBody {
        leftoverLen = len(nextData)
    } else if nextStart := bodyStart + bodyLen; nextStart < len(accumulated) {
        leftoverLen = len(accumulated) - nextStart
    }
    if leftoverLen <= cap(*rawBuf) {
        buf = *rawBuf
    }
}
```

**Pool-safety / correctness:** `rawBuf` (and thus `*rawBuf`'s backing array) is owned
exclusively by this connection's `serveConn` goroutine for the connection's whole
lifetime (it's `defer putBuf(rawBuf)`'d back to the shared pool only when the connection
closes), so reusing it mid-connection doesn't touch anything another goroutine can see.
Any pipelined bytes that must survive the swap are `copy`'d into the smaller buffer
*before* `accumulated` is repointed at it, so no data is dropped. If the leftover doesn't
fit, the code intentionally keeps the larger buffer for that one iteration rather than
truncating a real pipelined request — correctness always wins over shrinking. Because a
request's own header/body bytes are fully consumed (parsed into `ctx.Header`/`ctx.body`
and handed to the handler) before this check runs, and the old array is dropped rather
than pooled, there's no possibility of a later request on this connection — or any other
connection — reading stale header/body/`Authorization` bytes out of a reused buffer.

Both changes only activate once a buffer has already grown past its pooled/default size,
which is why they show up as zero cost on every benchmark below: none of the benchmarked
workloads exercise oversized requests/responses, so the new capacity check
(`cap(x) > threshold`) is the only added cost on the common path — a single integer
comparison per keep-alive iteration.

### 3. New regression test

Added `buffer_shrink_test.go`
(`TestKeepAliveWriteBufferShrinksAfterLargeResponse`) which drives a real TCP keep-alive
connection through a large response followed by a small one and asserts
`connState.writeBuf`'s capacity drops back to the pooled ceiling afterward. This guards
change #1 end-to-end (status line, headers, and body are all read back to make sure the
shrink doesn't corrupt the response). Change #2 (the read buffer) is exercised indirectly
by the existing HTTP/1 test suite (`fasthttp_test.go`, `http1_reuse_test.go`,
`protocol_test.go`) but isn't independently assertable from outside the package since
`buf` is a `serveConn`-local variable; its correctness is covered by reasoning above plus
`go test ./...` passing with no behavior change to any existing keep-alive/chunked/large-body
test.

## Benchmarks: baseline vs after

Full suite: `go test -bench=Benchmark -benchmem -run=NONE .` (13th Gen Intel i9-13900K,
`GOMAXPROCS=32`). All `B/op` and `allocs/op` values below are **identical** before and
after — expected, since both changes only trigger once a buffer has grown past its normal
size, which none of these benchmarks do. `ns/op` moved by generally <5% in either
direction (within run-to-run noise on this shared machine); no benchmark regressed by
more than that.

| Benchmark | B/op (base → after) | allocs/op (base → after) |
|---|---|---|
| BenchmarkRouterStatic | 248 → 248 | 12 → 12 |
| BenchmarkRouterParam | 232 → 232 | 11 → 11 |
| BenchmarkRouterWildcard | 240 → 240 | 11 → 11 |
| BenchmarkCtxAcquireRelease | 0 → 0 | 0 → 0 |
| BenchmarkCtxJSON | 0 → 0 | 0 → 0 |
| BenchmarkCtxSendString | 0 → 0 | 0 → 0 |
| BenchmarkCtxSendBytes | 0 → 0 | 0 → 0 |
| BenchmarkBufPool512/4K/16K/64K | 0 → 0 | 0 → 0 |
| BenchmarkFullRequestJSON | 561 → 561 | 13 → 13 |
| BenchmarkFullRequestPlaintext | 248 → 248 | 12 → 12 |
| BenchmarkFullRequestEcho | 224 → 224 | 11 → 11 |
| BenchmarkConcurrentRouting | 280 → 280 | 12 → 12 |
| BenchmarkConcurrentJSON | 611 → 611 | 14 → 14 |
| BenchmarkMiddlewareChainNoMiddleware | 224 → 224 | 11 → 11 |
| BenchmarkMiddlewareChain3Middleware | 224 → 224 | 11 → 11 |
| BenchmarkPrebuiltResponse | 224 → 224 | 11 → 11 |
| BenchmarkFH_HelloWorld | 216 → 216 | 10 → 10 |
| BenchmarkFH_ParallelRequests | 216 → 216 | 10 → 10 |
| BenchmarkFH_RouteWithParams | 224 → 224 | 11 → 11 |
| BenchmarkHelloWorld | 216 → 216 | 10 → 10 |
| BenchmarkParallelRequests | 217 → 217 | 10 → 10 |
| BenchmarkRouteWithParams | 224 → 224 | 11 → 11 |
| BenchmarkParseFormBytesSimple/Nested/Encoded | unchanged | unchanged |
| BenchmarkCodecJSONMarshal/Unmarshal/Encoder | unchanged | unchanged |
| BenchmarkH2ValidateRequestFields/ReadFrameSmallData/HandleWindowUpdate | unchanged | unchanged |
| (every other benchmark in the suite) | unchanged | unchanged |

A full diff of every benchmark line (`base_clean.txt` vs `after_clean.txt`, generated with
`awk '{print $1, $(NF-1), $NF}'` to isolate `B/op`/`allocs/op` regardless of `ns/op` noise)
produced **zero differences** — confirmed with `diff`, not eyeballed.

### Idle-connection memory impact (why this matters despite 0 B/op change above)

The benchmarks above measure steady-state per-request cost on *uniformly small*
request/response bodies, so they can't show the fix's actual effect, which is on the
*tail*: a connection that serves one oversized request/response and then goes idle.
Before this change, that connection pinned:

- up to `MaxRequestBodySize`/oversized-header worth of read-buffer capacity (unbounded by
  `ReadBufferSize`'s 16KB default — bounded only by the configured body/header ceilings,
  which are typically MBs), for its entire remaining keep-alive lifetime, and
- up to `maxPooledBytesCap` (1MB) or more of write-buffer capacity, same lifetime.

After this change, both drop back to ~4KB (or the original pooled size class) as soon as
the connection returns to idle, which is exactly the many-idle-keep-alive-connections
scenario this project is designed around. With, say, 50k idle connections behind a load
balancer where 1% of requests happen to be a large upload/response, this removes on the
order of tens to low hundreds of MB of otherwise-pinned RSS that would sit there until
each such connection eventually closed.

## Non-changes (flagged, not fixed)

- **`codec.go` `string(data)` copies in `BodyParser`/`QueryParser` binding** (lines ~684,
  690, 720, 1233): these copy out of a buffer the connection will reuse for the next
  request. Converting to zero-copy (`b2s`) would let a struct field alias memory that
  gets overwritten by (or exposes) a *different* request's data — including headers like
  `Authorization` if that buffer region is ever reused for headers — on the same
  connection. This is a security-relevant tradeoff (the kind the security-focused parallel
  effort is checking for), so it was left as a real copy rather than "optimized" away.
- **HTTP/2 `frameBuf`** (`http2.go`): also grows via `make` on a per-frame basis, but
  `h.localMaxFrame` is fixed at `h2DefaultFrame` (16KB) and is never raised by client
  SETTINGS (RFC 9113 §6.5.2 makes `SETTINGS_MAX_FRAME_SIZE` from the peer only affect what
  *we* may send, not what we must be willing to receive), so `frameBuf` cannot exceed 16KB
  in practice. No fix needed there.
- **`ReadBufferSize` default (16KB)**: considered shrinking the *initial* per-connection
  allocation, but `ReadBufferSize` is a documented, tested public config knob
  (`config_test.go` asserts the 16KB default) and `getBuf(a.cfg.ReadBufferSize)` uses it
  directly to pick the pool size class; decoupling the initial allocation from that value
  would change user-visible behavior for a modest win already covered by fix #2 above
  (which shrinks back down after any growth), so left unchanged.

## Verification

- `go build ./...` — passes.
- `go test ./...` — passes (see below).
- `go vet ./...` — clean.
- No `.prof` or scratch benchmark output files left in the tree.
