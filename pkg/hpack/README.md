# hpack — High-Performance HPACK for Go

A complete, production-ready implementation of [RFC 7541](https://httpwg.org/specs/rfc7541.html)
(HPACK — HTTP/2 Header Compression), written as a drop-in replacement for
`golang.org/x/net/http2/hpack` with meaningful performance and safety improvements.

## Files

| File | Purpose |
|---|---|
| `hpack.go` | `Decoder`, `Encoder`, `HeaderField`, `DecoderPool`, varint codec |
| `tables.go` | Static table (RFC 7541 Appendix A), dynamic table with eviction |
| `huffman.go` | Huffman encoder (`AppendHuffmanString`) and decoder (`huffmanDecode`) |
| `hpack_test.go` | RFC compliance tests, edge cases, benchmarks, fuzz entry point |

---

## What's improved over `x/net/http2/hpack`

### 1 — Zero allocation on static-table hot path

One-byte indexed fields that map to static table entries use a dedicated hot path and
decode with **0 B/op, 0 allocs/op**.
The emit callback receives a `HeaderField` whose `Name`/`Value` strings point directly into
the static table's pre-interned backing — no copy, no heap allocation.

```
BenchmarkComparison/DecodeStatic/Custom   54M ops   22.1 ns/op   0 B/op   0 allocs/op
BenchmarkComparison/DecodeStatic/XNet     34M ops   34.9 ns/op   0 B/op   0 allocs/op
```

On an Apple M2 Pro with Go 1.25, the custom static-table path is about 36% lower-latency
than `golang.org/x/net/http2/hpack` v0.56.0. Run `go test ./pkg/hpack -run '^$'
-bench BenchmarkComparison -benchmem` to reproduce the comparison on your CPU.

### 2 — Pooled scratch buffers for Huffman decoding

Huffman decoding uses a `sync.Pool` of `[]byte` scratch buffers (`decodeBufPool`).
No `bytes.Buffer` is allocated per decode; instead we reuse `*[]byte` slices that
grow to working size and stay in the pool between calls.

### 3 — Flat trie, not pointer-chased tree

The Huffman decoder uses a 256-entry `*[256]*node` at every trie level.
Each decode step is a single array index — no linked-list traversal.
The tree is built once (via `sync.Once`) and thereafter read-only.

### 4 — 64-bit accumulation register in encoder

`AppendHuffmanString` accumulates bits in a `uint64` register, flushing a full
`uint32` (4 bytes) whenever ≥ 32 bits are valid. This minimises `append` calls
compared to byte-by-byte emission.

```
BenchmarkHuffmanEncode   27M ops   130 ns/op   0 B/op   0 allocs/op
BenchmarkHuffmanDecode   10M ops   336 ns/op   0 B/op   0 allocs/op
```

### 5 — `DecoderPool` for concurrent servers

```go
pool := hpack.NewDecoderPool(4096, 8192, 1<<20)

// Per-request handler:
d := pool.Get(func(hf hpack.HeaderField) {
    processHeader(hf)
})
_, err := d.Write(headerBlock)
err2 := d.Close()
pool.Put(d) // zero-alloc reuse
```

`DecoderPool` wraps `sync.Pool` and calls `Decoder.Reset(...)` to fully clear
state (dynamic table, saveBuf, counters) before returning a decoder to a caller.
No allocations on the hot path after pool warm-up.

```
BenchmarkDecoderPool   44M ops   84 ns/op   0 B/op   0 allocs/op
```

### 6 — `Decoder.Reset` for allocation-free reuse

Rather than allocating a new `Decoder` per connection/stream, callers can
`Reset` an existing one:

```go
d.Reset(maxDynTableSize, newEmitFunc)
```

This clears the dynamic table in-place (map `delete` rather than `make`),
resets the save buffer without reallocating, and zeros all counters.

### 7 — `SetMaxHeaderBytes` — header list size enforcement

Enforces `MAX_HEADER_LIST_SIZE` (RFC 9113 §4.6.1) at the decoder level:

```go
d.SetMaxHeaderBytes(1 << 20) // 1 MiB
```

Returns `ErrHeaderListSize` when the cumulative RFC-7541-§4.1 size of decoded
headers in a block exceeds the limit, while keeping the decoder state consistent.

### 8 — Strict RFC 7541 compliance

- Dynamic table size updates validated against `allowedMaxSize` (mirrors peer's SETTINGS).
- `ErrInvalidHuffman` for truncated or improperly padded Huffman strings.
- `errVarintOverflow` for varints that would exceed 63-bit range.
- `ErrStringLength` enforced before Huffman decoding begins (not just after).
- Sensitive header indexing prevention (`indexedNever` / `Sensitive: true`).
- `DecodingError` wraps all spec-violation errors for structured handling.

---

## API

### Decoder

```go
// Create
d := hpack.NewDecoder(4096, func(hf hpack.HeaderField) {
    fmt.Println(hf.Name, hf.Value)
})
d.SetMaxStringLength(8192)
d.SetMaxHeaderBytes(1 << 20)

// Feed data (streaming-safe; handles fragmented writes)
d.Write(wireBytes)

// End of header block
if err := d.Close(); err != nil { /* truncation or spec violation */ }

// Convenience: decode entire block at once
fields, err := d.DecodeFull(wireBytes)

// Reuse without allocation
d.Reset(newMaxSize, newEmitFunc)
```

### Encoder

```go
var buf bytes.Buffer
enc := hpack.NewEncoder(&buf)
enc.SetMaxDynamicTableSizeLimit(4096) // cap from peer SETTINGS

enc.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})
enc.WriteField(hpack.HeaderField{Name: "authorization", Value: "secret", Sensitive: true})
```

### Huffman utilities

```go
// Encode
dst = hpack.AppendHuffmanString(dst[:0], s)

// Decode
dst, err = hpack.HuffmanDecode(dst[:0], encoded)
s, err   = hpack.HuffmanDecodeToString(encoded)
n        := hpack.HuffmanEncodeLength(s) // predicted byte length
```

---

## Benchmarks

Run on Intel Xeon @ 2.80 GHz, Go 1.22, linux/amd64.

```
BenchmarkDecodeStaticHit      48M ops    74 ns/op    0 B/op   0 allocs/op
BenchmarkDecodeHuffmanLiteral  5.8M ops  636 ns/op   40 B/op  2 allocs/op  ← string copy only
BenchmarkEncodeTypical         7.2M ops  497 ns/op    0 B/op  0 allocs/op
BenchmarkHuffmanEncode         27M ops   130 ns/op    0 B/op  0 allocs/op
BenchmarkHuffmanDecode         10M ops   336 ns/op    0 B/op  0 allocs/op
BenchmarkDecoderPool           44M ops    84 ns/op    0 B/op  0 allocs/op
```

The 2 allocs in `BenchmarkDecodeHuffmanLiteral` are unavoidable: one for the
decoded string value (Huffman → `string`) and one for the emit closure capture.
In production, use `SetEmitEnabled(false)` or an emit-less path for headers
beyond `MAX_HEADER_LIST_SIZE` to avoid even those.

---

## Testing

```bash
# All tests
go test ./...

# Tests + race detector
go test -race ./...

# Benchmarks
go test -bench=. -benchmem ./...

# Fuzz (run for N seconds)
go test -fuzz=FuzzDecode -fuzztime=60s
```

---

## Package path

```
github.com/oarkflow/fh/pkg/hpack
```

Replace `golang.org/x/net/http2/hpack` imports with the above. The public API
is a strict superset: all upstream types and functions are present with identical
signatures; new APIs (`Reset`, `SetMaxHeaderBytes`, `DecoderPool`) are additive.
