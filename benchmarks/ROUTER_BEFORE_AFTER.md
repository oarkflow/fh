# Router Before/After Benchmarks

This report compares the router at committed baseline `5e6a1da664c4` with the
current working tree. Both sides used the exact benchmark implementation in
`router_operations_bench_test.go`.

Environment: Apple M2 Pro, Darwin/arm64, Go 1.26.5. Each benchmark ran for at
least 200 ms, three times. The table reports the median `ns/op`; lower is
better. Method and route-shape lookups use 256 registered routes.

```sh
go test -run '^$' -bench '^BenchmarkRouterOperations$' \
  -benchmem -benchtime=200ms -count=3
```

## Canonical method dispatch

| Operation | Before | After | Speedup | After allocs |
|---|---:|---:|---:|---:|
| GET | 185.0 ns | 11.40 ns | 16.2x | 0 |
| HEAD | 182.0 ns | 12.29 ns | 14.8x | 0 |
| POST | 189.1 ns | 11.34 ns | 16.7x | 0 |
| PUT | 183.5 ns | 11.53 ns | 15.9x | 0 |
| PATCH | 183.9 ns | 11.43 ns | 16.1x | 0 |
| DELETE | 205.8 ns | 11.43 ns | 18.0x | 0 |
| CONNECT | 190.2 ns | 11.85 ns | 16.1x | 0 |
| OPTIONS | 189.0 ns | 11.89 ns | 15.9x | 0 |
| TRACE | 190.5 ns | 11.51 ns | 16.6x | 0 |
| QUERY | 190.1 ns | 11.54 ns | 16.5x | 0 |

## Matching operations

| Operation | Before | After | Speedup | After allocs |
|---|---:|---:|---:|---:|
| Static hit | 185.5 ns | 11.63 ns | 16.0x | 0 |
| Parameter hit | 622.0 ns | 30.36 ns | 20.5x | 0 |
| Multi-parameter hit | 466.2 ns | 50.80 ns | 9.2x | 0 |
| Wildcard hit | 446.5 ns | 34.42 ns | 13.0x | 0 |
| Explicit HEAD | 7.674 ns | 7.458 ns | 1.03x | 0 |
| HEAD-to-GET fallback | 681.4 ns | 58.63 ns | 11.6x | 0 |
| Custom method | 40.49 ns | 39.76 ns | 1.02x | 0 |
| Static miss | 435.8 ns | 22.85 ns | 19.1x | 0 |
| Parameter miss | 1,128 ns | 34.85 ns | 32.4x | 0 |

## Router utilities and construction

Utilities were measured again after dedicated optimization, using five runs of
at least 300 ms. Registration used five one-second runs and prebuilt route
strings so formatting work is not charged to the router. The registration case
uses the common two-segment `/resource-<n>/:id` shape. For this section,
"Before" is commit `d57d82c1095c`, immediately before the utility-specific
changes; the earlier matcher tables continue to use baseline `5e6a1da664c4`.

| Operation | Before | After | Speedup | Before allocation profile | After allocation profile |
|---|---:|---:|---:|---:|---:|
| Allowed | 369.3 ns | 242.6 ns | 1.52x | 160 B, 2 allocs | 128 B, 1 alloc |
| Methods | 188.0 ns | 46.68 ns | 4.03x | 96 B, 1 alloc | 80 B, 1 alloc |
| Named URL | 369.3 ns | 209.4 ns | 1.76x | 200 B, 7 allocs | 64 B, 1 alloc |
| Compiled pattern | 63.89 ns | 41.79 ns | 1.53x | 8 B, 2 allocs | 0 B, 0 allocs |
| Register 256 routes | 90.29 us | 87.12 us | 1.04x | 139,248 B, 1,846 allocs | 116,336 B, 1,842 allocs |

## Functional verification

Performance benchmarks cannot validate behavior that was incorrect at the
baseline. Dedicated regression tests additionally verify:

- static routes retain precedence with more than eight registered routes;
- `HEAD` falls back to the matching `GET` route on a per-path basis;
- query strings are ignored for root, static, parameter, and wildcard routes;
- a frozen 512-route table is safe under concurrent lookup and the race detector.

Run the functional and concurrency checks with:

```sh
go test . -run 'TestRouter|TestFrozenRouter' -count=1
go test -race . -run 'TestRouter|TestFrozenRouter' -count=1
```
