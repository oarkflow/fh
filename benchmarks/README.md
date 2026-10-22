# Apples-to-apples benchmark

This benchmark compares fh, Fiber, and fasthttp using the same six application
operations:

- plaintext response
- small JSON response
- path-parameter JSON response
- query-parameter JSON response
- JSON body echo
- 100-element JSON response

Each server binds only to `127.0.0.1`, disables response compression, uses a
16 KiB read buffer and a 4 MiB request-body limit, and uses the same response
shapes. The harness validates every route before measuring it, warms every
route on each server, randomizes server order with a reported seed, then uses
the same standard-library HTTP/1.1 keep-alive client pool,
duration, and concurrency for every server/scenario pair.

Run:

```bash
./run.sh
```

Useful options:

```bash
./run.sh -duration=10s -warmup=2s -concurrency=64 -output=results/10s.json
```

To isolate a suspected route and rotate scenario order between runs:

```bash
./run.sh -scenarios=json,params,query -duration=8s -warmup=2s -concurrency=64 -seed=17 -scenario-seed=5 -output=results/rotated.json
```

`-seed` changes server order; `-scenario-seed` changes scenario order within
each server. Both orders are printed. Repeat with different seeds and keep the
machine otherwise idle. For an fh CPU profile, set
`FH_BENCH_CPU_PROFILE=/tmp/fh-benchmark.pprof` and use `-only=fh`; inspect it
with `go tool pprof -top /tmp/fh-benchmark.pprof` after the run.
Set `FH_BENCH_WRITE_BUFFER_SIZE` to a positive byte count to compare an
explicit fh write-buffer size against its lazy default.

The output JSON contains request rate, average latency, p50/p95/p99 latency,
and errors. Benchmark numbers are machine- and kernel-dependent; compare
servers from the same run rather than treating them as universal rankings.
The Go client, server processes, and percentile collection all share the
benchmark host's CPU and memory. A short run can therefore show transient
throughput dips unrelated to one handler's serialization cost.
