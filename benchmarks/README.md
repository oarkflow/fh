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

The output JSON contains request rate, average latency, p50/p95/p99 latency,
and errors. Benchmark numbers are machine- and kernel-dependent; compare
servers from the same run rather than treating them as universal rankings.
