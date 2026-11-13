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

The launcher builds the servers in a temporary directory and removes them on
exit, preserving checked-in or locally built binaries. The harness also accepts
`-bin-dir=/path/to/binaries` when launching a custom set of prebuilt servers.

Useful options:

```bash
./run.sh -duration=10s -warmup=2s -concurrency=64 -output=results/10s.json
```

Use `-fh-port`, `-fiber-port`, and `-fasthttp-port` to choose alternate server
ports when the defaults are occupied. Locally launched child servers bind to
loopback, and the harness passes each selected address through `FH_BENCH_ADDR`.

To keep client CPU and server CPU on separate machines, start all three server
binaries on the target host, bound to a server address reachable from the
benchmark client, then run the harness with `-nostart -host=<server-host>`:

```bash
cd benchmarks/servers/go
go build -o /tmp/fh-server ./fh
go build -o /tmp/fiber-server ./fiber
go build -o /tmp/fasthttp-server ./fasthttp
FH_BENCH_ADDR=10.0.0.12:3001 /tmp/fh-server &
fh_pid=$!
FH_BENCH_ADDR=10.0.0.12:3003 /tmp/fiber-server &
fiber_pid=$!
FH_BENCH_ADDR=10.0.0.12:3004 /tmp/fasthttp-server &
fasthttp_pid=$!
trap 'kill "$fh_pid" "$fiber_pid" "$fasthttp_pid"; wait' EXIT
wait
```

On the client host, run `go run main.go -nostart -host=10.0.0.12` from
`benchmarks`. The harness probes the remote ports, checks each server's runtime
stats endpoint, and records the target host in the result metadata. `-host`
accepts an unbracketed IPv4 address, IPv6 address, or hostname. Remote mode
cannot report server RSS because the harness does not own those processes; Go
heap, GC and CPU stats remain available through the benchmark endpoint. Keep
that endpoint reachable only from the benchmark client.

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
errors, Go allocated bytes per completed request, heap allocation before and
after the timed window, GC count delta, estimated Go CPU milliseconds per
request, and process RSS snapshots before and after the window. Runtime
statistics are fetched through an unmeasured `__bench/stats` route. RSS is
sampled with `ps` outside the measured window. The Go CPU estimate sums runtime
user, GC, and scavenger counters; it excludes kernel CPU and is not total
process CPU. If the runtime user CPU counter does not advance, the harness
reports Go CPU as unavailable rather than 0 ms/request. RSS is a point-in-time
reading, not peak memory or retained heap. RSS is unavailable with `-nostart`
because the harness does not own those server processes. Each row also carries
run metadata:
UTC start time, Go version, operating system, architecture, logical CPU count,
`GOMAXPROCS`, workload settings, server order, randomization seeds, selected
server/scenarios, `-nostart` mode, the fh write-buffer override, and the source
revision/dirty state when Go build information includes VCS data.
Each row also records the measured server binary's Go version, OS/architecture,
CPU settings, main module/version, and embedded VCS revision/dirty state. This
lets `-nostart` results reveal when the supplied server process is stale or was
built from a different revision.
Benchmark numbers are machine- and kernel-dependent; compare servers from the
same run rather than treating them as universal rankings.
The Go client, server processes, and percentile collection all share the
benchmark host's CPU and memory. A short run can therefore show transient
throughput dips unrelated to one handler's serialization cost.

For repeated sampling that is robust to those background bursts, use
interleaved rounds: `-rounds=N` (N > 1) reshuffles both server and scenario
order every round, measures every server once per round, and prints a
median-of-rounds table — each server is sampled inside the same time band, so
a multi-second background spike cannot penalize a whole server section
exclusively. `-nostart` assumes the servers are already running (for profiling
or when starting them manually with custom `GODEBUG` settings).
