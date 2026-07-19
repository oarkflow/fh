# Prefork & Zero-Downtime Restarts

`app.ListenPrefork` runs a supervisor of multiple OS worker *processes*
bound to the same port via `SO_REUSEPORT`, instead of a single process. It
serves two related purposes with one mechanism:

- **Prefork**: real multi-core parallelism from separate OS processes, on top
  of (or instead of) the existing goroutine-level `SO_REUSEPORT` reactor
  sharding described in [Linux Kernel Transport](kernel-transport.md).
- **Zero-downtime restarts**: sending `SIGHUP` to the master process spawns a
  fresh generation of workers, waits for them to report a bound listener,
  then gracefully drains and terminates the previous generation — the port
  never stops accepting connections.

## Usage

Call it exactly where you would otherwise call `Listen` or
`ListenWithGracefulShutdown`:

```go
app := fh.New()
app.Get("/", handler)

log.Fatal(app.ListenPrefork(":8080"))
```

The calling binary re-executes itself once per worker (`os.Executable()` +
the original `os.Args`), so route registration in `main()` naturally runs
again in every worker process — there's no separate "build the app" callback
to wire up.

```go
log.Fatal(app.ListenPrefork(":8080",
    fh.WithPreforkWorkers(runtime.NumCPU()), // default
    fh.WithPreforkReadyTimeout(10*time.Second),
    fh.WithPreforkShutdownTimeout(30*time.Second),
))
```

## Zero-downtime rolling restart

```bash
# Deploy a new binary to the same path, then:
kill -HUP <master-pid>
```

The master spawns a full new generation of workers, waits for all of them to
report a bound listener (`ReadyTimeout`), swaps them in, then sends `SIGTERM`
to the previous generation — each worker's own graceful shutdown
(`ShutdownWithContext`, already used by `ListenWithGracefulShutdown`) drains
its in-flight requests within `ShutdownTimeout` before exiting. If the new
generation fails to spawn or become ready in time, the rollout is rolled
back automatically and the previous generation keeps serving — a rollout
failure never drops the previous generation.

`SIGINT`/`SIGTERM` to the master gracefully stops the whole supervisor
(current generation drained, then exit).

**Windows** has no `SIGHUP`. Call `app.Reload()` from within the running
master process instead — e.g. from an admin endpoint or an external
supervisor sending some other trigger — to get the identical rolling
restart. `Reload()` also works on Unix, for the same programmatic use case.

## Crash recovery

If a worker exits unexpectedly (not as part of a deliberate stop or
rollout), the master respawns it with exponential backoff
(`RestartBackoffMin` → `RestartBackoffMax`, doubling), to avoid crash-looping
a broken binary while still recovering from transient failures.

## Composing with kernel reactors

`ListenPrefork` requires the kernel-assisted transport (only it sets
`SO_REUSEPORT`, needed for multiple *processes* to bind the same port); it is
enabled automatically for workers if not already configured. Each worker
defaults to a single goroutine-level reactor (`WorkerReactors: 1`) — with N
worker processes already providing OS-level parallelism, stacking additional
per-process reactors by default would oversubscribe the machine. Override
with `fh.WithPreforkWorkerReactors(n)` for intentional two-level parallelism.

## Reference

| Type / Func | Description |
|---|---|
| `app.ListenPrefork(addr string, opts ...PreforkOption) error` | Master/worker entry point |
| `app.Reload() error` | Programmatic rolling restart (works on all platforms; required on Windows) |
| `fh.WithPreforkWorkers(n)` | Worker process count (default `runtime.NumCPU()`) |
| `fh.WithPreforkWorkerReactors(n)` | Per-worker `kernel.Reactors` override (default `1`) |
| `fh.WithPreforkReadyTimeout(d)` | Max wait for a worker to bind before startup/rollout fails (default `10s`) |
| `fh.WithPreforkShutdownTimeout(d)` | Max drain time before a worker is force-killed (default `30s`) |
| `fh.WithPreforkRestartBackoff(min, max)` | Crash-respawn backoff bounds (default `500ms`–`30s`) |

See [`examples/prefork`](../examples/prefork) for a runnable worker binary,
and `prefork_integration_test.go` for an end-to-end test that drives it with
real `SIGHUP`/`SIGTERM` and asserts zero dropped requests during a rollout.
