# Cross-platform kernel-assisted transport

`fh` keeps HTTP/1, HTTP/2, WebSocket, TLS, routing, middleware, reliability and
application handlers in memory-safe Go. Socket creation, readiness/completion
notification and accept scheduling use the native operating-system networking
facility.

The implementation, platform tests, and XDP assets are contained in the
`kernel` package and directory. The root `fh` package keeps compatibility
aliases and the small `App` lifecycle bridge, so existing `fh.KernelConfig` and
`fh.WithKernel` callers do not need to change imports.

## Platform matrix

| Platform | Balanced `auto` backend | Throughput candidate | Implementation |
|---|---|---|---|
| Linux | `epoll` | probed `io_uring`, then `epoll` | Raw nonblocking sockets, sharded reactors, optional reuse-port BPF and XDP |
| macOS and BSD | `kqueue` | `kqueue` | Raw nonblocking sockets and edge-triggered kqueue reactors |
| Windows | `iocp` | `iocp` | Go runtime IOCP/overlapped poller with fh tuning and lifecycle |
| Solaris/illumos | `event_ports` | `event_ports` | Go runtime event-port poller |
| AIX | `pollset` | `pollset` | Go runtime pollset |
| Other server-capable targets | `native` | `native` | Native listener with fh lifecycle and admission limits |
| js/wasm, wasip1 | unavailable | unavailable | Inbound TCP server sockets are unavailable; startup fails explicitly |

An incompatible explicit backend is rejected. Runtime status reports the actual
backend; fallback never reports the requested accelerator as active.

## Profiles and production startup

```go
kernel := fh.ProductionKernelConfig() // Linux: mature epoll default
kernel.Required = true
app := fh.NewProduction(fh.WithKernel(kernel))
if err := app.ValidateKernelProduction(); err != nil { log.Fatal(err) }
log.Fatal(app.ListenWithGracefulShutdown(":8080"))
```

`fh.HighPerformanceKernelConfig()` is an aggressive benchmark candidate. On
Linux it permits io_uring auto-selection only after required-operation probing.
It is not a universal guarantee and must be compared with the balanced profile.

`KernelRuntimeInfo` reports active/peak connections, global/per-IP rejections,
socket-option failures, affinity, fallbacks and XDP state. `KernelReadiness`
separates errors, warnings and deployment information and always requires a
workload benchmark.

## Production hardening

- bounded connection admission and per-IP limits;
- configurable resource-pressure backoff;
- strict or best-effort socket tuning;
- no-delay, keepalive, user timeout and optional socket buffers;
- Linux cpuset-aware CPU affinity with restoration;
- immediate kernel wakeups for idle reactor shutdown;
- completion-safe io_uring buffer lifetime and shutdown wakeup;
- graceful request draining through the existing protocol engine.

Fixed socket buffers override OS autotuning. Busy polling is opt-in because it
consumes CPU while idle.

## XDP boundary

XDP is an optional Linux packet-admission layer for protected ports, source
blocking and coarse rate control. HTTP, TLS, authentication and application
logic remain in Go user space. Other operating systems use their own firewall
subsystems; fh does not claim portable XDP equivalents.

## Validation

```bash
go test ./...
CGO_ENABLED=0 go test -race ./kernel . -run 'Kernel'
go vet ./...
CGO_ENABLED=0 go test ./kernel -run '^$' -bench 'Kernel.*HTTPKeepAlive' -benchmem -count=5
```

Also run native-host integration, soak, overload, connection-churn, TLS,
shutdown and fault-injection tests on every deployed OS. Cross-compilation proves
source compatibility, not native kernel behavior.
