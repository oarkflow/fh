# fh Production Kernel Transport Hardening Report

## Readiness conclusion

This revision is a substantially hardened, production-oriented kernel-assisted
networking implementation. It is not honest to certify any HTTP server as
universally "the fastest" or production-ready for every operating system,
kernel, NIC, workload and deployment topology without native-host load, soak,
fault-injection and security validation.

The delivered code is therefore classified as a **production candidate with
fail-safe defaults and deployment preflight**, not as an unconditional
performance or operational certification.

## Implemented hardening

### Safe backend policy

- `ProductionKernelConfig()` provides a balanced production profile.
- Linux balanced `auto` selects mature epoll without probing or automatically
  activating the custom io_uring path.
- `HighPerformanceKernelConfig()` permits probed io_uring selection on Linux as
  a benchmark/canary candidate.
- An explicit required io_uring request fails clearly when required operations
  are unavailable.
- Fallback runtime reporting records `standard` as the actual backend rather
  than claiming that the requested accelerator started.

### Lifecycle and shutdown

- Linux epoll and io_uring reactors use explicit nonblocking kernel wake pipes,
  removing one-second idle shutdown polling.
- Reactor resource cleanup is idempotent.
- Linux thread affinity respects the process affinity mask/cgroup cpuset.
- Original thread affinity is restored before returning the OS thread to the Go
  scheduler.
- io_uring connection close performs socket shutdown before close to wake
  pending receive/send operations.
- io_uring caller buffers remain live until completion processing finishes.

### Admission and overload behavior

- Runtime counters now include active and peak connections, global and per-IP
  rejections, and socket-option failures.
- epoll, kqueue, runtime-native listeners and io_uring accept paths use bounded,
  configurable exponential backoff under descriptor or memory pressure.
- Existing global and per-IP connection limits remain enforced before TLS and
  HTTP parsing.

### Socket configuration

- Portable and Linux raw-socket paths support no-delay, keepalive, optional
  receive/send buffers and strict or best-effort tuning.
- Linux raw sockets additionally support user timeout, deferred accept, Fast
  Open and optional busy polling.
- Tuning errors are counted. `StrictSocketOptions` converts them into startup or
  accept errors rather than silently ignoring them.
- Fixed socket buffers remain opt-in so OS autotuning is preserved by default.

### Production preflight

`KernelReadiness()` and `ValidateKernelProduction()` detect or report:

- disabled kernel mode;
- unbounded global connections;
- absent header/body/TLS/idle timeouts;
- disabled panic recovery;
- benchmark-only fast mode;
- invalid multi-reactor/reuse-port configuration;
- standard-listener fallback;
- required XDP not attached;
- strict socket-option failures;
- io_uring canary requirements;
- busy-poll and fixed-buffer operational risks.

The readiness report always sets `RequiresWorkloadBenchmark=true` because
configuration alone cannot establish the fastest backend.

## Platform implementation

| Platform | Implementation |
|---|---|
| Linux | Raw nonblocking listeners; sharded epoll; optional direct io_uring receive/send/linked timeouts; reuse-port BPF; optional XDP |
| macOS/FreeBSD/OpenBSD/NetBSD/DragonFly | Raw nonblocking listeners and edge-triggered kqueue accept reactors |
| Windows | IOCP/overlapped network poller supplied by the Go runtime, integrated with fh tuning, limits and lifecycle |
| Solaris/illumos | Go runtime event-port poller integrated with fh |
| AIX | Go runtime pollset integrated with fh |
| Other server-capable targets | Functional native listener path |
| Browser WASM/WASI | Explicit unsupported error because inbound TCP server sockets are unavailable |

This is kernel-assisted networking. Arbitrary Go handlers, TLS policy and HTTP
business logic correctly remain in user space. XDP is Linux-only and is not
misrepresented as available on other systems.

## Validation completed in this environment

The project declares Go 1.26.5. The runner provides Go 1.23.2 and cannot fetch
that toolchain, so validation used a temporary compatibility copy only. The
source artifact retains Go 1.26.5 and its secure `os.Root` implementation.

Completed:

- focused Linux epoll/reuse-port HTTP integration tests;
- io_uring ABI, linked-timeout and completion-lifetime tests;
- balanced-auto epoll selection test;
- process-affinity/cpuset selection test;
- production-readiness tests;
- targeted race-detector run;
- root-package `go vet`;
- kernel CLI and example builds;
- keep-alive epoll benchmark smoke run;
- root package suite, with one expected compatibility-only failure because Go
  1.23 cannot reproduce Go 1.26 `os.Root` symlink confinement;
- test-binary compilation for:
  - linux/amd64
  - darwin/amd64 and darwin/arm64
  - freebsd/amd64
  - openbsd/amd64
  - netbsd/amd64
  - dragonfly/amd64
  - windows/amd64 and windows/arm64
  - solaris/amd64
  - illumos/amd64
  - aix/ppc64
  - plan9/amd64

The aggregate `go test ./...` process repeatedly exceeded this sandbox's process
stability and restarted the container. It is not reported as passed. Focused
package validation and per-target compilation completed reliably.

## Required release gates on deployment infrastructure

Before calling a specific release production-ready for a specific fleet:

1. Run the full suite with the declared Go 1.26.5 toolchain.
2. Run native kqueue, IOCP, event-port and pollset integration tests on those
   operating systems; cross-compilation is not runtime verification.
3. Run at least 24-72 hour keep-alive and connection-churn soak tests.
4. Test descriptor exhaustion, memory pressure, slow clients, half-closed
   sockets, TLS handshake floods and graceful deploy drains.
5. Benchmark balanced epoll/runtime-native paths against the throughput profile
   under the real handlers, payloads, TLS settings and concurrency distribution.
6. Canary io_uring before broad activation.
7. Compile, verifier-load and attach XDP on the actual kernel/NIC/driver, then
   validate fail-open/fail-closed behavior and rollback.
8. Run security review, fuzzing and race detection under representative load.

## Performance statement

The implementation removes several avoidable latency and operational hazards,
but the included smoke benchmark is only a correctness check. It is not a
credible cross-machine performance comparison. "Fastest" must be established
with repeatable p50/p95/p99/p99.9 latency, throughput, CPU, RSS, allocations,
syscalls, context switches and error-rate measurements on the target system.
