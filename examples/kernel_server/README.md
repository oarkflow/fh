# Kernel-assisted server example

Linux:

```bash
go run ./cmd/fh-kernelctl probe
go run ./examples/kernel_server
curl http://127.0.0.1:8080/
curl http://127.0.0.1:8080/_kernel
```

`KernelBackendAuto` uses `io_uring` when the running kernel and security policy
allow ring creation, otherwise it falls back to the epoll reactor. On non-Linux
systems `fh` uses its portable listener unless `Kernel.Required` is set.

XDP changes a network interface and should first be tested in a disposable VM or
network namespace. See `docs/kernel-transport.md`.
