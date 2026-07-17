//go:build windows || solaris || illumos || aix

package kernel

import (
	"crypto/tls"
	"errors"
	"runtime"
)

// Listen serves using the Go runtime's native poller.
func Listen(addr string, t *tls.Config, c KernelConfig, host Host) error {
	if c.Backend == KernelBackendStandard {
		return listenStandard(addr, t, c, host, c.Backend, runtimeNativePoller())
	}
	b, p := runtimeKernelBackend()
	if c.Backend != KernelBackendAuto && c.Backend != KernelBackendNative && c.Backend != b {
		return errors.New("fh: requested backend " + string(c.Backend) + " does not match " + runtime.GOOS + " native backend " + string(b))
	}
	return listenRuntime(addr, t, c, host, KernelRuntimeInfo{Enabled: true, Accelerated: true, Profile: c.Profile, OS: runtime.GOOS, Arch: runtime.GOARCH, Backend: b, RequestedBackend: c.Backend, NativePoller: p, Reactors: 1})
}
func runtimeKernelBackend() (KernelBackend, string) {
	switch runtime.GOOS {
	case "windows":
		return KernelBackendIOCP, "iocp"
	case "solaris", "illumos":
		return KernelBackendEventPorts, "event_ports"
	case "aix":
		return KernelBackendPollset, "pollset"
	}
	return KernelBackendNative, "native"
}
func runtimeNativePoller() string { _, p := runtimeKernelBackend(); return p }
