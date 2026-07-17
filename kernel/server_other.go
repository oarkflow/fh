//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows && !solaris && !illumos && !aix && !js && !wasip1

package kernel

import (
	"crypto/tls"
	"runtime"
)

// Listen serves using the platform's native Go runtime poller.
func Listen(addr string, t *tls.Config, c KernelConfig, host Host) error {
	if c.Backend == KernelBackendStandard {
		return listenStandard(addr, t, c, host, c.Backend, "native")
	}
	return listenRuntime(addr, t, c, host, KernelRuntimeInfo{Enabled: true, Accelerated: true, Profile: c.Profile, OS: runtime.GOOS, Arch: runtime.GOARCH, Backend: KernelBackendNative, RequestedBackend: c.Backend, NativePoller: "native", Reactors: 1})
}
