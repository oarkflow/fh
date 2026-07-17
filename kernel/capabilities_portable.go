//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows && !solaris && !illumos && !aix && !js && !wasip1

package kernel

func ProbeKernel() KernelCapabilities {
	c := baseKernelCapabilities()
	c.NativePoller = "native"
	c.ServerSockets = true
	return c
}
