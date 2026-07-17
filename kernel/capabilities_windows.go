//go:build windows

package kernel

func ProbeKernel() KernelCapabilities {
	c := baseKernelCapabilities()
	c.NativePoller = "iocp"
	c.ServerSockets = true
	c.RuntimeNetpoll = true
	c.IOCP = true
	return c
}
