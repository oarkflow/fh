//go:build aix

package kernel

func ProbeKernel() KernelCapabilities {
	c := baseKernelCapabilities()
	c.NativePoller = "pollset"
	c.ServerSockets = true
	c.RuntimeNetpoll = true
	c.Pollset = true
	return c
}
