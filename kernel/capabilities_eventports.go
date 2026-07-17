//go:build solaris || illumos

package kernel

func ProbeKernel() KernelCapabilities {
	c := baseKernelCapabilities()
	c.NativePoller = "event_ports"
	c.ServerSockets = true
	c.RuntimeNetpoll = true
	c.EventPorts = true
	return c
}
