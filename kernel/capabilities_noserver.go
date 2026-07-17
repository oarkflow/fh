//go:build js || wasip1

package kernel

func ProbeKernel() KernelCapabilities {
	c := baseKernelCapabilities()
	c.NativePoller = "none"
	return c
}
