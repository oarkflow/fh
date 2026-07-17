//go:build js || wasip1

package kernel

import "errors"

func applyPlatformKernelDefaults(c *KernelConfig) {
	c.Reactors = 1
	c.ReusePort = false
	c.ReusePortBPF = false
	c.PinThreads = false
	c.TCPUserTimeout = 0
	c.TCPDeferAccept = 0
	c.TCPFastOpenQueue = 0
	c.BusyPoll = 0
}
func validatePlatformKernelBackend(b KernelBackend) error {
	if b == KernelBackendStandard || b == KernelBackendAuto || b == KernelBackendNative {
		return nil
	}
	return errors.New("fh: backend " + string(b) + " is unavailable on this non-server target")
}
func validatePlatformKernelConfig(c *KernelConfig) error {
	if c.Enabled {
		return errors.New("fh: inbound TCP listeners are unavailable on js/wasm and wasip1")
	}
	return nil
}
