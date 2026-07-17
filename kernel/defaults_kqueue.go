//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import "errors"

func applyPlatformKernelDefaults(c *KernelConfig) {
	c.ReusePort = true
	c.ReusePortBPF = false
	c.PinThreads = false
	c.TCPUserTimeout = 0
	c.TCPDeferAccept = 0
	c.TCPFastOpenQueue = 0
	c.BusyPoll = 0
}
func validatePlatformKernelBackend(b KernelBackend) error {
	switch b {
	case KernelBackendAuto, KernelBackendStandard, KernelBackendNative, KernelBackendKqueue:
		return nil
	}
	return errors.New("fh: backend " + string(b) + " is not supported on this kqueue platform")
}
func validatePlatformKernelConfig(c *KernelConfig) error {
	if c.XDP.Enabled {
		return errors.New("fh: XDP is Linux-only; use pf or the host firewall on this platform")
	}
	return nil
}
