//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows && !solaris && !illumos && !aix && !js && !wasip1

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
	switch b {
	case KernelBackendAuto, KernelBackendStandard, KernelBackendNative:
		return nil
	}
	return errors.New("fh: backend " + string(b) + " is not supported on this platform")
}
func validatePlatformKernelConfig(c *KernelConfig) error {
	if c.XDP.Enabled {
		return errors.New("fh: XDP is Linux-only")
	}
	return nil
}
