//go:build linux

package kernel

import (
	"errors"
	"time"
)

func applyPlatformKernelDefaults(c *KernelConfig) {
	c.ReusePort = true
	c.ReusePortBPF = true
	c.PinThreads = true
	c.TCPUserTimeout = 30 * time.Second
	c.TCPDeferAccept = time.Second
	c.TCPFastOpenQueue = 4096
}
func validatePlatformKernelBackend(b KernelBackend) error {
	switch b {
	case KernelBackendAuto, KernelBackendStandard, KernelBackendNative, KernelBackendEpoll, KernelBackendIOUring:
		return nil
	}
	return errors.New("fh: backend " + string(b) + " is not supported on Linux")
}
func validatePlatformKernelConfig(*KernelConfig) error { return nil }
