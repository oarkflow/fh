//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import "syscall"

func ProbeKernel() KernelCapabilities {
	c := baseKernelCapabilities()
	c.NativePoller = "kqueue"
	c.ServerSockets = true
	c.RuntimeNetpoll = true
	c.ReusePort = true
	k, e := syscall.Kqueue()
	if e == nil {
		c.Kqueue = true
		_ = syscall.Close(k)
	}
	return c
}
