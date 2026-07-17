//go:build !linux

package kernel

func BuildXDP(source, output string) error { return ErrXDPUnsupported }
