//go:build linux

package kernel

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func ProbeKernel() KernelCapabilities {
	caps := baseKernelCapabilities()
	caps.KernelRelease = linuxKernelRelease()
	caps.NativePoller = "epoll"
	caps.ServerSockets = true
	caps.RuntimeNetpoll = true
	caps.Epoll = true
	caps.ReusePort = true
	caps.ReusePortBPF = true
	available, features, err := probeIOUring(8)
	caps.IOUring = available
	caps.IOUringFeatures = features
	if err != nil {
		caps.IOUringProbeError = err.Error()
	}
	_, ipErr := exec.LookPath("ip")
	_, bpfErr := exec.LookPath("bpftool")
	caps.IPToolAvailable = ipErr == nil
	caps.BPFToolAvailable = bpfErr == nil
	if _, err := os.Stat("/sys/fs/bpf"); err == nil {
		caps.BPFFSMounted = true
	}
	caps.ClangBPFAvailable, caps.ClangBPFProbeError = probeClangBPF()
	caps.XDPTooling = caps.IPToolAvailable && caps.BPFToolAvailable && caps.ClangBPFAvailable
	return caps
}

func probeClangBPF() (bool, string) {
	if _, err := exec.LookPath("clang"); err != nil {
		return false, err.Error()
	}
	cmd := exec.Command("clang", "-target", "bpf", "-x", "c", "-c", "-", "-o", os.DevNull)
	cmd.Stdin = strings.NewReader("int x;\n")
	if out, err := cmd.CombinedOutput(); err != nil {
		message := strings.TrimSpace(string(out))
		if message == "" {
			message = err.Error()
		}
		return false, message
	}
	return true, ""
}

func linuxKernelRelease() string {
	var u syscall.Utsname
	if err := syscall.Uname(&u); err != nil {
		return ""
	}
	buf := make([]byte, 0, len(u.Release))
	for _, c := range u.Release {
		if c == 0 {
			break
		}
		buf = append(buf, byte(c))
	}
	return string(buf)
}
