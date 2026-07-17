package kernel

import "runtime"

// KernelCapabilities reports locally detectable kernel facilities without
// attaching programs, changing interfaces, or requiring elevated privileges.
type KernelCapabilities struct {
	OS                 string `json:"os"`
	Arch               string `json:"arch"`
	KernelRelease      string `json:"kernel_release,omitempty"`
	NativePoller       string `json:"native_poller,omitempty"`
	ServerSockets      bool   `json:"server_sockets"`
	RuntimeNetpoll     bool   `json:"runtime_netpoll"`
	Epoll              bool   `json:"epoll"`
	Kqueue             bool   `json:"kqueue"`
	IOCP               bool   `json:"iocp"`
	EventPorts         bool   `json:"event_ports"`
	Pollset            bool   `json:"pollset"`
	ReusePort          bool   `json:"reuse_port"`
	ReusePortBPF       bool   `json:"reuse_port_bpf"`
	IOUring            bool   `json:"io_uring"`
	IOUringFeatures    uint32 `json:"io_uring_features,omitempty"`
	IOUringProbeError  string `json:"io_uring_probe_error,omitempty"`
	BPFFSMounted       bool   `json:"bpffs_mounted"`
	IPToolAvailable    bool   `json:"ip_tool_available"`
	XDPTooling         bool   `json:"xdp_tooling"`
	BPFToolAvailable   bool   `json:"bpftool_available"`
	ClangBPFAvailable  bool   `json:"clang_bpf_available"`
	ClangBPFProbeError string `json:"clang_bpf_probe_error,omitempty"`
}

func baseKernelCapabilities() KernelCapabilities {
	return KernelCapabilities{OS: runtime.GOOS, Arch: runtime.GOARCH}
}
