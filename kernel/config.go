package kernel

import (
	"errors"
	"runtime"
	"strings"
	"time"
)

// KernelProfile selects production defaults without hiding the actual backend.
type KernelProfile string

const (
	KernelProfileBalanced      KernelProfile = "balanced"
	KernelProfileThroughput    KernelProfile = "throughput"
	KernelProfileLatency       KernelProfile = "latency"
	KernelProfileCompatibility KernelProfile = "compatibility"
)

// KernelBackend selects the operating-system-native network reactor used by Listen.
type KernelBackend string

const (
	KernelBackendAuto       KernelBackend = "auto"
	KernelBackendStandard   KernelBackend = "standard"
	KernelBackendNative     KernelBackend = "native"
	KernelBackendEpoll      KernelBackend = "epoll"
	KernelBackendIOUring    KernelBackend = "io_uring"
	KernelBackendKqueue     KernelBackend = "kqueue"
	KernelBackendIOCP       KernelBackend = "iocp"
	KernelBackendEventPorts KernelBackend = "event_ports"
	KernelBackendPollset    KernelBackend = "pollset"
)

// KernelConfig enables operating-system-native serving.
type KernelConfig struct {
	Enabled  bool
	Required bool
	Backend  KernelBackend
	Profile  KernelProfile

	PreferIOUring       bool
	StrictSocketOptions bool

	Reactors     int
	ReusePort    bool
	ReusePortBPF bool
	PinThreads   bool
	CPUSet       []int

	Backlog               int
	ReceiveBufferBytes    int
	SendBufferBytes       int
	AcceptErrorBackoffMin time.Duration
	AcceptErrorBackoffMax time.Duration

	TCPNoDelay         bool
	TCPKeepAlive       time.Duration
	TCPKeepAliveIdle   time.Duration
	TCPKeepAliveIntvl  time.Duration
	TCPKeepAliveProbes int
	TCPUserTimeout     time.Duration
	TCPDeferAccept     time.Duration
	TCPFastOpenQueue   int
	BusyPoll           time.Duration

	IOUringEntries uint32
	XDP            XDPConfig
}

func nextAcceptBackoff(current time.Duration, cfg KernelConfig) time.Duration {
	if current <= 0 {
		return cfg.AcceptErrorBackoffMin
	}
	next := current * 2
	if next < current || next > cfg.AcceptErrorBackoffMax {
		return cfg.AcceptErrorBackoffMax
	}
	return next
}

func finishSocketOptions(cfg KernelConfig, optionErrors []error) (int, error) {
	if len(optionErrors) == 0 {
		return 0, nil
	}
	if cfg.StrictSocketOptions {
		return len(optionErrors), errors.Join(optionErrors...)
	}
	return len(optionErrors), nil
}

type XDPConfig struct {
	Enabled        bool
	Required       bool
	AutoAttach     bool
	Interface      string
	Mode           XDPMode
	ObjectPath     string
	Section        string
	PinPath        string
	ProtectedPorts []uint16
	RatePerSecond  uint64
	Burst          uint64
}

type XDPMode string

const (
	XDPModeNative  XDPMode = "native"
	XDPModeGeneric XDPMode = "generic"
	XDPModeOffload XDPMode = "offload"
)

// KernelRuntimeInfo reports what the process actually activated.
type KernelRuntimeInfo struct {
	Enabled            bool          `json:"enabled"`
	Accelerated        bool          `json:"accelerated"`
	Profile            KernelProfile `json:"profile,omitempty"`
	OS                 string        `json:"os,omitempty"`
	Arch               string        `json:"arch,omitempty"`
	Backend            KernelBackend `json:"backend,omitempty"`
	RequestedBackend   KernelBackend `json:"requested_backend,omitempty"`
	NativePoller       string        `json:"native_poller,omitempty"`
	Reactors           int           `json:"reactors,omitempty"`
	ReusePort          bool          `json:"reuse_port,omitempty"`
	ReusePortBPF       bool          `json:"reuse_port_bpf,omitempty"`
	ThreadsPinned      int           `json:"threads_pinned,omitempty"`
	IOUringProbed      bool          `json:"io_uring_probed,omitempty"`
	IOUringAvailable   bool          `json:"io_uring_available,omitempty"`
	IOUringFeatures    uint32        `json:"io_uring_features,omitempty"`
	IOUringNetworkIO   bool          `json:"io_uring_network_io,omitempty"`
	XDPAttached        bool          `json:"xdp_attached,omitempty"`
	XDPInterface       string        `json:"xdp_interface,omitempty"`
	Accepted           uint64        `json:"accepted"`
	AcceptErrors       uint64        `json:"accept_errors"`
	Dropped            uint64        `json:"dropped"`
	ActiveConnections  uint64        `json:"active_connections"`
	PeakConnections    uint64        `json:"peak_connections"`
	RejectedGlobal     uint64        `json:"rejected_global"`
	RejectedPerIP      uint64        `json:"rejected_per_ip"`
	SocketOptionErrors uint64        `json:"socket_option_errors"`
	FallbackReason     string        `json:"fallback_reason,omitempty"`
}

func DefaultKernelConfig() KernelConfig {
	cfg := KernelConfig{
		Backend: KernelBackendAuto, Profile: KernelProfileBalanced,
		Reactors: runtime.GOMAXPROCS(0), Backlog: 4096,
		AcceptErrorBackoffMin: 5 * time.Millisecond, AcceptErrorBackoffMax: time.Second,
		TCPNoDelay: true, TCPKeepAlive: 3 * time.Minute, TCPKeepAliveIdle: 60 * time.Second,
		TCPKeepAliveIntvl: 15 * time.Second, TCPKeepAliveProbes: 4, IOUringEntries: 4096,
	}
	applyPlatformKernelDefaults(&cfg)
	return cfg
}

func ProductionKernelConfig() KernelConfig {
	cfg := DefaultKernelConfig()
	cfg.Enabled = true
	cfg.Profile = KernelProfileBalanced
	cfg.PreferIOUring = false
	return cfg
}

func HighPerformanceKernelConfig() KernelConfig {
	cfg := DefaultKernelConfig()
	cfg.Enabled = true
	cfg.Profile = KernelProfileThroughput
	cfg.PreferIOUring = runtime.GOOS == "linux"
	cfg.Backlog = 16384
	cfg.IOUringEntries = 8192
	return cfg
}

// NormalizeConfig validates cfg and fills platform-specific defaults.
func NormalizeConfig(cfg *KernelConfig) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	defaults := DefaultKernelConfig()
	if cfg.Backend == "" {
		cfg.Backend = defaults.Backend
	}
	if cfg.Profile == "" {
		cfg.Profile = defaults.Profile
	}
	switch cfg.Profile {
	case KernelProfileBalanced, KernelProfileThroughput, KernelProfileLatency, KernelProfileCompatibility:
	default:
		return errors.New("fh: invalid kernel profile " + string(cfg.Profile))
	}
	if cfg.Profile == KernelProfileThroughput && runtime.GOOS == "linux" {
		cfg.PreferIOUring = true
	}
	if cfg.Profile == KernelProfileCompatibility && cfg.Backend == KernelBackendAuto {
		cfg.Backend = KernelBackendNative
		cfg.PreferIOUring = false
	}
	switch cfg.Backend {
	case KernelBackendAuto, KernelBackendStandard, KernelBackendNative, KernelBackendEpoll, KernelBackendIOUring, KernelBackendKqueue, KernelBackendIOCP, KernelBackendEventPorts, KernelBackendPollset:
	default:
		return errors.New("fh: invalid kernel backend " + string(cfg.Backend))
	}
	if err := validatePlatformKernelBackend(cfg.Backend); err != nil {
		return err
	}
	if cfg.Reactors <= 0 {
		cfg.Reactors = runtime.GOMAXPROCS(0)
		if cfg.Reactors > 1 {
			cfg.ReusePort = true
		}
	}
	if cfg.Reactors < 1 {
		cfg.Reactors = 1
	}
	if cfg.Reactors > 1 && !cfg.ReusePort {
		return errors.New("fh: kernel reactors > 1 require SO_REUSEPORT")
	}
	if cfg.Backlog <= 0 {
		cfg.Backlog = defaults.Backlog
	}
	if cfg.ReceiveBufferBytes < 0 || cfg.SendBufferBytes < 0 {
		return errors.New("fh: socket buffer sizes must be non-negative")
	}
	const maxSocketBuffer = 1 << 30
	if cfg.ReceiveBufferBytes > maxSocketBuffer || cfg.SendBufferBytes > maxSocketBuffer {
		return errors.New("fh: socket buffer sizes must not exceed 1 GiB")
	}
	if cfg.AcceptErrorBackoffMin <= 0 {
		cfg.AcceptErrorBackoffMin = defaults.AcceptErrorBackoffMin
	}
	if cfg.AcceptErrorBackoffMax <= 0 {
		cfg.AcceptErrorBackoffMax = defaults.AcceptErrorBackoffMax
	}
	if cfg.AcceptErrorBackoffMax < cfg.AcceptErrorBackoffMin {
		return errors.New("fh: accept backoff max must be >= min")
	}
	if len(cfg.CPUSet) != 0 {
		seen := make(map[int]struct{}, len(cfg.CPUSet))
		clean := make([]int, 0, len(cfg.CPUSet))
		for _, cpu := range cfg.CPUSet {
			if cpu < 0 {
				return errors.New("fh: CPUSet entries must be non-negative")
			}
			if _, ok := seen[cpu]; ok {
				continue
			}
			seen[cpu] = struct{}{}
			clean = append(clean, cpu)
		}
		cfg.CPUSet = clean
	}
	if cfg.IOUringEntries == 0 {
		cfg.IOUringEntries = defaults.IOUringEntries
	}
	if cfg.IOUringEntries < 8 {
		cfg.IOUringEntries = 8
	}
	if cfg.IOUringEntries > 32768 {
		cfg.IOUringEntries = 32768
	}
	if cfg.XDP.Enabled {
		if strings.TrimSpace(cfg.XDP.Interface) == "" {
			return errors.New("fh: XDP interface is required")
		}
		if cfg.XDP.Mode == "" {
			cfg.XDP.Mode = XDPModeNative
		}
		switch cfg.XDP.Mode {
		case XDPModeNative, XDPModeGeneric, XDPModeOffload:
		default:
			return errors.New("fh: invalid XDP mode " + string(cfg.XDP.Mode))
		}
		if cfg.XDP.Section == "" {
			cfg.XDP.Section = "xdp"
		}
		const maxXDPPacketRate = 1_000_000_000
		if cfg.XDP.RatePerSecond > maxXDPPacketRate || cfg.XDP.Burst > maxXDPPacketRate {
			return errors.New("fh: XDP rate and burst must not exceed 1000000000 packets")
		}
		seenPorts := make(map[uint16]struct{}, len(cfg.XDP.ProtectedPorts))
		ports := cfg.XDP.ProtectedPorts[:0]
		for _, port := range cfg.XDP.ProtectedPorts {
			if port == 0 {
				return errors.New("fh: XDP protected ports must be non-zero")
			}
			if _, exists := seenPorts[port]; exists {
				continue
			}
			seenPorts[port] = struct{}{}
			ports = append(ports, port)
		}
		cfg.XDP.ProtectedPorts = ports
	}
	return validatePlatformKernelConfig(cfg)
}
