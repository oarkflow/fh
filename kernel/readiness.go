package kernel

import (
	"fmt"
	"runtime"
)

type ReadinessSeverity string

const (
	ReadinessInfo    ReadinessSeverity = "info"
	ReadinessWarning ReadinessSeverity = "warning"
	ReadinessError   ReadinessSeverity = "error"
)

type ReadinessIssue struct {
	Severity ReadinessSeverity `json:"severity"`
	Code     string            `json:"code"`
	Message  string            `json:"message"`
}

type ReadinessReport struct {
	Ready                     bool              `json:"ready"`
	RequiresWorkloadBenchmark bool              `json:"requires_workload_benchmark"`
	Runtime                   KernelRuntimeInfo `json:"runtime"`
	Issues                    []ReadinessIssue  `json:"issues,omitempty"`
}

type ReadinessConfig struct {
	Kernel                 KernelConfig
	MaxConnections         int
	MaxConnectionsPerIP    int
	ReadHeaderTimeoutSet   bool
	RequestBodyTimeoutSet  bool
	TLSHandshakeTimeoutSet bool
	IdleTimeoutSet         bool
	DisablePanicRecovery   bool
	FastMode               bool
}

func EvaluateReadiness(c ReadinessConfig, runtimeInfo KernelRuntimeInfo) ReadinessReport {
	r := ReadinessReport{Ready: true, RequiresWorkloadBenchmark: true, Runtime: runtimeInfo}
	k := c.Kernel
	add := func(s ReadinessSeverity, code, msg string) {
		r.Issues = append(r.Issues, ReadinessIssue{s, code, msg})
		if s == ReadinessError {
			r.Ready = false
		}
	}
	if !k.Enabled {
		add(ReadinessError, "kernel_disabled", "kernel transport is not enabled")
	}
	if c.MaxConnections <= 0 {
		add(ReadinessError, "unbounded_connections", "MaxConnections must be bounded for production")
	}
	if c.MaxConnectionsPerIP <= 0 {
		add(ReadinessWarning, "unbounded_per_ip_connections", "MaxConnectionsPerIP is disabled; enforce an equivalent limit at a trusted edge")
	}
	if !c.ReadHeaderTimeoutSet {
		add(ReadinessError, "missing_header_timeout", "ReadHeaderTimeout must be positive to limit slow-header attacks")
	}
	if !c.RequestBodyTimeoutSet {
		add(ReadinessWarning, "missing_body_timeout", "RequestBodyTimeout is disabled")
	}
	if !c.TLSHandshakeTimeoutSet {
		add(ReadinessWarning, "missing_tls_timeout", "TLSHandshakeTimeout is disabled")
	}
	if !c.IdleTimeoutSet {
		add(ReadinessWarning, "missing_idle_timeout", "IdleTimeout is disabled")
	}
	if c.DisablePanicRecovery {
		add(ReadinessError, "panic_recovery_disabled", "panic recovery is disabled")
	}
	if c.FastMode {
		add(ReadinessWarning, "fast_mode", "ModeFast prioritizes benchmark throughput and immediate shutdown over production request draining")
	}
	if k.Reactors > 1 && !k.ReusePort && (runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" || runtime.GOOS == "openbsd" || runtime.GOOS == "netbsd" || runtime.GOOS == "dragonfly") {
		add(ReadinessError, "reactors_without_reuseport", "multiple raw listener reactors require ReusePort")
	}
	if runtime.GOOS == "linux" && k.Backend == KernelBackendAuto && k.PreferIOUring {
		add(ReadinessInfo, "io_uring_auto", "throughput profile may select io_uring after runtime probing; canary and benchmark this path")
	}
	if k.BusyPoll > 0 {
		add(ReadinessWarning, "busy_poll", "busy polling consumes CPU while idle")
	}
	if k.ReceiveBufferBytes > 0 || k.SendBufferBytes > 0 {
		add(ReadinessInfo, "fixed_socket_buffers", "fixed socket buffers override OS autotuning")
	}
	if r.Runtime.Backend != "" {
		if r.Runtime.Backend == KernelBackendStandard && k.Backend != KernelBackendStandard {
			s := ReadinessWarning
			if k.Required {
				s = ReadinessError
			}
			add(s, "transport_fallback", "configured kernel backend fell back to the standard listener: "+r.Runtime.FallbackReason)
		}
		if k.XDP.Enabled && k.XDP.Required && !r.Runtime.XDPAttached {
			add(ReadinessError, "xdp_not_attached", "XDP is required but is not attached")
		}
		if r.Runtime.SocketOptionErrors > 0 {
			s := ReadinessWarning
			if k.StrictSocketOptions {
				s = ReadinessError
			}
			add(s, "socket_option_errors", fmt.Sprintf("%d socket option operations failed", r.Runtime.SocketOptionErrors))
		}
	}
	return r
}
