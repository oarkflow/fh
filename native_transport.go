package fh

import (
	"errors"
	"fmt"

	"github.com/oarkflow/fh/kernel"
)

type KernelProfile = kernel.KernelProfile
type KernelBackend = kernel.KernelBackend
type KernelConfig = kernel.KernelConfig
type KernelRuntimeInfo = kernel.KernelRuntimeInfo
type KernelCapabilities = kernel.KernelCapabilities
type XDPConfig = kernel.XDPConfig
type XDPMode = kernel.XDPMode
type XDPManager = kernel.XDPManager
type KernelReadinessSeverity = kernel.ReadinessSeverity
type KernelReadinessIssue = kernel.ReadinessIssue
type KernelReadinessReport = kernel.ReadinessReport

const (
	KernelProfileBalanced      = kernel.KernelProfileBalanced
	KernelProfileThroughput    = kernel.KernelProfileThroughput
	KernelProfileLatency       = kernel.KernelProfileLatency
	KernelProfileCompatibility = kernel.KernelProfileCompatibility

	KernelBackendAuto       = kernel.KernelBackendAuto
	KernelBackendStandard   = kernel.KernelBackendStandard
	KernelBackendNative     = kernel.KernelBackendNative
	KernelBackendEpoll      = kernel.KernelBackendEpoll
	KernelBackendIOUring    = kernel.KernelBackendIOUring
	KernelBackendKqueue     = kernel.KernelBackendKqueue
	KernelBackendIOCP       = kernel.KernelBackendIOCP
	KernelBackendEventPorts = kernel.KernelBackendEventPorts
	KernelBackendPollset    = kernel.KernelBackendPollset

	XDPModeNative  = kernel.XDPModeNative
	XDPModeGeneric = kernel.XDPModeGeneric
	XDPModeOffload = kernel.XDPModeOffload

	KernelReadinessInfo    = kernel.ReadinessInfo
	KernelReadinessWarning = kernel.ReadinessWarning
	KernelReadinessError   = kernel.ReadinessError
)

var ErrXDPUnsupported = kernel.ErrXDPUnsupported

func DefaultKernelConfig() KernelConfig          { return kernel.DefaultKernelConfig() }
func ProductionKernelConfig() KernelConfig       { return kernel.ProductionKernelConfig() }
func HighPerformanceKernelConfig() KernelConfig  { return kernel.HighPerformanceKernelConfig() }
func ProbeKernel() KernelCapabilities            { return kernel.ProbeKernel() }
func NewXDPManager(cfg XDPConfig) *XDPManager    { return kernel.NewXDPManager(cfg) }
func DefaultXDPPinPath(name string) string       { return kernel.DefaultXDPPinPath(name) }
func BuildXDP(source, output string) error       { return kernel.BuildXDP(source, output) }
func DetachXDP(iface string, mode XDPMode) error { return kernel.DetachXDP(iface, mode) }

func WithKernel(cfg KernelConfig) Option {
	return func(c *Config) {
		cfg.Enabled = true
		c.Kernel = cfg
	}
}

func WithKernelDefaults() Option {
	return func(c *Config) { c.Kernel = ProductionKernelConfig() }
}

func (a *App) KernelReadiness() KernelReadinessReport {
	if a == nil {
		return KernelReadinessReport{Ready: false, RequiresWorkloadBenchmark: true, Issues: []KernelReadinessIssue{{Severity: KernelReadinessError, Code: "nil_app", Message: "application is nil"}}}
	}
	c := a.cfg
	return kernel.EvaluateReadiness(kernel.ReadinessConfig{
		Kernel: c.Kernel, MaxConnections: c.MaxConnections, MaxConnectionsPerIP: c.MaxConnectionsPerIP,
		ReadHeaderTimeoutSet: c.ReadHeaderTimeout > 0, RequestBodyTimeoutSet: c.RequestBodyTimeout > 0,
		TLSHandshakeTimeoutSet: c.TLSHandshakeTimeout > 0, IdleTimeoutSet: c.IdleTimeout > 0,
		DisablePanicRecovery: c.DisablePanicRecovery, FastMode: c.Mode == ModeFast,
	}, a.KernelRuntimeInfo())
}

func (a *App) ValidateKernelProduction() error {
	r := a.KernelReadiness()
	var errs []error
	for _, issue := range r.Issues {
		if issue.Severity == KernelReadinessError {
			errs = append(errs, fmt.Errorf("%s: %s", issue.Code, issue.Message))
		}
	}
	return errors.Join(errs...)
}
