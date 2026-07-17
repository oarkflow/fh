package kernel

import (
	"runtime"
	"testing"
)

func TestNormalizeKernelConfig(t *testing.T) {
	cfg := KernelConfig{Enabled: true, Backend: KernelBackendAuto}
	if err := NormalizeConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	defaults := DefaultKernelConfig()
	if cfg.Reactors != defaults.Reactors {
		t.Fatalf("reactors=%d want=%d for %s", cfg.Reactors, defaults.Reactors, runtime.GOOS)
	}
	if cfg.Reactors > 1 && !cfg.ReusePort {
		t.Fatal("multiple reactors must enable reuseport")
	}
	if cfg.Backlog <= 0 || cfg.IOUringEntries < 8 {
		t.Fatalf("invalid defaults: backlog=%d entries=%d", cfg.Backlog, cfg.IOUringEntries)
	}
}

func TestNormalizeKernelConfigRejectsInvalidBackend(t *testing.T) {
	cfg := KernelConfig{Enabled: true, Backend: "invalid"}
	if err := NormalizeConfig(&cfg); err == nil {
		t.Fatal("expected invalid backend error")
	}
}

func TestNormalizeKernelConfigRequiresReusePort(t *testing.T) {
	cfg := KernelConfig{Enabled: true, Backend: KernelBackendEpoll, Reactors: 2}
	if err := NormalizeConfig(&cfg); err == nil {
		t.Fatal("expected reactors/reuseport validation error")
	}
}
