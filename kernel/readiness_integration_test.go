package kernel_test

import (
	"runtime"
	"testing"

	"github.com/oarkflow/fh"
)

func TestKernelReadinessRejectsUnboundedProductionConfig(t *testing.T) {
	c := fh.ProductionKernelConfig()
	a := fh.NewProduction(fh.WithKernel(c), fh.WithMaxConnections(0))
	if e := a.ValidateKernelProduction(); e == nil {
		t.Fatal("expected failure")
	}
}
func TestKernelReadinessAcceptsConfiguredProductionApp(t *testing.T) {
	c := fh.ProductionKernelConfig()
	c.Reactors = 1
	c.ReusePort = false
	a := fh.NewProduction(fh.WithKernel(c))
	r := a.KernelReadiness()
	if !r.Ready {
		t.Fatalf("issues: %+v", r.Issues)
	}
	if !r.RequiresWorkloadBenchmark {
		t.Fatal("benchmark requirement missing")
	}
}
func TestHighPerformanceKernelConfigPrefersIOUring(t *testing.T) {
	c := fh.HighPerformanceKernelConfig()
	if c.Profile != fh.KernelProfileThroughput {
		t.Fatal(c)
	}
	if runtimeGOOSLinux() && !c.PreferIOUring {
		t.Fatal(c)
	}
}
func runtimeGOOSLinux() bool { return runtime.GOOS == "linux" }
