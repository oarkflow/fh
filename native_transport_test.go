package fh

import "testing"

func TestKernelRuntimeCounters(t *testing.T) {
	app := New(WithStartupBannerDisabled(true))
	app.setKernelRuntime(KernelRuntimeInfo{Enabled: true, Backend: KernelBackendEpoll, Reactors: 2})
	app.kernelCounters.accepted.Add(3)
	app.kernelCounters.acceptErrors.Add(2)
	app.kernelCounters.dropped.Add(1)
	info := app.KernelRuntimeInfo()
	if info.Accepted != 3 || info.AcceptErrors != 2 || info.Dropped != 1 {
		t.Fatalf("unexpected counters: %+v", info)
	}
}
