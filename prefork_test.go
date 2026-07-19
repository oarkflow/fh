package fh

import (
	"testing"
	"time"
)

func TestPreforkConfigNormalizeDefaults(t *testing.T) {
	var cfg PreforkConfig
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	d := defaultPreforkConfig()
	if cfg.Workers != d.Workers {
		t.Fatalf("Workers = %d, want default %d", cfg.Workers, d.Workers)
	}
	if cfg.WorkerReactors != 1 {
		t.Fatalf("WorkerReactors = %d, want 1", cfg.WorkerReactors)
	}
	if cfg.ReadyTimeout != d.ReadyTimeout {
		t.Fatalf("ReadyTimeout = %v, want %v", cfg.ReadyTimeout, d.ReadyTimeout)
	}
	if cfg.ShutdownTimeout != d.ShutdownTimeout {
		t.Fatalf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, d.ShutdownTimeout)
	}
	if cfg.RestartBackoffMin != d.RestartBackoffMin || cfg.RestartBackoffMax != d.RestartBackoffMax {
		t.Fatalf("backoff = [%v,%v], want [%v,%v]", cfg.RestartBackoffMin, cfg.RestartBackoffMax, d.RestartBackoffMin, d.RestartBackoffMax)
	}
}

func TestPreforkConfigNormalizePreservesExplicitValues(t *testing.T) {
	cfg := PreforkConfig{
		Workers:           4,
		WorkerReactors:    2,
		ReadyTimeout:      2 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		RestartBackoffMin: time.Second,
		RestartBackoffMax: 10 * time.Second,
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	if cfg.Workers != 4 || cfg.WorkerReactors != 2 || cfg.ReadyTimeout != 2*time.Second ||
		cfg.ShutdownTimeout != 5*time.Second || cfg.RestartBackoffMin != time.Second || cfg.RestartBackoffMax != 10*time.Second {
		t.Fatalf("normalize mutated explicit values: %+v", cfg)
	}
}

func TestPreforkConfigNormalizeRejectsInvertedBackoff(t *testing.T) {
	cfg := PreforkConfig{RestartBackoffMin: 10 * time.Second, RestartBackoffMax: time.Second}
	if err := cfg.normalize(); err == nil {
		t.Fatal("expected error when RestartBackoffMax < RestartBackoffMin")
	}
}

func TestPreforkOptionsApply(t *testing.T) {
	var cfg PreforkConfig
	for _, opt := range []PreforkOption{
		WithPreforkWorkers(3),
		WithPreforkWorkerReactors(2),
		WithPreforkReadyTimeout(7 * time.Second),
		WithPreforkShutdownTimeout(9 * time.Second),
		WithPreforkRestartBackoff(100*time.Millisecond, time.Minute),
	} {
		opt(&cfg)
	}
	if cfg.Workers != 3 || cfg.WorkerReactors != 2 || cfg.ReadyTimeout != 7*time.Second ||
		cfg.ShutdownTimeout != 9*time.Second || cfg.RestartBackoffMin != 100*time.Millisecond || cfg.RestartBackoffMax != time.Minute {
		t.Fatalf("options did not apply: %+v", cfg)
	}
}

func TestNextPreforkBackoffDoublesAndCaps(t *testing.T) {
	cfg := PreforkConfig{RestartBackoffMin: 500 * time.Millisecond, RestartBackoffMax: 4 * time.Second}
	got := nextPreforkBackoff(0, cfg)
	if got != cfg.RestartBackoffMin {
		t.Fatalf("first backoff = %v, want min %v", got, cfg.RestartBackoffMin)
	}
	got = nextPreforkBackoff(got, cfg)
	if got != time.Second {
		t.Fatalf("second backoff = %v, want 1s", got)
	}
	got = nextPreforkBackoff(got, cfg)
	if got != 2*time.Second {
		t.Fatalf("third backoff = %v, want 2s", got)
	}
	got = nextPreforkBackoff(got, cfg)
	if got != cfg.RestartBackoffMax {
		t.Fatalf("fourth backoff = %v, want capped at max %v", got, cfg.RestartBackoffMax)
	}
	got = nextPreforkBackoff(got, cfg)
	if got != cfg.RestartBackoffMax {
		t.Fatalf("backoff exceeded max: %v", got)
	}
}

func TestReloadWithoutActiveMasterErrors(t *testing.T) {
	app := New()
	if err := app.Reload(); err == nil {
		t.Fatal("expected Reload to error when no ListenPrefork master is active")
	}
}

func TestWaitChildrenReadyTimesOutAndReportsEarlyExit(t *testing.T) {
	readyChild := &preforkChild{index: 0, ready: make(chan struct{}), exited: make(chan struct{})}
	close(readyChild.ready)

	neverReady := &preforkChild{index: 1, ready: make(chan struct{}), exited: make(chan struct{})}
	if err := waitChildrenReady([]*preforkChild{readyChild, neverReady}, 20*time.Millisecond); err == nil {
		t.Fatal("expected timeout error when a worker never becomes ready")
	}

	exitedChild := &preforkChild{index: 2, ready: make(chan struct{}), exited: make(chan struct{})}
	close(exitedChild.exited)
	if err := waitChildrenReady([]*preforkChild{exitedChild}, time.Second); err == nil {
		t.Fatal("expected error when a worker exits before becoming ready")
	}
}
