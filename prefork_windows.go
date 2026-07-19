//go:build windows

package fh

import (
	"os"
	"os/signal"
	"syscall"
)

// runSignalLoop blocks until Ctrl+C (or an equivalent termination request)
// stops the master. Windows has no SIGHUP, so unattended zero-downtime
// rolling restarts via signal are Unix-only here; use App.Reload for the
// programmatic equivalent (e.g. from an external control endpoint).
func (sup *preforkSupervisor) runSignalLoop() error {
	ch := make(chan os.Signal, 4)
	// syscall.SIGINT/SIGTERM are the same portable signal set app.go's own
	// runWithSignal listens for on every platform, including Windows.
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	sup.shutdown()
	return nil
}

func preforkTerminate(p *os.Process) error {
	// Windows worker processes have no graceful SIGTERM delivery guarantee,
	// so terminate directly; the worker's own ShutdownWithContext-based drain
	// only ever runs in response to a real signal on platforms that have one.
	return p.Kill()
}
