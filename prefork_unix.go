//go:build !windows

package fh

import (
	"os"
	"os/signal"
	"syscall"
)

// runSignalLoop blocks until the master should stop: SIGHUP triggers a
// zero-downtime rolling restart (see preforkSupervisor.rollingRestart) and
// keeps the loop running; SIGINT/SIGTERM gracefully stops every worker and
// returns.
func (sup *preforkSupervisor) runSignalLoop() error {
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(ch)
	for sig := range ch {
		switch sig {
		case syscall.SIGHUP:
			go sup.rollingRestart()
		case syscall.SIGINT, syscall.SIGTERM:
			sup.shutdown()
			return nil
		}
	}
	return nil
}

func preforkTerminate(p *os.Process) error {
	return p.Signal(syscall.SIGTERM)
}
