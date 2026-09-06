package fh

import (
	"net"
	"os"
	"sync"
)

// unlinkListener owns a newly-created Unix socket and removes only that path
// when closed. The close operation is idempotent because shutdown may be
// initiated by either the serving context or the caller.
type unlinkListener struct {
	net.Listener
	path string
	once sync.Once
	err  error
}

func (l *unlinkListener) Close() error {
	l.once.Do(func() {
		l.err = l.Listener.Close()
		if err := os.Remove(l.path); l.err == nil && !os.IsNotExist(err) {
			l.err = err
		}
	})
	return l.err
}
