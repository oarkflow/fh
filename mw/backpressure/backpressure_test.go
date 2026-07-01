package backpressure

import (
	"github.com/oarkflow/fh"
	"testing"
)

type q struct{ st fh.QueueStats }

func (q q) Stats() (fh.QueueStats, error) { return q.st, nil }
func TestNewDefaultDoesNotPanic(t *testing.T) {
	_ = New(Config{Queue: q{st: fh.QueueStats{Pending: 1}}, MaxPending: 10})
}
