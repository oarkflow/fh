package mira

import (
	"testing"
	"time"

	"github.com/oarkflow/fh/internal/wire"
)

func TestPersistentCongestionCollapseAndAckRecovery(t *testing.T) {
	t0 := time.Now()
	s := &Session{cwnd: 128 * wire.MaxDatagram, ssthresh: 64 * wire.MaxDatagram, lastAckTime: t0}
	p := &sentPacket{first: t0, tries: 2, size: wire.MaxDatagram}
	pto := 100 * time.Millisecond
	s.detectPersistentCongestion(t0.Add(2*pto), p, pto)
	if s.persistentCongestion != 0 {
		t.Fatal("collapsed before threshold")
	}
	s.detectPersistentCongestion(t0.Add(4*pto), p, pto)
	if s.persistentCongestion != 1 || s.cwnd != 2*wire.MaxDatagram || !s.persistentRecovery {
		t.Fatal("persistent congestion not detected")
	}
	s.detectPersistentCongestion(t0.Add(5*pto), p, pto)
	if s.persistentCongestion != 1 {
		t.Fatal("duplicate collapse")
	}
	// Real ack must clear the recovery state, but a normal congestion window
	// increment must never revive it to the prior high transmission rate.
	s.pending = map[uint64]*sentPacket{2: {size: 1200, sent: t0.Add(5 * pto), tries: 0}}
	s.onAck(2)
	if s.persistentRecovery || s.lastAckTime.IsZero() || s.cwnd > 4*wire.MaxDatagram {
		t.Fatal("ACK recovery incorrect")
	}
	p.first = time.Now().Add(-5 * pto)
	s.detectPersistentCongestion(time.Now(), p, pto)
	if s.persistentCongestion != 1 {
		t.Fatal("recent ACK should suppress false persistent congestion")
	}
}

func TestPersistentCongestionExcludesProbes(t *testing.T) {
	t0 := time.Now()
	s := &Session{cwnd: 64 * wire.MaxDatagram, ssthresh: 64 * wire.MaxDatagram}
	p := &sentPacket{first: t0.Add(-time.Second), tries: 4, frame: txFrame{probeMTU: 1400}}
	s.detectPersistentCongestion(t0, p, 100*time.Millisecond)
	if s.persistentCongestion != 0 {
		t.Fatal("MTU probe caused congestion collapse")
	}
}
