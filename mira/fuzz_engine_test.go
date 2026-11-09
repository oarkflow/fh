package mira

import (
	"github.com/oarkflow/fh/internal/wire"
	"testing"
)

// Inputs model an authenticated but malicious peer sending arbitrary frames.
// Bounds are intentionally tighter than the network defaults.
func FuzzAuthenticatedFrameMachine(f *testing.F) {
	f.Add([]byte{3, 0, 1, 0, 0, 0, 4, 2, 1, 2})
	f.Add([]byte{9, 0, 4, 1, 1, 1, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		opts := (Options{MaxChannels: 4, MaxMessage: 1 << 16}).normalized()
		s := &Session{opts: opts, client: false, channels: make(map[uint64]*Channel), closedIDs: make(map[uint64]struct{}), pending: make(map[uint64]*sentPacket), accepts: make(chan *Channel, 4), done: make(chan struct{}), cwnd: 10 * 1200}
		max := min(len(b)/2, 32)
		for i := 0; i < max; i++ {
			typ := 1 + b[2*i]%wire.FrameAckRanges
			size := int(b[2*i+1])
			start := 2*i + 2
			if start > len(b) {
				break
			}
			end := min(start+size, len(b))
			s.onFrame(wire.Frame{Type: typ, Body: b[start:end]})
		}
	})
}
