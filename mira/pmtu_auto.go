package mira

import (
	"time"

	"github.com/oarkflow/fh/internal/wire"
)

// Conservative PLPMTUD search sizes. These are UDP datagram *payload* sizes,
// not link MTUs. The application does not change the size of an in-flight
// message: doing so would corrupt reliable message reassembly.
var autoMTUCandidates = [...]int{1280, 1340, 1400, 1452, wire.MaxDatagram}

// autoProbe is called only from the session owner goroutine. Probe packets are
// authenticated, ACK-eliciting, and are the requested wire size. A failed probe
// never increases the envelope size; repeated failures back off exponentially.
func (s *Session) autoProbe(now time.Time) {
	if s.opts.DisableAutoMTU || (s.bootstrapTLS && !s.validated) ||
		s.mtuProbeOutstanding || len(s.control) > 16 || len(s.pending) >= s.opts.MaxInflight/2 ||
		now.Before(s.nextMTUProbe) || now.Sub(s.started) < 500*time.Millisecond {
		return
	}
	target := 0
	for _, n := range autoMTUCandidates {
		if n > s.pathMTU {
			target = n
			break
		}
	}
	if target == 0 {
		return
	}
	s.queueMTUProbe(target, nil)
}

func (s *Session) queueMTUProbe(target int, reply chan int) {
	if target < 1200 || target > wire.MaxDatagram {
		if reply != nil {
			select {
			case reply <- 0:
			default:
			}
		}
		return
	}
	// Frame overhead: 1-byte kind, 1-byte flags, 2-byte canonical varint.
	pad := target - wire.HeaderSize - wire.AuthTagSize - 4
	if pad < 0 {
		if reply != nil {
			select {
			case reply <- 0:
			default:
			}
		}
		return
	}
	if s.mtuProbeOutstanding {
		if reply != nil {
			select {
			case reply <- 0:
			default:
			}
		}
		return
	}
	s.mtuProbeOutstanding = true
	s.mtuProbeTarget = target
	s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameMTUProbe, Body: make([]byte, pad)}, reliable: true, probeMTU: target, probeReply: reply})
	// Prevent a burst of probes even when the pending probe is abruptly
	// canceled by backpressure or send failure.
	s.nextMTUProbe = time.Now().Add(2 * time.Second)
}

func (s *Session) onProbeSuccess(size int) {
	s.mtuProbeOutstanding = false
	s.mtuProbeTarget = 0
	s.mtuProbeFailures = 0
	if size > s.pathMTU {
		s.pathMTU = size
	}
	s.nextMTUProbe = time.Now().Add(3 * time.Second)
}

func (s *Session) onProbeFailure() {
	s.mtuProbeOutstanding = false
	s.mtuProbeTarget = 0
	if s.mtuProbeFailures < 8 {
		s.mtuProbeFailures++
	}
	backoff := 3 * time.Second << min(s.mtuProbeFailures, 6)
	if backoff > 3*time.Minute {
		backoff = 3 * time.Minute
	}
	s.nextMTUProbe = time.Now().Add(backoff)
}

func (s *Session) resetPathMTU() {
	s.pathMTU = 1200
	// Old-path probes are still in s.pending; they are not moved to the new
	// path or allowed to upgrade the new path on a delayed old-path ACK.
	for pn, p := range s.pending {
		if p.frame.probeMTU == 0 {
			continue
		}
		delete(s.pending, pn)
		s.inFlightBytes -= p.size
		if p.frame.probeReply != nil {
			select {
			case p.frame.probeReply <- 0:
			default:
			}
		}
	}
	kept := s.control[:0]
	for _, f := range s.control {
		if f.probeMTU == 0 {
			kept = append(kept, f)
		} else if f.probeReply != nil {
			select {
			case f.probeReply <- 0:
			default:
			}
		}
	}
	s.control = kept
	s.mtuProbeOutstanding = false
	s.mtuProbeFailures = 0
	s.mtuProbeTarget = 0
	s.nextMTUProbe = time.Now().Add(2 * time.Second)
}

// recoverMTUBlackhole drops jumbo frames that have repeatedly failed, and
// resends each affected outstanding reliable message from offset zero using
// conservative 900-byte chunks. The original payload is retained until ACKed.
// Receivers permit a smaller chunk size to replace an incomplete assembly.
// The existing message ID and reserved receiver credits are preserved.
func (s *Session) recoverMTUBlackhole(now time.Time) {
	if s.pathMTU <= 1200 {
		return
	}
	type key struct {
		channel uint64
		msg     uint64
	}
	impacted := map[key]*Channel{}
	for _, p := range s.pending {
		if p.frame.c != nil && p.frame.frame.Type == wire.FrameData && p.size > 1200 {
			impacted[key{p.frame.c.id, p.frame.msgID}] = p.frame.c
		}
	}
	if len(impacted) == 0 {
		return
	}
	s.mtuBlackholeRecoveries++
	s.resetPathMTU() // also invalidate stale probe ACKs from the previous path
	s.mtuProbeFailures = 6
	s.nextMTUProbe = now.Add(3 * time.Minute)
	for pn, p := range s.pending {
		if _, found := impacted[keyOf(p.frame)]; found {
			delete(s.pending, pn)
			s.inFlightBytes -= p.size
		}
	}
	pendingQueue := s.queue[:0]
	for _, f := range s.queue {
		if _, found := impacted[keyOf(f)]; !found {
			pendingQueue = append(pendingQueue, f)
		}
	}
	s.queue = pendingQueue
	// Place resent fragments ahead of later messages on the same channel.
	prefix := make([]txFrame, 0, 256)
	for k, c := range impacted {
		data := c.payloadForRecovery(k.msg)
		if data == nil {
			continue
		}
		count := (len(data) + fragmentSize - 1) / fragmentSize
		if count == 0 {
			count = 1
		}
		c.resetRecoveryFragments(k.msg, count)
		for i := 0; i < count; i++ {
			start := i * fragmentSize
			end := min(len(data), start+fragmentSize)
			body := encodeDataSized(c.id, k.msg, uint64(len(data)), uint64(start), uint64(fragmentSize), data[start:end])
			prefix = append(prefix, txFrame{frame: wire.Frame{Type: wire.FrameData, Body: body}, reliable: true, c: c, msgID: k.msg, reclaimCredit: start == 0})
		}
	}
	s.queue = append(prefix, s.queue...)
	s.cwnd = max(2*wire.MaxDatagram, min(s.cwnd, 10*wire.MaxDatagram))
	s.ssthresh = s.cwnd
	s.nextPaced = time.Time{}
}
func keyOf(f txFrame) struct{ channel, msg uint64 } {
	if f.c == nil || f.frame.Type != wire.FrameData {
		return struct{ channel, msg uint64 }{}
	}
	return struct{ channel, msg uint64 }{f.c.id, f.msgID}
}
