package mira

import (
	"encoding/binary"
	"time"

	"github.com/oarkflow/fh/internal/wire"
	"github.com/oarkflow/fh/protocol"
)

func (s *Session) onOpen(body []byte) bool {
	id, cfg, ok := decodeOpen(body)
	if !ok || id == 0 {
		return false
	}
	// Locally-created channel IDs are odd for clients and even for servers.
	if (id%2 == 1) == s.client {
		return false
	}
	if _, wasClosed := s.closedIDs[id]; wasClosed {
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameReset, Body: binary.AppendUvarint(nil, id)}, reliable: true})
		return true
	}
	if c := s.channels[id]; c != nil {
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameOpenAck, Body: binary.AppendUvarint(nil, id)}, reliable: true})
		return true
	}
	if len(s.channels) >= s.opts.MaxChannels || cfg.MaxMessage > int64(s.opts.MaxMessage) || (s.server != nil && !s.server.allowChannel(s.identity, cfg)) {
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameReset, Body: binary.AppendUvarint(nil, id)}, reliable: true})
		return true
	}
	c := newChannel(s, id, cfg, false)
	select {
	case s.accepts <- c:
		s.channels[id] = c
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameOpenAck, Body: binary.AppendUvarint(nil, id)}, reliable: true})
	default:
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameReset, Body: binary.AppendUvarint(nil, id)}, reliable: true})
	}
	return true
}

func (s *Session) onData(body []byte) bool {
	cid, id, total, offset, size, chunk, ok := decodeDataSized(body)
	if !ok || id == 0 {
		return false
	}
	c := s.channels[cid]
	if c == nil {
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameReset, Body: binary.AppendUvarint(nil, cid)}, reliable: true})
		return true
	}
	if c.closed.Load() || c.expired() {
		return true
	}
	if total > uint64(c.cfg.MaxMessage) || total > uint64(s.opts.MaxMessage) || offset > total || uint64(len(chunk)) > total-offset {
		return false
	}
	if size == 0 || size > 1300 || offset%size != 0 {
		return false
	}
	expected := min(int(size), int(total-offset))
	if len(chunk) != expected {
		return false
	}
	if c.cfg.Delivery == protocol.LatestState || c.cfg.Delivery == protocol.BestEffort {
		if !s.admitApplicationBytes(len(chunk)) {
			return true
		} // best effort may drop
		if offset != 0 || total > size {
			return false
		}
		if c.cfg.Delivery == protocol.LatestState && id <= c.lastState {
			return true
		}
		if !s.reserveReceive(int64(len(chunk))) {
			return true
		} // best-effort drop on pressure
		m := protocol.Message{ChannelID: cid, MessageID: id, Payload: append([]byte(nil), chunk...)}
		if c.cfg.Delivery == protocol.LatestState {
			c.lastState = id
			select {
			case old := <-c.recv:
				s.recvBytes.Add(-int64(len(old.Payload)))
			default:
			}
		}
		select {
		case c.recv <- m:
		default:
			s.recvBytes.Add(-int64(len(chunk)))
		}
		return true
	}
	if c.cfg.Delivery == protocol.ReliableOrdered && id < c.nextDeliver {
		return true
	}
	if id <= c.maxSeen && c.maxSeen-id > 4096 {
		return true
	}
	if _, exists := c.seen[id]; exists {
		return true
	}
	if _, exists := c.completed[id]; exists {
		return true
	}
	a := c.assemblies[id]
	if a == nil {
		if len(c.assemblies) >= 64 || len(c.completed) >= 64 {
			return false
		}
		if !s.reserveReceive(int64(total)) {
			return false
		}
		count := (int(total) + int(size) - 1) / int(size)
		if count == 0 {
			count = 1
		}
		a = &assembly{payload: make([]byte, int(total)), got: make([]bool, count), chunkSize: size, last: time.Now()}
		c.assemblies[id] = a
	}
	if uint64(len(a.payload)) != total || a.chunkSize != size {
		// A sender may restart an entire reliable message using smaller
		// fragments after discovering an MTU black hole. A reset is only
		// accepted at offset 0, for the same length and a *smaller* chunk.
		// Previously received larger chunks may arrive later but cannot
		// overwrite this new assembly because their size will differ.
		if offset != 0 || uint64(len(a.payload)) != total || size >= a.chunkSize {
			return false
		}
		count := (int(total) + int(size) - 1) / int(size)
		if count == 0 {
			count = 1
		}
		a.got = make([]bool, count)
		a.count = 0
		a.chunkSize = size
	}
	index := int(offset / size)
	if index >= len(a.got) {
		return false
	}
	if !a.got[index] {
		if !s.admitApplicationBytes(len(chunk)) {
			return false
		}
		copy(a.payload[int(offset):], chunk)
		a.got[index] = true
		a.count++
	}
	a.last = time.Now()
	if a.count != len(a.got) {
		return true
	}
	m := protocol.Message{ChannelID: cid, MessageID: id, Payload: a.payload}
	if c.cfg.Delivery == protocol.ReliableOrdered {
		if len(c.completed) >= 64 {
			return false
		}
		c.completed[id] = m
		delete(c.assemblies, id)
		s.noteSeen(c, id)
		s.drainCompleted()
		return true
	}
	select {
	case c.recv <- m:
		delete(c.assemblies, id)
		s.noteSeen(c, id)
		return true
	default:
		return false
	}
}
func (s *Session) noteSeen(c *Channel, id uint64) {
	if id > c.maxSeen {
		c.maxSeen = id
	}
	c.seen[id] = struct{}{}
	c.seenOrder = append(c.seenOrder, id)
	if len(c.seenOrder) > 4096 {
		old := c.seenOrder[0]
		delete(c.seen, old)
		c.seenOrder = c.seenOrder[1:]
	}
}
