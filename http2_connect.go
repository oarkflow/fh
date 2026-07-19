package fh

import (
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// h2StreamConn adapts an RFC 8441 extended CONNECT HTTP/2 stream to a
// net.Conn, so the same c.Upgrade(protocol, handler) API used for HTTP/1.1
// upgrades (see websocket.go) also works transparently over HTTP/2.
//
// Writes reuse h2Conn.sendData verbatim, so they participate in the stream's
// existing flow control exactly like a normal response body. Reads drain a
// small queue fed by handleData; the receive window is credited back (via
// WINDOW_UPDATE) only as bytes are actually dequeued here, not at frame
// arrival, so backpressure on a slow reader is enforced by the peer's own
// flow-control accounting rather than by an unbounded buffer.
type h2StreamConn struct {
	h      *h2Conn
	stream *h2Stream

	// mu guards inbound/remoteClosed only. It is independent of h.mu (the
	// stream registry lock) and h.flowMu (the send-window lock) and must
	// never be held while calling into either.
	mu           sync.Mutex
	inbound      [][]byte
	remoteClosed bool

	notify chan struct{} // cap 1, non-blocking send; wakes a blocked Read

	closeOnce sync.Once

	readDeadline atomic.Pointer[time.Time]
}

func newH2StreamConn(h *h2Conn, s *h2Stream) *h2StreamConn {
	return &h2StreamConn{h: h, stream: s, notify: make(chan struct{}, 1)}
}

// deliver queues a chunk of DATA payload for Read. Must be called without
// holding h.mu.
func (sc *h2StreamConn) deliver(p []byte) {
	sc.mu.Lock()
	sc.inbound = append(sc.inbound, p)
	sc.mu.Unlock()
	sc.wake()
}

// closeRemote marks the tunnel as half-closed by the peer (END_STREAM
// received, or RST_STREAM/connection loss). Must be called without holding
// h.mu.
func (sc *h2StreamConn) closeRemote() {
	sc.mu.Lock()
	sc.remoteClosed = true
	sc.mu.Unlock()
	sc.wake()
}

func (sc *h2StreamConn) wake() {
	select {
	case sc.notify <- struct{}{}:
	default:
	}
}

func (sc *h2StreamConn) creditWindow(n int) {
	if n <= 0 {
		return
	}
	h := sc.h
	s := sc.stream
	h.mu.Lock()
	h.connRecvWindowAccum += int64(n)
	s.recvWindowAccum += int64(n)
	var connWU, streamWU uint32
	if h.connRecvWindowAccum >= windowsUpdateThreshold {
		connWU = uint32(h.connRecvWindowAccum)
		h.connRecvWindowRemaining += h.connRecvWindowAccum
		h.connRecvWindowAccum = 0
	}
	if s.recvWindowAccum >= windowsUpdateThreshold {
		streamWU = uint32(s.recvWindowAccum)
		s.recvWindow += s.recvWindowAccum
		s.recvWindowAccum = 0
	}
	h.mu.Unlock()
	if connWU > 0 {
		_ = h.sendWindowUpdate(0, connWU)
	}
	if streamWU > 0 {
		_ = h.sendWindowUpdate(s.id, streamWU)
	}
}

func (sc *h2StreamConn) Read(p []byte) (int, error) {
	for {
		sc.mu.Lock()
		if len(sc.inbound) > 0 {
			chunk := sc.inbound[0]
			n := copy(p, chunk)
			if n < len(chunk) {
				sc.inbound[0] = chunk[n:]
			} else {
				sc.inbound = sc.inbound[1:]
			}
			sc.mu.Unlock()
			sc.creditWindow(n)
			return n, nil
		}
		remoteClosed := sc.remoteClosed
		sc.mu.Unlock()
		if remoteClosed {
			return 0, io.EOF
		}

		var timerC <-chan time.Time
		if dl := sc.readDeadline.Load(); dl != nil && !dl.IsZero() {
			d := time.Until(*dl)
			if d <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer := time.NewTimer(d)
			defer timer.Stop()
			timerC = timer.C
		}
		select {
		case <-sc.notify:
		case <-sc.stream.ctx.Done():
			sc.mu.Lock()
			hasData := len(sc.inbound) > 0
			sc.mu.Unlock()
			if hasData {
				continue
			}
			if sc.stream.reset.Load() {
				return 0, net.ErrClosed
			}
			return 0, io.EOF
		case <-timerC:
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (sc *h2StreamConn) Write(p []byte) (int, error) {
	if err := sc.h.sendData(sc.stream, p, false); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (sc *h2StreamConn) Close() error {
	var err error
	sc.closeOnce.Do(func() {
		err = sc.h.sendData(sc.stream, nil, true)
	})
	return err
}

func (sc *h2StreamConn) LocalAddr() net.Addr  { return sc.h.conn.LocalAddr() }
func (sc *h2StreamConn) RemoteAddr() net.Addr { return sc.h.conn.RemoteAddr() }

func (sc *h2StreamConn) SetDeadline(t time.Time) error {
	sc.SetReadDeadline(t)
	return sc.SetWriteDeadline(t)
}

func (sc *h2StreamConn) SetReadDeadline(t time.Time) error {
	sc.readDeadline.Store(&t)
	sc.wake()
	return nil
}

// SetWriteDeadline is not independently supported: writes fall back to the
// server's configured WriteTimeout (see h2Conn.sendData), consistent with
// how HTTP/2 response bodies are already written elsewhere in this file.
func (sc *h2StreamConn) SetWriteDeadline(time.Time) error { return nil }
