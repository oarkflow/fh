//go:build linux

package kernel

import (
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	ioUringOpLinkTimeout = 15
	ioUringOpSend        = 26
	ioUringOpRecv        = 27
	ioUringSQEFlagIOLink = 1 << 2
)

type ioUringKernelTimespec struct {
	Sec  int64
	Nsec int64
}

type ioUringResult struct {
	n   int
	err error
}

type ioUringRequest struct {
	opID            uint64
	timeoutID       uint64
	read            bool
	buf             []byte
	conn            *ioUringConn
	timespec        ioUringKernelTimespec
	timedOut        atomic.Bool
	opDone          bool
	opRes           int32
	timeoutResolved bool
	done            chan ioUringResult
}

type ioUringRequestRef struct {
	req     *ioUringRequest
	timeout bool
}

var ioUringRequestPool = sync.Pool{New: func() any {
	return &ioUringRequest{done: make(chan ioUringResult, 1)}
}}

type ioUringConn struct {
	fd    int
	shard *ioUringShard
	local net.Addr
	peer  net.Addr

	closed        atomic.Bool
	readDeadline  atomic.Int64
	writeDeadline atomic.Int64
}

func newIOUringConn(fd int, shard *ioUringShard) (*ioUringConn, error) {
	local, err := syscall.Getsockname(fd)
	if err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	peer, err := syscall.Getpeername(fd)
	if err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	conn := &ioUringConn{fd: fd, shard: shard, local: sockaddrTCPAddr(local), peer: sockaddrTCPAddr(peer)}
	shard.conns.Store(fd, conn)
	shard.connCount.Add(1)
	return conn, nil
}

func (c *ioUringConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c == nil || c.closed.Load() {
		return 0, net.ErrClosed
	}
	return c.doIO(p, true, c.readDeadline.Load())
}

func (c *ioUringConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c == nil || c.closed.Load() {
		return 0, net.ErrClosed
	}
	return c.doIO(p, false, c.writeDeadline.Load())
}

func (c *ioUringConn) doIO(p []byte, read bool, deadlineNanos int64) (int, error) {
	deadline := time.Time{}
	if deadlineNanos > 0 {
		deadline = time.Unix(0, deadlineNanos)
		if !deadline.After(time.Now()) {
			return 0, os.ErrDeadlineExceeded
		}
	}
	req, err := c.shard.submitIO(c, p, read, deadline)
	if err != nil {
		return 0, err
	}
	result := <-req.done
	runtime.KeepAlive(p)
	req.buf = nil
	req.conn = nil
	req.opID = 0
	req.timeoutID = 0
	req.read = false
	req.timespec = ioUringKernelTimespec{}
	req.timedOut.Store(false)
	req.opDone = false
	req.opRes = 0
	req.timeoutResolved = false
	ioUringRequestPool.Put(req)
	return result.n, result.err
}

func (c *ioUringConn) Close() error {
	if c == nil || !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	shutdownErr := syscall.Shutdown(c.fd, syscall.SHUT_RDWR)
	if errors.Is(shutdownErr, syscall.ENOTCONN) || errors.Is(shutdownErr, syscall.EBADF) {
		shutdownErr = nil
	}
	closeErr := syscall.Close(c.fd)
	if errors.Is(closeErr, syscall.EBADF) {
		closeErr = nil
	}
	c.shard.conns.Delete(c.fd)
	c.shard.connCount.Add(-1)
	return errors.Join(shutdownErr, closeErr)
}

func (c *ioUringConn) LocalAddr() net.Addr  { return c.local }
func (c *ioUringConn) RemoteAddr() net.Addr { return c.peer }

func (c *ioUringConn) SetDeadline(t time.Time) error {
	if c == nil || c.closed.Load() {
		return net.ErrClosed
	}
	n := deadlineUnixNano(t)
	c.readDeadline.Store(n)
	c.writeDeadline.Store(n)
	return nil
}

func (c *ioUringConn) SetReadDeadline(t time.Time) error {
	if c == nil || c.closed.Load() {
		return net.ErrClosed
	}
	c.readDeadline.Store(deadlineUnixNano(t))
	return nil
}

func (c *ioUringConn) SetWriteDeadline(t time.Time) error {
	if c == nil || c.closed.Load() {
		return net.ErrClosed
	}
	c.writeDeadline.Store(deadlineUnixNano(t))
	return nil
}

func deadlineUnixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func (s *ioUringShard) submitIO(conn *ioUringConn, buf []byte, read bool, deadline time.Time) (*ioUringRequest, error) {
	req := ioUringRequestPool.Get().(*ioUringRequest)
	req.conn = conn
	req.buf = buf
	req.read = read
	base := s.nextID.Add(2) - 1
	req.opID = base
	count := uint32(1)
	if !deadline.IsZero() {
		req.timeoutID = base + 1
		d := time.Until(deadline)
		if d <= 0 {
			req.buf = nil
			req.conn = nil
			req.opID = 0
			req.timeoutID = 0
			ioUringRequestPool.Put(req)
			return nil, os.ErrDeadlineExceeded
		}
		req.timespec.Sec = int64(d / time.Second)
		req.timespec.Nsec = int64(d % time.Second)
		count = 2
	}

	for {
		if conn.closed.Load() || s.closed.Load() && s.ringClosing() {
			s.recycleUnsubmitted(req)
			return nil, net.ErrClosed
		}
		s.submitMu.Lock()
		if !s.ring.hasSQSpace(count) {
			s.submitMu.Unlock()
			runtimeGosched()
			continue
		}
		s.requests.Store(req.opID, ioUringRequestRef{req: req})
		if req.timeoutID != 0 {
			s.requests.Store(req.timeoutID, ioUringRequestRef{req: req, timeout: true})
		}
		s.requestCount.Add(1)
		err := s.ring.submitIORequest(req)
		s.submitMu.Unlock()
		if err != nil {
			s.requests.Delete(req.opID)
			if req.timeoutID != 0 {
				s.requests.Delete(req.timeoutID)
			}
			s.requestCount.Add(-1)
			s.recycleUnsubmitted(req)
			return nil, err
		}
		return req, nil
	}
}

func (s *ioUringShard) recycleUnsubmitted(req *ioUringRequest) {
	req.buf = nil
	req.conn = nil
	req.opID = 0
	req.timeoutID = 0
	req.read = false
	req.timespec = ioUringKernelTimespec{}
	req.timedOut.Store(false)
	req.opDone = false
	req.opRes = 0
	req.timeoutResolved = false
	ioUringRequestPool.Put(req)
}

func (s *ioUringShard) ringClosing() bool {
	return s.connCount.Load() == 0
}

func runtimeGosched() {
	// Kept behind a tiny helper so submission retry behavior is easy to profile
	// and replace with a futex/eventfd strategy without touching net.Conn code.
	runtime.Gosched()
}

func (r *ioUring) hasSQSpace(count uint32) bool {
	head := atomic.LoadUint32(r.sqHead)
	tail := atomic.LoadUint32(r.sqTail)
	entries := atomic.LoadUint32(r.sqEntries)
	return tail-head+count <= entries
}

func (r *ioUring) submitIORequest(req *ioUringRequest) error {
	count := uint32(1)
	if req.timeoutID != 0 {
		count = 2
	}
	return r.submitSQEs(count, func(n uint32, sqe *ioUringSQE) {
		if n == 0 {
			if req.read {
				sqe.Opcode = ioUringOpRecv
			} else {
				sqe.Opcode = ioUringOpSend
			}
			sqe.FD = int32(req.conn.fd)
			sqe.Addr = uint64(uintptr(unsafe.Pointer(&req.buf[0])))
			sqe.Len = uint32(len(req.buf))
			sqe.UserData = req.opID
			if req.timeoutID != 0 {
				sqe.Flags = ioUringSQEFlagIOLink
			}
			return
		}
		sqe.Opcode = ioUringOpLinkTimeout
		sqe.Addr = uint64(uintptr(unsafe.Pointer(&req.timespec)))
		sqe.Len = 1
		sqe.UserData = req.timeoutID
	})
}

func (s *ioUringShard) completeIO(cqe ioUringCQE) {
	value, ok := s.requests.Load(cqe.UserData)
	if !ok {
		return
	}
	ref := value.(ioUringRequestRef)
	req := ref.req
	if ref.timeout {
		req.timeoutResolved = true
		if cqe.Res == -int32(syscall.ETIME) {
			req.timedOut.Store(true)
		}
		if req.opDone {
			s.finishIO(req, req.opRes)
		}
		return
	}
	req.opDone = true
	req.opRes = cqe.Res
	if req.timeoutID != 0 && !req.timeoutResolved {
		// The linked timeout SQE references req.timespec even when the socket
		// operation wins. Wait for both CQEs before returning req to the pool,
		// otherwise a later request could overwrite memory still owned by the
		// kernel.
		return
	}
	s.finishIO(req, req.opRes)
}

func (s *ioUringShard) finishIO(req *ioUringRequest, res int32) {
	s.requests.Delete(req.opID)
	if req.timeoutID != 0 {
		s.requests.Delete(req.timeoutID)
	}
	s.requestCount.Add(-1)
	result := ioUringResult{}
	if res >= 0 {
		result.n = int(res)
		if req.read && res == 0 {
			result.err = io.EOF
		}
	} else {
		errno := syscall.Errno(-res)
		switch {
		case errno == syscall.ECANCELED && req.timedOut.Load():
			result.err = os.ErrDeadlineExceeded
		case req.conn == nil || req.conn.closed.Load() || errno == syscall.EBADF:
			result.err = net.ErrClosed
		default:
			result.err = errno
		}
	}
	req.done <- result
}

func (s *ioUringShard) failAllIO(err error) {
	if err == nil {
		err = net.ErrClosed
	}
	// Closing the ring synchronously cancels and releases all requests before
	// blocked callers are allowed to reuse their read/write buffers.
	s.submitMu.Lock()
	s.ring.abort()
	s.submitMu.Unlock()
	s.requests.Range(func(key, value any) bool {
		ref := value.(ioUringRequestRef)
		if ref.timeout {
			return true
		}
		req := ref.req
		s.requests.Delete(req.opID)
		if req.timeoutID != 0 {
			s.requests.Delete(req.timeoutID)
		}
		s.requestCount.Add(-1)
		select {
		case req.done <- ioUringResult{err: err}:
		default:
		}
		return true
	})
	s.conns.Range(func(_, value any) bool {
		_ = value.(*ioUringConn).Close()
		return true
	})
}
