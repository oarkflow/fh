//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import (
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"time"
)

type kqueueShard struct {
	id            int
	listener      *kernelFDListener
	kq            int
	cfg           KernelConfig
	host          Host
	tlsWrap       func(net.Conn) net.Conn
	closed        *atomic.Bool
	pressureDelay time.Duration
}

func newKqueueShard(id int, l *kernelFDListener, c KernelConfig, h Host, t func(net.Conn) net.Conn, closed *atomic.Bool) (*kqueueShard, error) {
	k, e := syscall.Kqueue()
	if e != nil {
		return nil, e
	}
	syscall.CloseOnExec(k)
	var ch syscall.Kevent_t
	syscall.SetKevent(&ch, l.fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_ENABLE|syscall.EV_CLEAR)
	if _, e = syscall.Kevent(k, []syscall.Kevent_t{ch}, nil, nil); e != nil {
		_ = syscall.Close(k)
		return nil, e
	}
	return &kqueueShard{id: id, listener: l, kq: k, cfg: c, host: h, tlsWrap: t, closed: closed}, nil
}
func (s *kqueueShard) close() {
	if s != nil && s.kq >= 0 {
		_ = syscall.Close(s.kq)
		s.kq = -1
	}
}
func (s *kqueueShard) run() error {
	var events [8]syscall.Kevent_t
	for !s.closed.Load() && !s.host.closed() {
		n, e := syscall.Kevent(s.kq, nil, events[:], kqueueWaitTimeout())
		if e != nil {
			if errors.Is(e, syscall.EINTR) {
				continue
			}
			if s.closed.Load() || errors.Is(e, syscall.EBADF) || errors.Is(e, syscall.EINVAL) {
				return nil
			}
			return e
		}
		if n > 0 {
			if e = s.acceptReady(); e != nil {
				return e
			}
		}
	}
	return nil
}
func (s *kqueueShard) acceptReady() error {
	for !s.closed.Load() && !s.host.closed() {
		fd, _, e := syscall.Accept(s.listener.fd)
		if e != nil {
			switch {
			case errors.Is(e, syscall.EAGAIN), errors.Is(e, syscall.EWOULDBLOCK):
				return nil
			case errors.Is(e, syscall.EINTR), errors.Is(e, syscall.ECONNABORTED):
				continue
			case s.closed.Load(), errors.Is(e, syscall.EBADF), errors.Is(e, syscall.EINVAL):
				return nil
			case temporaryAcceptPressure(e):
				s.host.acceptError()
				s.pressureDelay = nextAcceptBackoff(s.pressureDelay, s.cfg)
				time.Sleep(s.pressureDelay)
				return nil
			default:
				s.host.acceptError()
				return e
			}
		}
		c, count, e := acceptedFDToConn(fd, s.cfg)
		if count > 0 {
			s.host.socketOptionErrors(count)
		}
		if e != nil {
			s.host.acceptError()
			continue
		}
		s.pressureDelay = 0
		if s.tlsWrap != nil {
			c = s.tlsWrap(c)
		}
		s.host.accept(c)
	}
	return nil
}
