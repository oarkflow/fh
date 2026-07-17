//go:build linux

package kernel

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type epollShard struct {
	id                 int
	listener           *kernelFDListener
	epfd, wakeR, wakeW int
	cfg                KernelConfig
	host               Host
	tlsWrap            func(net.Conn) net.Conn
	closed             *atomic.Bool
	closeOnce          sync.Once
	pressureDelay      time.Duration
}

func newEpollShard(id int, l *kernelFDListener, c KernelConfig, h Host, t func(net.Conn) net.Conn, closed *atomic.Bool) (*epollShard, error) {
	ep, e := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if e != nil {
		return nil, e
	}
	var pipe [2]int
	if e = syscall.Pipe2(pipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); e != nil {
		_ = syscall.Close(ep)
		return nil, e
	}
	fail := func(x error) (*epollShard, error) {
		_ = syscall.Close(pipe[0])
		_ = syscall.Close(pipe[1])
		_ = syscall.Close(ep)
		return nil, x
	}
	if e = syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, l.fd, &syscall.EpollEvent{Events: uint32(syscall.EPOLLIN) | uint32(1<<31), Fd: int32(l.fd)}); e != nil {
		return fail(e)
	}
	if e = syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, pipe[0], &syscall.EpollEvent{Events: uint32(syscall.EPOLLIN), Fd: int32(pipe[0])}); e != nil {
		return fail(e)
	}
	return &epollShard{id: id, listener: l, epfd: ep, wakeR: pipe[0], wakeW: pipe[1], cfg: c, host: h, tlsWrap: t, closed: closed}, nil
}
func (s *epollShard) wake() {
	if s != nil && s.wakeW >= 0 {
		_, _ = syscall.Write(s.wakeW, []byte{1})
	}
}
func (s *epollShard) close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.wakeR >= 0 {
			_ = syscall.Close(s.wakeR)
			s.wakeR = -1
		}
		if s.wakeW >= 0 {
			_ = syscall.Close(s.wakeW)
			s.wakeW = -1
		}
		if s.epfd >= 0 {
			_ = syscall.Close(s.epfd)
			s.epfd = -1
		}
	})
}
func (s *epollShard) run() error {
	if s.cfg.PinThreads {
		cpu, e := reactorCPU(s.id, s.cfg)
		if e == nil {
			var restore func()
			restore, e = pinCurrentThread(cpu)
			if e == nil {
				s.host.pinned(1)
				defer func() { s.host.pinned(-1); restore() }()
			}
		}
		if e != nil {
			if s.cfg.Required {
				return e
			}
			s.host.warn("fh: reactor CPU affinity unavailable", "reactor", s.id, "error", e)
		}
	}
	var events [8]syscall.EpollEvent
	var buf [64]byte
	for !s.closed.Load() && !s.host.closed() {
		n, e := syscall.EpollWait(s.epfd, events[:], -1)
		if e != nil {
			if errors.Is(e, syscall.EINTR) {
				continue
			}
			if s.closed.Load() || errors.Is(e, syscall.EBADF) {
				return nil
			}
			return e
		}
		for i := 0; i < n; i++ {
			fd := int(events[i].Fd)
			switch fd {
			case s.wakeR:
				for {
					if _, x := syscall.Read(s.wakeR, buf[:]); x != nil {
						break
					}
				}
				if s.closed.Load() || s.host.closed() {
					return nil
				}
			case s.listener.fd:
				if e := s.acceptReady(); e != nil {
					return e
				}
			}
		}
	}
	return nil
}
func (s *epollShard) acceptReady() error {
	for !s.closed.Load() && !s.host.closed() {
		fd, _, e := syscall.Accept4(s.listener.fd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if e != nil {
			switch {
			case errors.Is(e, syscall.EAGAIN), errors.Is(e, syscall.EWOULDBLOCK):
				return nil
			case errors.Is(e, syscall.EINTR), errors.Is(e, syscall.ECONNABORTED), errors.Is(e, syscall.EPROTO):
				continue
			case s.closed.Load(), errors.Is(e, syscall.EBADF), errors.Is(e, syscall.EINVAL):
				return nil
			case errors.Is(e, syscall.EMFILE), errors.Is(e, syscall.ENFILE), errors.Is(e, syscall.ENOBUFS), errors.Is(e, syscall.ENOMEM):
				s.host.acceptError()
				s.pressureDelay = nextAcceptBackoff(s.pressureDelay, s.cfg)
				time.Sleep(s.pressureDelay)
				return nil
			default:
				s.host.acceptError()
				return e
			}
		}
		count, x := configureAcceptedFD(fd, s.cfg)
		if count > 0 {
			s.host.socketOptionErrors(count)
		}
		if x != nil {
			_ = syscall.Close(fd)
			s.host.acceptError()
			continue
		}
		c, x := acceptedFDToConn(fd)
		if x != nil {
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
