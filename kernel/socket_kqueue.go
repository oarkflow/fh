//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

type kernelFDListener struct {
	fd           int
	addr         *net.TCPAddr
	optionErrors int
	closed       atomic.Bool
}

func (l *kernelFDListener) Addr() net.Addr { return l.addr }
func (l *kernelFDListener) Close() error {
	if l == nil || !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	if e := syscall.Close(l.fd); e != nil && !errors.Is(e, syscall.EBADF) {
		return e
	}
	return nil
}
func openKernelListener(addr string, c KernelConfig) (*kernelFDListener, error) {
	a, e := net.ResolveTCPAddr("tcp", addr)
	if e != nil {
		return nil, fmt.Errorf("fh: resolve kqueue listen address: %w", e)
	}
	d := syscall.AF_INET6
	if a.IP != nil && a.IP.To4() != nil {
		d = syscall.AF_INET
	}
	fd, e := syscall.Socket(d, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if e != nil {
		return nil, e
	}
	fail := func(x error) (*kernelFDListener, error) { _ = syscall.Close(fd); return nil, x }
	syscall.CloseOnExec(fd)
	if e = syscall.SetNonblock(fd, true); e != nil {
		return fail(e)
	}
	if e = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); e != nil {
		return fail(e)
	}
	if c.ReusePort {
		if e = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1); e != nil {
			return fail(e)
		}
	}
	if d == syscall.AF_INET6 {
		v := 0
		if a.IP != nil && !a.IP.IsUnspecified() {
			v = 1
		}
		if e = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v); e != nil {
			return fail(e)
		}
	}
	var sa syscall.Sockaddr
	if d == syscall.AF_INET {
		var r [4]byte
		if v := a.IP.To4(); v != nil {
			copy(r[:], v)
		}
		sa = &syscall.SockaddrInet4{Port: a.Port, Addr: r}
	} else {
		var r [16]byte
		if v := a.IP.To16(); v != nil {
			copy(r[:], v)
		}
		s := &syscall.SockaddrInet6{Port: a.Port, Addr: r}
		if a.Zone != "" {
			i, x := net.InterfaceByName(a.Zone)
			if x != nil {
				return fail(x)
			}
			s.ZoneId = uint32(i.Index)
		}
		sa = s
	}
	if e = syscall.Bind(fd, sa); e != nil {
		return fail(e)
	}
	if e = syscall.Listen(fd, c.Backlog); e != nil {
		return fail(e)
	}
	actual, e := syscall.Getsockname(fd)
	if e != nil {
		return fail(e)
	}
	return &kernelFDListener{fd: fd, addr: sockaddrTCPAddr(actual)}, nil
}
func sockaddrTCPAddr(sa syscall.Sockaddr) *net.TCPAddr {
	switch v := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: net.IPv4(v.Addr[0], v.Addr[1], v.Addr[2], v.Addr[3]), Port: v.Port}
	case *syscall.SockaddrInet6:
		i := make(net.IP, 16)
		copy(i, v.Addr[:])
		z := ""
		if v.ZoneId != 0 {
			if x, e := net.InterfaceByIndex(int(v.ZoneId)); e == nil {
				z = x.Name
			}
		}
		return &net.TCPAddr{IP: i, Port: v.Port, Zone: z}
	}
	return &net.TCPAddr{}
}
func acceptedFDToConn(fd int, c KernelConfig) (net.Conn, int, error) {
	syscall.CloseOnExec(fd)
	if e := syscall.SetNonblock(fd, true); e != nil {
		_ = syscall.Close(fd)
		return nil, 0, e
	}
	f := os.NewFile(uintptr(fd), "fh-kqueue-accepted")
	if f == nil {
		_ = syscall.Close(fd)
		return nil, 0, errors.New("fh: invalid accepted fd")
	}
	n, e := net.FileConn(f)
	ce := f.Close()
	if e != nil {
		return nil, 0, e
	}
	if ce != nil {
		_ = n.Close()
		return nil, 0, ce
	}
	count, e := configurePortableTCPConn(n, c)
	if e != nil {
		_ = n.Close()
		return nil, count, e
	}
	return n, count, nil
}
func temporaryAcceptPressure(e error) bool {
	return errors.Is(e, syscall.EMFILE) || errors.Is(e, syscall.ENFILE) || errors.Is(e, syscall.ENOBUFS) || errors.Is(e, syscall.ENOMEM)
}
func kqueueWaitTimeout() *syscall.Timespec {
	t := syscall.NsecToTimespec(time.Second.Nanoseconds())
	return &t
}
