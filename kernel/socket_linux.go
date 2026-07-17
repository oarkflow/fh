//go:build linux

package kernel

import (
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	linuxSOReusePort           = 15
	linuxSOAttachReuseportCBPF = 51
	linuxSOBusyPoll            = 46
	linuxTCPDeferAccept        = 9
	linuxTCPFastOpen           = 23
	linuxTCPUserTimeout        = 18
	linuxTCPKeepIdle           = 4
	linuxTCPKeepIntvl          = 5
	linuxTCPKeepCnt            = 6
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
		return nil, fmt.Errorf("fh: resolve kernel listen address: %w", e)
	}
	d := syscall.AF_INET6
	if a.IP != nil && a.IP.To4() != nil {
		d = syscall.AF_INET
	}
	fd, e := syscall.Socket(d, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, syscall.IPPROTO_TCP)
	if e != nil {
		return nil, fmt.Errorf("fh: create kernel listener: %w", e)
	}
	fail := func(x error) (*kernelFDListener, error) { _ = syscall.Close(fd); return nil, x }
	if e = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); e != nil {
		return fail(fmt.Errorf("fh: SO_REUSEADDR: %w", e))
	}
	if c.ReusePort {
		if e = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, linuxSOReusePort, 1); e != nil {
			return fail(fmt.Errorf("fh: SO_REUSEPORT: %w", e))
		}
	}
	if d == syscall.AF_INET6 {
		v := 0
		if a.IP != nil && !a.IP.IsUnspecified() {
			v = 1
		}
		if e = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v); e != nil {
			return fail(fmt.Errorf("fh: IPV6_V6ONLY: %w", e))
		}
	}
	var opts []error
	if c.TCPFastOpenQueue > 0 {
		if e = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, linuxTCPFastOpen, c.TCPFastOpenQueue); e != nil {
			opts = append(opts, fmt.Errorf("TCP_FASTOPEN: %w", e))
		}
	}
	if c.TCPDeferAccept > 0 {
		if e = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, linuxTCPDeferAccept, durationSecondsCeil(c.TCPDeferAccept)); e != nil {
			opts = append(opts, fmt.Errorf("TCP_DEFER_ACCEPT: %w", e))
		}
	}
	count, optErr := finishSocketOptions(c, opts)
	if optErr != nil {
		return fail(optErr)
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
		return fail(fmt.Errorf("fh: bind kernel listener %s: %w", addr, e))
	}
	if e = syscall.Listen(fd, c.Backlog); e != nil {
		return fail(fmt.Errorf("fh: listen kernel socket: %w", e))
	}
	actual, e := syscall.Getsockname(fd)
	if e != nil {
		return fail(fmt.Errorf("fh: getsockname: %w", e))
	}
	return &kernelFDListener{fd: fd, addr: sockaddrTCPAddr(actual), optionErrors: count}, nil
}
func sockaddrTCPAddr(sa syscall.Sockaddr) *net.TCPAddr {
	switch v := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: net.IPv4(v.Addr[0], v.Addr[1], v.Addr[2], v.Addr[3]), Port: v.Port}
	case *syscall.SockaddrInet6:
		ip := make(net.IP, net.IPv6len)
		copy(ip, v.Addr[:])
		z := ""
		if v.ZoneId != 0 {
			if i, e := net.InterfaceByIndex(int(v.ZoneId)); e == nil {
				z = i.Name
			}
		}
		return &net.TCPAddr{IP: ip, Port: v.Port, Zone: z}
	}
	return &net.TCPAddr{}
}
func configureAcceptedFD(fd int, c KernelConfig) (int, error) {
	var errs []error
	set := func(level, opt, val int, name string) {
		if e := syscall.SetsockoptInt(fd, level, opt, val); e != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, e))
		}
	}
	if c.TCPNoDelay {
		set(syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1, "TCP_NODELAY")
	}
	if c.ReceiveBufferBytes > 0 {
		set(syscall.SOL_SOCKET, syscall.SO_RCVBUF, c.ReceiveBufferBytes, "SO_RCVBUF")
	}
	if c.SendBufferBytes > 0 {
		set(syscall.SOL_SOCKET, syscall.SO_SNDBUF, c.SendBufferBytes, "SO_SNDBUF")
	}
	if c.TCPKeepAlive > 0 || c.TCPKeepAliveIdle > 0 {
		set(syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, 1, "SO_KEEPALIVE")
		idle := c.TCPKeepAliveIdle
		if idle <= 0 {
			idle = c.TCPKeepAlive
		}
		if idle > 0 {
			set(syscall.IPPROTO_TCP, linuxTCPKeepIdle, durationSecondsCeil(idle), "TCP_KEEPIDLE")
		}
		if c.TCPKeepAliveIntvl > 0 {
			set(syscall.IPPROTO_TCP, linuxTCPKeepIntvl, durationSecondsCeil(c.TCPKeepAliveIntvl), "TCP_KEEPINTVL")
		}
		if c.TCPKeepAliveProbes > 0 {
			set(syscall.IPPROTO_TCP, linuxTCPKeepCnt, c.TCPKeepAliveProbes, "TCP_KEEPCNT")
		}
	}
	if c.TCPUserTimeout > 0 {
		ms := int(c.TCPUserTimeout / time.Millisecond)
		if ms < 1 {
			ms = 1
		}
		set(syscall.IPPROTO_TCP, linuxTCPUserTimeout, ms, "TCP_USER_TIMEOUT")
	}
	if c.BusyPoll > 0 {
		us := int(c.BusyPoll / time.Microsecond)
		if us < 1 {
			us = 1
		}
		set(syscall.SOL_SOCKET, linuxSOBusyPoll, us, "SO_BUSY_POLL")
	}
	return finishSocketOptions(c, errs)
}
func acceptedFDToConn(fd int) (net.Conn, error) {
	f := os.NewFile(uintptr(fd), "fh-kernel-accepted")
	if f == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("fh: convert accepted fd: invalid file")
	}
	c, e := net.FileConn(f)
	ce := f.Close()
	if e != nil {
		return nil, e
	}
	if ce != nil {
		_ = c.Close()
		return nil, ce
	}
	return c, nil
}
func durationSecondsCeil(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	s := int((d + time.Second - 1) / time.Second)
	if s < 1 {
		return 1
	}
	return s
}
func allowedLinuxCPUs() ([]int, error) {
	var mask [128]byte
	_, _, e := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY, 0, uintptr(len(mask)), uintptr(unsafe.Pointer(&mask[0])))
	if e != 0 {
		return nil, e
	}
	cpus := make([]int, 0, runtime.NumCPU())
	for cpu := 0; cpu < len(mask)*8; cpu++ {
		if mask[cpu/8]&(1<<uint(cpu%8)) != 0 {
			cpus = append(cpus, cpu)
		}
	}
	if len(cpus) == 0 {
		return nil, errors.New("fh: process affinity mask contains no CPUs")
	}
	return cpus, nil
}
func reactorCPU(id int, c KernelConfig) (int, error) {
	allowed, e := allowedLinuxCPUs()
	if e != nil {
		return -1, e
	}
	candidates := allowed
	if len(c.CPUSet) > 0 {
		candidates = slices.Clone(c.CPUSet)
		set := make(map[int]struct{}, len(allowed))
		for _, cpu := range allowed {
			set[cpu] = struct{}{}
		}
		for _, cpu := range candidates {
			if _, ok := set[cpu]; !ok {
				return -1, fmt.Errorf("fh: CPU %d is outside the process affinity mask", cpu)
			}
		}
	}
	if len(candidates) == 0 {
		return -1, errors.New("fh: no CPUs available for reactor affinity")
	}
	return candidates[id%len(candidates)], nil
}
func pinCurrentThread(cpu int) (func(), error) {
	if cpu < 0 {
		return func() {}, nil
	}
	runtime.LockOSThread()
	var old [128]byte
	_, _, e := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY, 0, uintptr(len(old)), uintptr(unsafe.Pointer(&old[0])))
	if e != 0 {
		runtime.UnlockOSThread()
		return nil, e
	}
	var desired [128]byte
	if cpu/8 >= len(desired) {
		runtime.UnlockOSThread()
		return nil, fmt.Errorf("fh: CPU %d exceeds affinity mask", cpu)
	}
	desired[cpu/8] = 1 << uint(cpu%8)
	_, _, e = syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, uintptr(len(desired)), uintptr(unsafe.Pointer(&desired[0])))
	if e != 0 {
		runtime.UnlockOSThread()
		return nil, e
	}
	return func() {
		_, _, _ = syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, uintptr(len(old)), uintptr(unsafe.Pointer(&old[0])))
		runtime.UnlockOSThread()
	}, nil
}
func attachReusePortCPUFilter(fd, reactors int) error {
	if reactors <= 1 {
		return nil
	}
	const (
		bpfLD    = 0x00
		bpfW     = 0x00
		bpfABS   = 0x20
		bpfALU   = 0x04
		bpfMOD   = 0x90
		bpfK     = 0x00
		bpfRET   = 0x06
		bpfA     = 0x10
		skfADOff = -0x1000
		skfADCPU = 36
	)
	offset := int32(skfADOff + skfADCPU)
	filters := []syscall.SockFilter{{Code: bpfLD | bpfW | bpfABS, K: uint32(offset)}, {Code: bpfALU | bpfMOD | bpfK, K: uint32(reactors)}, {Code: bpfRET | bpfA}}
	prog := syscall.SockFprog{Len: uint16(len(filters)), Filter: &filters[0]}
	_, _, e := syscall.RawSyscall6(syscall.SYS_SETSOCKOPT, uintptr(fd), uintptr(syscall.SOL_SOCKET), uintptr(linuxSOAttachReuseportCBPF), uintptr(unsafe.Pointer(&prog)), uintptr(unsafe.Sizeof(prog)), 0)
	if e != 0 {
		return e
	}
	return nil
}
