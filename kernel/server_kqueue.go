//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import (
	"crypto/tls"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

type kernelAddrListener struct{ addr net.Addr }

func (l kernelAddrListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l kernelAddrListener) Close() error              { return nil }
func (l kernelAddrListener) Addr() net.Addr            { return l.addr }

type kqueueServer struct {
	host      Host
	cfg       KernelConfig
	listeners []*kernelFDListener
	shards    []*kqueueShard
	closed    atomic.Bool
	once      sync.Once
}

func (s *kqueueServer) Close() error {
	var first error
	s.once.Do(func() {
		s.closed.Store(true)
		for _, l := range s.listeners {
			if e := l.Close(); e != nil && first == nil {
				first = e
			}
		}
		for _, x := range s.shards {
			x.close()
		}
	})
	return first
}
func (s *kqueueServer) run() error {
	var w sync.WaitGroup
	ec := make(chan error, len(s.shards))
	for _, x := range s.shards {
		w.Add(1)
		go func(x *kqueueShard) {
			defer w.Done()
			if e := x.run(); e != nil {
				select {
				case ec <- e:
				default:
				}
			}
		}(x)
	}
	d := make(chan struct{})
	go func() { w.Wait(); close(d) }()
	var e error
	select {
	case e = <-ec:
		_ = s.Close()
		<-d
	case <-d:
	}
	return e
}

// Listen serves with the configured kqueue transport.
func Listen(addr string, t *tls.Config, c KernelConfig, host Host) error {
	if c.Backend == KernelBackendStandard {
		return listenStandard(addr, t, c, host, c.Backend, "kqueue")
	}
	s, i, e := newKqueueServer(host, addr, c, t)
	if e != nil {
		if c.Required {
			return e
		}
		host.warn("fh: raw kqueue unavailable; using runtime kqueue", "error", e)
		return listenStandardWithFallback(addr, t, c, host, KernelBackendKqueue, "kqueue", e)
	}
	b := kernelAddrListener{addr: s.listeners[0].Addr()}
	if e = host.StartServing(b); e != nil {
		_ = s.Close()
		return e
	}
	host.SetRuntime(s, i)
	host.PrintStartupBanner(b)
	host.info("listening", "addr", b.Addr(), "transport", i.Backend, "poller", i.NativePoller, "reactors", i.Reactors, "reuseport", i.ReusePort)
	e = s.run()
	if e != nil && !host.closed() {
		host.acceptError()
		_ = host.BeginShutdown()
	}
	host.FinishServing()
	return host.normalize(e)
}
func newKqueueServer(host Host, addr string, c KernelConfig, t *tls.Config) (*kqueueServer, KernelRuntimeInfo, error) {
	b := c.Backend
	if b == KernelBackendAuto || b == KernelBackendNative {
		b = KernelBackendKqueue
	}
	if b != KernelBackendKqueue {
		return nil, KernelRuntimeInfo{}, errors.New("fh: unsupported kqueue backend " + string(b))
	}
	s := &kqueueServer{host: host, cfg: c}
	open := addr
	for n := 0; n < c.Reactors; n++ {
		l, e := openKernelListener(open, c)
		if e != nil {
			_ = s.Close()
			return nil, KernelRuntimeInfo{}, e
		}
		s.listeners = append(s.listeners, l)
		host.socketOptionErrors(l.optionErrors)
		if n == 0 && l.addr.Port != 0 {
			h := l.addr.IP.String()
			if l.addr.IP == nil || l.addr.IP.IsUnspecified() {
				h = ""
			}
			open = net.JoinHostPort(h, strconv.Itoa(l.addr.Port))
		}
	}
	var wrap func(net.Conn) net.Conn
	if t != nil {
		wrap = func(c net.Conn) net.Conn { return tls.Server(c, t) }
	}
	for n, l := range s.listeners {
		x, e := newKqueueShard(n, l, c, host, wrap, &s.closed)
		if e != nil {
			_ = s.Close()
			return nil, KernelRuntimeInfo{}, e
		}
		s.shards = append(s.shards, x)
	}
	return s, KernelRuntimeInfo{Enabled: true, Accelerated: true, Profile: c.Profile, OS: runtime.GOOS, Arch: runtime.GOARCH, Backend: KernelBackendKqueue, RequestedBackend: c.Backend, NativePoller: "kqueue", Reactors: len(s.shards), ReusePort: c.ReusePort && len(s.shards) > 1}, nil
}
