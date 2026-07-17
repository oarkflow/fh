//go:build linux

package kernel

import (
	"crypto/tls"
	"errors"
	"fmt"
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

type kernelShard interface {
	run() error
}

type kernelCleanup interface {
	close()
}

type kernelWaker interface {
	wake()
}

type kernelServer struct {
	host      Host
	cfg       KernelConfig
	backend   KernelBackend
	listeners []*kernelFDListener
	shards    []kernelShard
	cleanups  []kernelCleanup
	closed    atomic.Bool
	closeOnce sync.Once
	xdp       *XDPManager
	xdpOwned  bool
}

func (s *kernelServer) Close() error {
	var first error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		for _, listener := range s.listeners {
			if err := listener.Close(); err != nil && first == nil {
				first = err
			}
		}
		for _, shard := range s.shards {
			if w, ok := shard.(kernelWaker); ok {
				w.wake()
			}
		}
		if s.xdpOwned && s.xdp != nil {
			if err := s.xdp.Detach(); err != nil && first == nil {
				first = err
			}
		}
	})
	return first
}

func (s *kernelServer) cleanup() {
	for _, cleanup := range s.cleanups {
		cleanup.close()
	}
}

func (s *kernelServer) run() error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(s.shards))
	for _, shard := range s.shards {
		wg.Add(1)
		go func(shard kernelShard) {
			defer wg.Done()
			if err := shard.run(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(shard)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var runErr error
	select {
	case runErr = <-errCh:
		_ = s.Close()
		<-done
	case <-done:
	}
	s.cleanup()
	return runErr
}

// Listen serves with the configured Linux kernel transport.
func Listen(addr string, tlsConfig *tls.Config, cfg KernelConfig, host Host) error {
	if cfg.Backend == KernelBackendStandard {
		return listenStandard(addr, tlsConfig, cfg, host, cfg.Backend, "epoll")
	}
	server, runtimeInfo, err := newKernelServer(host, addr, cfg, tlsConfig)
	if err != nil {
		if cfg.Required {
			return err
		}
		host.warn("fh: kernel transport unavailable; using standard listener", "error", err)
		return listenStandardWithFallback(addr, tlsConfig, cfg, host, cfg.Backend, "epoll", err)
	}

	bannerListener := kernelAddrListener{addr: server.listeners[0].Addr()}
	if err := host.StartServing(bannerListener); err != nil {
		_ = server.Close()
		server.cleanup()
		return err
	}
	host.SetRuntime(server, runtimeInfo)
	host.PrintStartupBanner(bannerListener)
	host.info("listening", "addr", bannerListener.Addr(), "transport", runtimeInfo.Backend, "reactors", runtimeInfo.Reactors, "reuseport", runtimeInfo.ReusePort, "reuseport_bpf", runtimeInfo.ReusePortBPF)

	runErr := server.run()
	if runErr != nil && !host.closed() {
		host.acceptError()
		_ = host.BeginShutdown()
	}
	host.FinishServing()
	return host.normalize(runErr)
}

func newKernelServer(host Host, addr string, cfg KernelConfig, tlsConfig *tls.Config) (*kernelServer, KernelRuntimeInfo, error) {
	info := KernelRuntimeInfo{Enabled: true, Accelerated: true, Profile: cfg.Profile, OS: runtime.GOOS, Arch: runtime.GOARCH, RequestedBackend: cfg.Backend, NativePoller: "epoll", Reactors: cfg.Reactors, ReusePort: cfg.ReusePort}
	if cfg.Backend == KernelBackendStandard {
		return nil, info, errors.New("fh: standard backend requested")
	}

	backend := cfg.Backend
	var available bool
	var features uint32
	var probeErr error
	if cfg.PreferIOUring || backend == KernelBackendIOUring {
		info.IOUringProbed = true
		available, features, probeErr = probeIOUring(cfg.IOUringEntries)
		info.IOUringAvailable = available
		info.IOUringFeatures = features
		if backend == KernelBackendIOUring && !available {
			if probeErr == nil {
				probeErr = errors.New("required io_uring network operations are unavailable")
			}
			return nil, info, fmt.Errorf("fh: io_uring requested but unavailable: %w", probeErr)
		}
	}
	if backend == KernelBackendNative {
		backend = KernelBackendAuto
	}
	if backend == KernelBackendAuto {
		if cfg.PreferIOUring && available {
			backend = KernelBackendIOUring
		} else {
			backend = KernelBackendEpoll
			if cfg.PreferIOUring && probeErr != nil {
				info.FallbackReason = "io_uring unavailable: " + probeErr.Error()
			}
		}
	}

	server := &kernelServer{host: host, cfg: cfg, backend: backend}
	openAddr := addr
	for i := 0; i < cfg.Reactors; i++ {
		listener, err := openKernelListener(openAddr, cfg)
		if err != nil {
			_ = server.Close()
			return nil, info, err
		}
		server.listeners = append(server.listeners, listener)
		host.socketOptionErrors(listener.optionErrors)
		if i == 0 && listener.addr.Port != 0 {
			openAddr = net.JoinHostPort(listener.addr.IP.String(), strconv.Itoa(listener.addr.Port))
		}
	}
	if cfg.ReusePortBPF && len(server.listeners) > 1 {
		if err := attachReusePortCPUFilter(server.listeners[0].fd, len(server.listeners)); err != nil {
			if cfg.Required {
				_ = server.Close()
				return nil, info, fmt.Errorf("fh: attach reuseport BPF: %w", err)
			}
			host.warn("fh: reuseport BPF CPU steering unavailable", "error", err)
			info.FallbackReason = joinFallback(info.FallbackReason, "reuseport BPF unavailable: "+err.Error())
		} else {
			info.ReusePortBPF = true
		}
	}

	var tlsWrap func(net.Conn) net.Conn
	if tlsConfig != nil {
		tlsWrap = func(conn net.Conn) net.Conn { return tls.Server(conn, tlsConfig) }
	}

	buildShards := func(selected KernelBackend) error {
		server.shards = nil
		server.cleanups = nil
		for i, listener := range server.listeners {
			switch selected {
			case KernelBackendIOUring:
				shard, err := newIOUringShard(i, listener, cfg, host, tlsWrap, &server.closed)
				if err != nil {
					for _, c := range server.cleanups {
						c.close()
					}
					server.cleanups = nil
					return err
				}
				server.shards = append(server.shards, shard)
				server.cleanups = append(server.cleanups, shard)
			case KernelBackendEpoll:
				shard, err := newEpollShard(i, listener, cfg, host, tlsWrap, &server.closed)
				if err != nil {
					for _, c := range server.cleanups {
						c.close()
					}
					server.cleanups = nil
					return err
				}
				server.shards = append(server.shards, shard)
				server.cleanups = append(server.cleanups, shard)
			default:
				return fmt.Errorf("fh: unsupported kernel backend %q", selected)
			}
		}
		return nil
	}

	if err := buildShards(backend); err != nil {
		if backend == KernelBackendIOUring && !cfg.Required {
			info.FallbackReason = joinFallback(info.FallbackReason, "io_uring reactor unavailable: "+err.Error())
			backend = KernelBackendEpoll
			if epollErr := buildShards(backend); epollErr != nil {
				_ = server.Close()
				return nil, info, errors.Join(err, epollErr)
			}
		} else {
			_ = server.Close()
			return nil, info, err
		}
	}
	server.backend = backend
	info.Backend = backend
	info.IOUringNetworkIO = backend == KernelBackendIOUring

	if cfg.XDP.Enabled && cfg.XDP.AutoAttach {
		if len(cfg.XDP.ProtectedPorts) == 0 && len(server.listeners) > 0 && server.listeners[0].addr.Port > 0 {
			cfg.XDP.ProtectedPorts = []uint16{uint16(server.listeners[0].addr.Port)}
		}
		if cfg.XDP.PinPath == "" {
			cfg.XDP.PinPath = DefaultXDPPinPath(cfg.XDP.Interface)
		}
		manager := NewXDPManager(cfg.XDP)
		if err := manager.Attach(); err != nil {
			if cfg.XDP.Required || cfg.Required {
				_ = server.Close()
				return nil, info, fmt.Errorf("fh: attach XDP: %w", err)
			}
			host.warn("fh: XDP auto-attach unavailable", "interface", cfg.XDP.Interface, "error", err)
			info.FallbackReason = joinFallback(info.FallbackReason, "XDP unavailable: "+err.Error())
		} else {
			server.xdp = manager
			server.xdpOwned = true
			info.XDPAttached = true
			info.XDPInterface = cfg.XDP.Interface
		}
	}

	if cfg.PinThreads && cfg.Reactors > runtime.NumCPU() {
		info.FallbackReason = joinFallback(info.FallbackReason, "reactors exceed online CPUs; affinity wraps")
	}
	return server, info, nil
}

func joinFallback(current, next string) string {
	if current == "" {
		return next
	}
	return current + "; " + next
}
