package kernel

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"runtime"
	"time"
)

type configuredKernelListener struct {
	net.Listener
	host      Host
	cfg       KernelConfig
	tlsConfig *tls.Config
}

func (l *configuredKernelListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	optionErrors, err := configurePortableTCPConn(c, l.cfg)
	l.host.socketOptionErrors(optionErrors)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if l.tlsConfig != nil {
		return tls.Server(c, l.tlsConfig), nil
	}
	return c, nil
}

func configurePortableTCPConn(c net.Conn, cfg KernelConfig) (int, error) {
	tcp, ok := c.(*net.TCPConn)
	if !ok {
		return 0, nil
	}
	var errs []error
	if cfg.TCPNoDelay {
		if err := tcp.SetNoDelay(true); err != nil {
			errs = append(errs, err)
		}
	}
	if cfg.ReceiveBufferBytes > 0 {
		if err := tcp.SetReadBuffer(cfg.ReceiveBufferBytes); err != nil {
			errs = append(errs, err)
		}
	}
	if cfg.SendBufferBytes > 0 {
		if err := tcp.SetWriteBuffer(cfg.SendBufferBytes); err != nil {
			errs = append(errs, err)
		}
	}
	if cfg.TCPKeepAlive > 0 || cfg.TCPKeepAliveIdle > 0 {
		idle := cfg.TCPKeepAliveIdle
		if idle <= 0 {
			idle = cfg.TCPKeepAlive
		}
		if err := tcp.SetKeepAliveConfig(net.KeepAliveConfig{Enable: true, Idle: idle, Interval: cfg.TCPKeepAliveIntvl, Count: cfg.TCPKeepAliveProbes}); err != nil {
			errs = append(errs, err)
		}
	}
	return finishSocketOptions(cfg, errs)
}

func listenStandard(addr string, tlsCfg *tls.Config, cfg KernelConfig, host Host, requested KernelBackend, poller string) error {
	return listenRuntime(addr, tlsCfg, cfg, host, KernelRuntimeInfo{Enabled: true, Accelerated: false, Profile: cfg.Profile, OS: runtime.GOOS, Arch: runtime.GOARCH, Backend: KernelBackendStandard, RequestedBackend: requested, NativePoller: poller, Reactors: 1})
}
func listenStandardWithFallback(addr string, tlsCfg *tls.Config, cfg KernelConfig, host Host, requested KernelBackend, poller string, cause error) error {
	return listenRuntime(addr, tlsCfg, cfg, host, KernelRuntimeInfo{Enabled: true, Accelerated: false, Profile: cfg.Profile, OS: runtime.GOOS, Arch: runtime.GOARCH, Backend: KernelBackendStandard, RequestedBackend: requested, NativePoller: poller, Reactors: 1, FallbackReason: cause.Error()})
}
func listenRuntime(addr string, tlsCfg *tls.Config, cfg KernelConfig, host Host, info KernelRuntimeInfo) error {
	ln, err := (&net.ListenConfig{KeepAlive: cfg.TCPKeepAlive}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}
	w := &configuredKernelListener{Listener: ln, host: host, cfg: cfg, tlsConfig: tlsCfg}
	if err = host.StartServing(w); err != nil {
		_ = w.Close()
		return err
	}
	host.SetRuntime(w, info)
	host.PrintStartupBanner(w)
	host.info("listening", "addr", w.Addr(), "transport", info.Backend, "poller", info.NativePoller, "reactors", info.Reactors)
	var acceptErr error
	var delay time.Duration
	for {
		c, e := w.Accept()
		if e != nil {
			if host.closed() {
				break
			}
			var ne net.Error
			if errors.As(e, &ne) && ne.Timeout() {
				continue
			}
			if isTemporaryAcceptError(e) {
				delay = nextAcceptBackoff(delay, cfg)
				host.acceptError()
				time.Sleep(delay)
				continue
			}
			acceptErr = e
			host.acceptError()
			_ = host.BeginShutdown()
			break
		}
		delay = 0
		host.accept(c)
	}
	host.FinishServing()
	return host.normalize(acceptErr)
}
func isTemporaryAcceptError(e error) bool { var n net.Error; return errors.As(e, &n) && n.Temporary() }
