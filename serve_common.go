package fh

import (
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"

	"github.com/oarkflow/fh/kernel"
)

type kernelCounters struct {
	accepted           atomic.Uint64
	acceptErrors       atomic.Uint64
	dropped            atomic.Uint64
	pinned             atomic.Int32
	active             atomic.Uint64
	peak               atomic.Uint64
	rejectedGlobal     atomic.Uint64
	rejectedPerIP      atomic.Uint64
	socketOptionErrors atomic.Uint64
}

func (a *App) startServing(ln net.Listener) error {
	a.buildMu.Lock()
	if !a.started.CompareAndSwap(false, true) {
		a.buildMu.Unlock()
		return ErrAppAlreadyStarted
	}
	a.buildMu.Unlock()

	a.connMu.Lock()
	a.listener = ln
	a.connMu.Unlock()
	a.closed.Store(false)
	a.draining.Store(false)

	if a.reliability != nil {
		if err := a.reliability.Start(); err != nil {
			if ln != nil {
				_ = ln.Close()
			}
			return err
		}
	}
	a.router.Freeze()
	for _, fn := range a.hooks.onListen {
		if err := fn(); err != nil {
			if ln != nil {
				_ = ln.Close()
			}
			return err
		}
	}
	return nil
}

func (a *App) finishServing() {
	a.activeConn.Wait()
	a.runShutdownHooks()
}

// acceptConnection applies process-wide and peer-specific admission limits,
// registers lifecycle state, and transfers the connection to the established
// HTTP/TLS protocol engine. It is safe to call concurrently from reactor shards.
func (a *App) acceptConnection(conn net.Conn) bool {
	if conn == nil {
		return false
	}
	if a.sem != nil {
		select {
		case a.sem <- struct{}{}:
		default:
			_ = conn.Close()
			a.kernelCounters.dropped.Add(1)
			a.kernelCounters.rejectedGlobal.Add(1)
			return false
		}
	}
	peerIP, reserved := a.reservePeer(conn.RemoteAddr())
	if !reserved {
		_ = conn.Close()
		if a.sem != nil {
			<-a.sem
		}
		a.kernelCounters.dropped.Add(1)
		a.kernelCounters.rejectedPerIP.Add(1)
		return false
	}

	a.connMu.Lock()
	if a.draining.Load() {
		a.connMu.Unlock()
		_ = conn.Close()
		a.releasePeer(peerIP)
		if a.sem != nil {
			<-a.sem
		}
		return false
	}
	a.activeConn.Add(1)
	a.conns[conn] = &connState{writeBuf: make([]byte, 0, 4096)}
	a.connMu.Unlock()
	a.kernelCounters.accepted.Add(1)
	active := a.kernelCounters.active.Add(1)
	for {
		peak := a.kernelCounters.peak.Load()
		if active <= peak || a.kernelCounters.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	go a.serveConn(conn, peerIP)
	return true
}

func (a *App) setKernelRuntime(info KernelRuntimeInfo) {
	a.kernelMu.Lock()
	a.kernelRuntime = info
	a.kernelMu.Unlock()
}

func (a *App) KernelRuntimeInfo() KernelRuntimeInfo {
	if a == nil {
		return KernelRuntimeInfo{}
	}
	a.kernelMu.RLock()
	info := a.kernelRuntime
	a.kernelMu.RUnlock()
	info.Accepted = a.kernelCounters.accepted.Load()
	info.AcceptErrors = a.kernelCounters.acceptErrors.Load()
	info.Dropped = a.kernelCounters.dropped.Load()
	info.ThreadsPinned = int(a.kernelCounters.pinned.Load())
	info.ActiveConnections = a.kernelCounters.active.Load()
	info.PeakConnections = a.kernelCounters.peak.Load()
	info.RejectedGlobal = a.kernelCounters.rejectedGlobal.Load()
	info.RejectedPerIP = a.kernelCounters.rejectedPerIP.Load()
	info.SocketOptionErrors = a.kernelCounters.socketOptionErrors.Load()
	return info
}

func normalizeServeError(err error, closed bool) error {
	if closed && (errors.Is(err, net.ErrClosed) || isExpectedConnErr(err)) {
		return nil
	}
	return err
}

func (a *App) listenKernel(addr string, tlsConfig *tls.Config) error {
	host := kernel.Host{
		StartServing:        a.startServing,
		FinishServing:       a.finishServing,
		AcceptConnection:    a.acceptConnection,
		PrintStartupBanner:  a.printStartupBanner,
		BeginShutdown:       a.beginShutdown,
		Closed:              a.closed.Load,
		NormalizeServeError: normalizeServeError,
		LogInfo:             a.logger.Info,
		LogWarn:             a.logger.Warn,
		AddAcceptErrors:     func(n uint64) { a.kernelCounters.acceptErrors.Add(n) },
		AddPinnedThreads:    func(n int32) { a.kernelCounters.pinned.Add(n) },
		AddSocketOptionErrors: func(n uint64) {
			a.kernelCounters.socketOptionErrors.Add(n)
		},
		SetRuntime: func(closer interface{ Close() error }, info kernel.KernelRuntimeInfo) {
			a.kernelMu.Lock()
			a.kernelCloser = closer
			a.kernelRuntime = info
			a.kernelMu.Unlock()
		},
	}
	return kernel.Listen(addr, tlsConfig, a.cfg.Kernel, host)
}
