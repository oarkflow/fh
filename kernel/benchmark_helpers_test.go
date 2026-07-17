package kernel

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type transportTestHost struct {
	closed       atomic.Bool
	accepted     atomic.Uint64
	acceptErrors atomic.Uint64
	socketErrors atomic.Uint64
	mu           sync.Mutex
	closer       interface{ Close() error }
	runtime      KernelRuntimeInfo
	listener     chan net.Listener
	wg           sync.WaitGroup
}

func newTransportTestHost() *transportTestHost {
	return &transportTestHost{listener: make(chan net.Listener, 1)}
}

func (h *transportTestHost) hooks() Host {
	return Host{
		StartServing: func(l net.Listener) error {
			select {
			case h.listener <- l:
			default:
			}
			return nil
		},
		FinishServing:    h.wg.Wait,
		AcceptConnection: h.accept,
		SetRuntime: func(c interface{ Close() error }, i KernelRuntimeInfo) {
			h.mu.Lock()
			h.closer, h.runtime = c, i
			h.mu.Unlock()
		},
		PrintStartupBanner:    func(net.Listener) {},
		BeginShutdown:         h.shutdown,
		Closed:                h.closed.Load,
		NormalizeServeError:   func(err error, _ bool) error { return err },
		LogInfo:               func(string, ...any) {},
		LogWarn:               func(string, ...any) {},
		AddAcceptErrors:       func(n uint64) { h.acceptErrors.Add(n) },
		AddSocketOptionErrors: func(n uint64) { h.socketErrors.Add(n) },
	}
}

func (h *transportTestHost) accept(c net.Conn) bool {
	h.accepted.Add(1)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer c.Close()
		r := bufio.NewReader(c)
		for {
			closeAfterResponse := false
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if strings.EqualFold(strings.TrimSpace(line), "Connection: close") {
					closeAfterResponse = true
				}
				if line == "\r\n" {
					break
				}
			}
			if _, err := io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nkernel-ok"); err != nil {
				return
			}
			if closeAfterResponse {
				return
			}
		}
	}()
	return true
}

func (h *transportTestHost) shutdown() error {
	h.closed.Store(true)
	h.mu.Lock()
	c := h.closer
	h.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

func (h *transportTestHost) info() KernelRuntimeInfo {
	h.mu.Lock()
	i := h.runtime
	h.mu.Unlock()
	i.Accepted = h.accepted.Load()
	i.AcceptErrors = h.acceptErrors.Load()
	i.SocketOptionErrors = h.socketErrors.Load()
	return i
}

func readBenchmarkHTTPResponse(r *bufio.Reader) error {
	status, e := r.ReadString('\n')
	if e != nil {
		return e
	}
	if !strings.Contains(status, " 200 ") {
		return fmt.Errorf("unexpected status line %q", strings.TrimSpace(status))
	}
	n := -1
	for {
		line, e := r.ReadString('\n')
		if e != nil {
			return e
		}
		if line == "\r\n" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(name), "Content-Length") {
			n, e = strconv.Atoi(strings.TrimSpace(value))
			if e != nil {
				return e
			}
		}
	}
	if n < 0 {
		return fmt.Errorf("response has no Content-Length")
	}
	_, e = io.CopyN(io.Discard, r, int64(n))
	return e
}
