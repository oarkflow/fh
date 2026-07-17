//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func BenchmarkKernelKqueueHTTPKeepAlive(b *testing.B) {
	c := HighPerformanceKernelConfig()
	c.Required = true
	c.Backend = KernelBackendKqueue
	c.Reactors = 1
	c.ReusePort = false
	c.ReusePortBPF = false
	c.PinThreads = false
	h := newTransportTestHost()
	s, i, e := newKqueueServer(h.hooks(), "127.0.0.1:0", c, nil)
	if e != nil {
		b.Fatal(e)
	}
	l := kernelAddrListener{addr: s.listeners[0].Addr()}
	h.hooks().SetRuntime(s, i)
	d := make(chan error, 1)
	go func() { d <- s.run() }()
	n, e := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if e != nil {
		_ = s.Close()
		b.Fatal(e)
	}
	r := bufio.NewReaderSize(n, 4096)
	req := []byte("GET /bench HTTP/1.1\r\nHost: benchmark\r\n\r\n")
	b.Cleanup(func() {
		_ = n.Close()
		_ = h.shutdown()
		select {
		case e := <-d:
			if e != nil {
				b.Errorf("server: %v", e)
			}
		case <-time.After(3 * time.Second):
			b.Error("did not stop")
		}
	})
	b.ReportAllocs()
	b.SetBytes(int64(len(req) + 2))
	b.ResetTimer()
	for range b.N {
		if _, e := n.Write(req); e != nil {
			b.Fatal(e)
		}
		if e := readBenchmarkHTTPResponse(r); e != nil {
			b.Fatal(e)
		}
	}
}
