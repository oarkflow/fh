//go:build windows || solaris || illumos || aix

package kernel

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func BenchmarkKernelRuntimeNativeHTTPKeepAlive(b *testing.B) {
	c := HighPerformanceKernelConfig()
	c.Required = true
	c.Backend = KernelBackendNative
	c.Reactors = 1
	c.ReusePort = false
	h := newTransportTestHost()
	d := make(chan error, 1)
	go func() { d <- Listen("127.0.0.1:0", nil, c, h.hooks()) }()
	var listener net.Listener
	select {
	case listener = <-h.listener:
	case <-time.After(3 * time.Second):
		b.Fatal("listener did not start")
	}
	n, e := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if e != nil {
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
