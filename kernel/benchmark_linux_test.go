//go:build linux

package kernel

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func BenchmarkKernelEpollHTTPKeepAlive(b *testing.B) {
	benchmarkKernelHTTPKeepAlive(b, KernelBackendEpoll)
}

func BenchmarkKernelIOUringHTTPKeepAlive(b *testing.B) {
	available, _, err := probeIOUring(256)
	if !available {
		b.Skipf("io_uring network operations unavailable: %v", err)
	}
	benchmarkKernelHTTPKeepAlive(b, KernelBackendIOUring)
}

func benchmarkKernelHTTPKeepAlive(b *testing.B, backend KernelBackend) {
	b.Helper()
	cfg := HighPerformanceKernelConfig()
	cfg.Required = true
	cfg.Backend = backend
	cfg.Reactors = 1
	cfg.ReusePort = false
	cfg.ReusePortBPF = false
	cfg.PinThreads = false
	cfg.TCPFastOpenQueue = 0
	cfg.TCPDeferAccept = 0

	host := newTransportTestHost()
	server, info, err := newKernelServer(host.hooks(), "127.0.0.1:0", cfg, nil)
	if err != nil {
		b.Fatal(err)
	}
	listener := kernelAddrListener{addr: server.listeners[0].Addr()}
	host.hooks().SetRuntime(server, info)

	done := make(chan error, 1)
	go func() {
		err := server.run()
		done <- err
	}()

	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	reader := bufio.NewReaderSize(conn, 4096)
	request := []byte("GET /bench HTTP/1.1\r\nHost: benchmark\r\n\r\n")

	b.Cleanup(func() {
		_ = conn.Close()
		_ = host.shutdown()
		select {
		case err := <-done:
			if err != nil {
				b.Errorf("kernel server: %v", err)
			}
		case <-time.After(3 * time.Second):
			b.Error("kernel benchmark server did not stop")
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(len(request) + 2))
	b.ResetTimer()
	for range b.N {
		if _, err := conn.Write(request); err != nil {
			b.Fatal(err)
		}
		if err := readBenchmarkHTTPResponse(reader); err != nil {
			b.Fatal(err)
		}
	}
}
