//go:build linux

package kernel

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestIOUringABISizes(t *testing.T) {
	if got := unsafe.Sizeof(ioUringSQE{}); got != 64 {
		t.Fatalf("io_uring SQE ABI size=%d want=64", got)
	}
	if got := unsafe.Sizeof(ioUringCQE{}); got != 16 {
		t.Fatalf("io_uring CQE ABI size=%d want=16", got)
	}
	if got := unsafe.Sizeof(ioUringParams{}); got != 120 {
		t.Fatalf("io_uring params ABI size=%d want=120", got)
	}
}

func TestPortKeyUsesNetworkByteOrder(t *testing.T) {
	got := portKey(0x1234)
	if len(got) != 2 || got[0] != 0x12 || got[1] != 0x34 {
		t.Fatalf("port key=%x want=1234", got)
	}
}

func TestKernelEpollHTTP(t *testing.T) {
	info := runKernelHTTPIntegration(t, KernelBackendEpoll, 1)
	if info.Backend != KernelBackendEpoll || info.Accepted == 0 {
		t.Fatalf("unexpected runtime info: %+v", info)
	}
}

func TestKernelEpollReusePortHTTP(t *testing.T) {
	info := runKernelHTTPIntegration(t, KernelBackendEpoll, 2)
	if info.Backend != KernelBackendEpoll || !info.ReusePort || info.Accepted < 4 {
		t.Fatalf("unexpected runtime info: %+v", info)
	}
}

func TestKernelIOUringHTTP(t *testing.T) {
	available, _, err := probeIOUring(64)
	if !available {
		t.Skipf("io_uring network operations unavailable: %v", err)
	}
	info := runKernelHTTPIntegration(t, KernelBackendIOUring, 1)
	if info.Backend != KernelBackendIOUring || !info.IOUringNetworkIO || info.Accepted == 0 {
		t.Fatalf("unexpected runtime info: %+v", info)
	}
}

func runKernelHTTPIntegration(t *testing.T, backend KernelBackend, reactors int) KernelRuntimeInfo {
	t.Helper()
	cfg := DefaultKernelConfig()
	cfg.Enabled = true
	cfg.Required = true
	cfg.Backend = backend
	cfg.Reactors = reactors
	cfg.ReusePort = reactors > 1
	cfg.ReusePortBPF = false
	cfg.PinThreads = false
	cfg.TCPFastOpenQueue = 0
	cfg.TCPDeferAccept = 0

	host := newTransportTestHost()
	server, info, err := newKernelServer(host.hooks(), "127.0.0.1:0", cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener := kernelAddrListener{addr: server.listeners[0].Addr()}
	host.hooks().SetRuntime(server, info)

	done := make(chan error, 1)
	go func() {
		err := server.run()
		done <- err
	}()

	stopped := false
	t.Cleanup(func() {
		if stopped {
			return
		}
		_ = host.shutdown()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	requests := 1
	if reactors > 1 {
		requests = 4
	}
	for range requests {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("GET /kernel HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		response, err := io.ReadAll(conn)
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		text := string(response)
		if !strings.Contains(text, "200 OK") || !strings.Contains(text, "kernel-ok") {
			t.Fatalf("unexpected response: %q", text)
		}
	}

	if err := host.shutdown(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("kernel server did not stop")
	}
	stopped = true
	return host.info()
}

func TestIOUringCompletionKeepsTimeoutMemoryPinned(t *testing.T) {
	shard := &ioUringShard{}
	req := &ioUringRequest{opID: 2, timeoutID: 3, read: true, done: make(chan ioUringResult, 1)}
	shard.requests.Store(req.opID, ioUringRequestRef{req: req})
	shard.requests.Store(req.timeoutID, ioUringRequestRef{req: req, timeout: true})
	shard.requestCount.Store(1)

	shard.completeIO(ioUringCQE{UserData: req.opID, Res: 7})
	select {
	case <-req.done:
		t.Fatal("operation completed before linked timeout CQE released its memory")
	default:
	}
	shard.completeIO(ioUringCQE{UserData: req.timeoutID, Res: -int32(syscall.ECANCELED)})
	select {
	case result := <-req.done:
		if result.n != 7 || result.err != nil {
			t.Fatalf("unexpected result: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("linked completion did not finish")
	}
}

func TestIOUringLinkedTimeoutResult(t *testing.T) {
	shard := &ioUringShard{}
	req := &ioUringRequest{opID: 4, timeoutID: 5, done: make(chan ioUringResult, 1)}
	shard.requests.Store(req.opID, ioUringRequestRef{req: req})
	shard.requests.Store(req.timeoutID, ioUringRequestRef{req: req, timeout: true})
	shard.requestCount.Store(1)

	shard.completeIO(ioUringCQE{UserData: req.timeoutID, Res: -int32(syscall.ETIME)})
	shard.completeIO(ioUringCQE{UserData: req.opID, Res: -int32(syscall.ECANCELED)})
	select {
	case result := <-req.done:
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("error=%v want deadline exceeded", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout completion did not finish")
	}
}

func TestProductionAutoUsesEpollWithoutIOUringProbe(t *testing.T) {
	cfg := ProductionKernelConfig()
	cfg.Required = true
	cfg.Reactors = 1
	cfg.ReusePort = false
	cfg.TCPFastOpenQueue = 0
	cfg.TCPDeferAccept = 0
	host := newTransportTestHost()
	server, info, err := newKernelServer(host.hooks(), "127.0.0.1:0", cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(); server.cleanup() }()
	if info.Backend != KernelBackendEpoll {
		t.Fatalf("backend=%s want=%s", info.Backend, KernelBackendEpoll)
	}
	if info.IOUringProbed {
		t.Fatal("balanced production profile must not probe io_uring")
	}
}

func TestReactorCPURespectsProcessAffinity(t *testing.T) {
	allowed, err := allowedLinuxCPUs()
	if err != nil {
		t.Fatal(err)
	}
	cfg := ProductionKernelConfig()
	cfg.CPUSet = []int{allowed[len(allowed)-1]}
	cpu, err := reactorCPU(9, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if cpu != cfg.CPUSet[0] {
		t.Fatalf("cpu=%d want=%d", cpu, cfg.CPUSet[0])
	}
}
