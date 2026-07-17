//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package kernel

import (
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestKernelKqueueHTTP(t *testing.T) {
	c := DefaultKernelConfig()
	c.Enabled = true
	c.Required = true
	c.Backend = KernelBackendKqueue
	c.Reactors = 1
	c.ReusePort = false
	h := newTransportTestHost()
	s, i, e := newKqueueServer(h.hooks(), "127.0.0.1:0", c, nil)
	if e != nil {
		if errors.Is(e, syscall.EPERM) {
			t.Skipf("sandbox blocks listener setup: %v", e)
		}
		t.Fatal(e)
	}
	l := kernelAddrListener{addr: s.listeners[0].Addr()}
	h.hooks().SetRuntime(s, i)
	d := make(chan error, 1)
	go func() { d <- s.run() }()
	n, e := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	_, e = n.Write([]byte("GET /kernel HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"))
	if e != nil {
		t.Fatal(e)
	}
	r, e := io.ReadAll(n)
	_ = n.Close()
	if e != nil {
		t.Fatal(e)
	}
	if x := string(r); !strings.Contains(x, "200 OK") || !strings.Contains(x, "kernel-ok") {
		t.Fatal(x)
	}
	if e = h.shutdown(); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-d:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not stop")
	}
	got := h.info()
	if got.Backend != KernelBackendKqueue || got.Accepted == 0 {
		t.Fatalf("%+v", got)
	}
}
