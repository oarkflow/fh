//go:build windows || solaris || illumos || aix

package kernel

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestRuntimeNativeKernelHTTP(t *testing.T) {
	c := DefaultKernelConfig()
	c.Enabled = true
	c.Required = true
	a := New(WithKernel(c), WithStartupBannerDisabled(true))
	a.Get("/kernel", func(x Ctx) error { return x.SendString("native-ok") })
	d := make(chan error, 1)
	go func() { d <- a.listenKernel("127.0.0.1:0", nil) }()
	var addr string
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		a.connMu.Lock()
		if a.listener != nil {
			addr = a.listener.Addr().String()
		}
		a.connMu.Unlock()
		if addr != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if addr == "" {
		t.Fatal("listener did not start")
	}
	n, e := net.DialTimeout("tcp", addr, time.Second)
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
	if x := string(r); !strings.Contains(x, "200 OK") || !strings.Contains(x, "native-ok") {
		t.Fatal(x)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e = a.ShutdownWithContext(ctx); e != nil {
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
	if i := a.KernelRuntimeInfo(); !i.Enabled || i.NativePoller == "" || i.Accepted == 0 {
		t.Fatalf("%+v", i)
	}
}
