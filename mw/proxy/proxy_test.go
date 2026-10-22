package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestProxyRewritePreservesTargetBasePath(t *testing.T) {
	paths := make(chan string, 1)
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.RequestURI()
		_, _ = w.Write([]byte("upstream reached"))
	}))
	upstreamListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream.Listener = upstreamListener
	upstream.Start()
	defer upstream.Close()

	app := fh.New()
	app.All("/*", New(Config{
		Target:           upstream.URL + "/base",
		StripPrefix:      "/api",
		AddPrefix:        "/v1",
		DisableSSRFGuard: true,
	}))
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	resp, err := http.Get("http://" + ln.Addr().String() + "/api/users?active=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "upstream reached" {
		t.Fatalf("unexpected proxy response: status=%d body=%q", resp.StatusCode, body)
	}
	select {
	case got := <-paths:
		if got != "/base/v1/users?active=true" {
			t.Fatalf("unexpected upstream URI: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream request was not observed")
	}
}

func TestSSRFGuardBlocksMetadataEndpointByDefault(t *testing.T) {
	denyNets, err := parseCIDRs(Config{})
	if err != nil {
		t.Fatal(err)
	}
	dial := guardedDialContext(&net.Dialer{}, denyNets)
	_, err = dial(context.Background(), "tcp", "169.254.169.254:80")
	if err == nil {
		t.Fatal("expected metadata endpoint dial to be rejected")
	}
	if !strings.Contains(err.Error(), "denied address") {
		t.Fatalf("expected denied-address error, got: %v", err)
	}
}

func TestSSRFGuardAllowsNormalUpstreamByDefault(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("upstream reached"))
	}))
	defer upstream.Close()

	denyNets, err := parseCIDRs(Config{})
	if err != nil {
		t.Fatal(err)
	}
	dial := guardedDialContext(&net.Dialer{}, denyNets)
	conn, err := dial(context.Background(), "tcp", upstream.Listener.Addr().String())
	if err != nil {
		t.Fatalf("expected normal upstream (127.0.0.1) dial to succeed, got: %v", err)
	}
	conn.Close()
}

func TestSSRFGuardDeniedCIDRsOptIn(t *testing.T) {
	denyNets, err := parseCIDRs(Config{DeniedCIDRs: []string{"127.0.0.0/8"}})
	if err != nil {
		t.Fatal(err)
	}
	dial := guardedDialContext(&net.Dialer{}, denyNets)
	_, err = dial(context.Background(), "tcp", "127.0.0.1:80")
	if err == nil {
		t.Fatal("expected loopback dial to be rejected when opted in via DeniedCIDRs")
	}
}

func TestParseCIDRsInvalidReturnsError(t *testing.T) {
	_, err := parseCIDRs(Config{DeniedCIDRs: []string{"not-a-cidr"}})
	if err == nil {
		t.Fatal("expected error for invalid CIDR")
	}
}

func TestGatewayValidatesUpstreamsAtSetup(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("invalid upstream was not rejected when Gateway was built")
		}
	}()
	_ = Gateway(map[string]Config{"/api": {Target: "://invalid"}})
}

func TestGatewayReusesUpstreamConnection(t *testing.T) {
	var connections atomic.Int32
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("upstream"))
	}))
	upstream.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	upstream.Start()
	defer upstream.Close()

	app := fh.New(fh.WithStartupBannerDisabled(true))
	app.All("/*", Gateway(map[string]Config{"/api": {Target: upstream.URL, DisableSSRFGuard: true}}))
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(listener) }()
	defer app.ShutdownWithTimeout(time.Second)
	for i := 0; i < 2; i++ {
		resp, err := http.Get("http://" + listener.Addr().String() + "/api/resource")
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("gateway request %d: response=%v err=%v", i, resp, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || string(body) != "upstream" {
			t.Fatalf("gateway response %d: body=%q err=%v", i, body, err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("gateway opened %d upstream connections for sequential requests", got)
	}
}

func TestSSRFGuardDisableSSRFGuard(t *testing.T) {
	denyNets, err := parseCIDRs(Config{DisableSSRFGuard: true})
	if err != nil {
		t.Fatal(err)
	}
	dial := guardedDialContext(&net.Dialer{Timeout: 200 * time.Millisecond}, denyNets)
	// With the guard disabled and no explicit DeniedCIDRs, even the
	// metadata address is allowed through to the dial step (it will fail to
	// connect in this sandboxed test environment, but must NOT fail with
	// our "denied address" error).
	_, err = dial(context.Background(), "tcp", "169.254.169.254:80")
	if err != nil && strings.Contains(err.Error(), "denied address") {
		t.Fatalf("expected guard to be disabled, got denied-address error: %v", err)
	}
}
