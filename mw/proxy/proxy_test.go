package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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
