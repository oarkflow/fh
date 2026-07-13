package proxy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
