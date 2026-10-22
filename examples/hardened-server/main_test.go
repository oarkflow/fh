package main

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestBuildAppRequiresDeploymentInputs(t *testing.T) {
	tests := []struct {
		name string
		opt  serverOptions
	}{
		{"host", serverOptions{mode: fh.DeploymentDirectOrigin, certFile: "cert", keyFile: "key"}},
		{"mode", serverOptions{host: "api.example.test"}},
		{"certificate", serverOptions{mode: fh.DeploymentDirectOrigin, host: "api.example.test"}},
		{"edge peers", serverOptions{mode: fh.DeploymentTrustedEdge, host: "api.example.test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if app, err := buildApp(tt.opt); err == nil || app != nil {
				t.Fatalf("buildApp() = %v, %v", app, err)
			}
		})
	}
}

func TestExamplePublicRoutesAndHostPolicy(t *testing.T) {
	app, err := buildApp(serverOptions{mode: fh.DeploymentTrustedEdge, host: "api.example.test", trustedCIDRs: []string{"127.0.0.1/32"}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(listener) }()
	defer app.ShutdownWithTimeout(time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	request := func(path, host string) (*http.Response, error) {
		req, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+path, nil)
		if err != nil {
			return nil, err
		}
		req.Host = host
		return client.Do(req)
	}
	for _, path := range []string{"/", "/health/live", "/health/ready"} {
		resp, err := request(path, "api.example.test")
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: response=%v err=%v", path, resp, err)
		}
		_ = resp.Body.Close()
	}
	resp, err := request("/", "other.example.test")
	if err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unlisted Host: response=%v err=%v", resp, err)
	}
	_ = resp.Body.Close()
}

func TestExampleStartupGate(t *testing.T) {
	direct, err := buildApp(serverOptions{mode: fh.DeploymentDirectOrigin, host: "api.example.test", certFile: "cert", keyFile: "key"})
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.Serve(closedListener{}); err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("direct origin served plaintext: %v", err)
	}
	edge, err := buildApp(serverOptions{mode: fh.DeploymentTrustedEdge, host: "api.example.test", trustedCIDRs: []string{"0.0.0.0/0"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := edge.Serve(closedListener{}); err == nil || !strings.Contains(err.Error(), "unrestricted trusted proxy CIDR") {
		t.Fatalf("edge accepted unrestricted peers: %v", err)
	}
}

type closedListener struct{}

func (closedListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (closedListener) Close() error              { return nil }
func (closedListener) Addr() net.Addr            { return &net.TCPAddr{} }
