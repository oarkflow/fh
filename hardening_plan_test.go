package fh

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHealthProbeTimeoutPanicAndSingleFlight(t *testing.T) {
	a := New(WithStartupBannerDisabled(true))
	var calls atomic.Int32
	block := make(chan struct{})
	a.AddHealthCheck("stuck", 10*time.Millisecond, func(context.Context) error {
		calls.Add(1)
		<-block
		return nil
	})
	for i := 0; i < 2; i++ {
		ok, results := a.HealthStatus(context.Background())
		if ok || len(results) != 1 || results[0].Status != "error" {
			t.Fatalf("timeout health status = %v, %v", ok, results)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("stuck check ran %d times", calls.Load())
	}
	a.AddHealthCheck("stuck", 10*time.Millisecond, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	_, _ = a.HealthStatus(context.Background())
	if calls.Load() != 1 {
		t.Fatalf("replacing a stuck check started %d callbacks", calls.Load())
	}
	close(block)
	a.AddHealthCheck("panic", time.Second, func(context.Context) error { panic("probe panic") })
	ok, results := a.HealthStatus(context.Background())
	if ok || len(results) != 2 || !strings.Contains(results[1].Error, "panic") {
		t.Fatalf("panic health status = %v, %v", ok, results)
	}
}

func TestLegacyHealthProbeTimeoutAndDrainingReadiness(t *testing.T) {
	block := make(chan struct{})
	a := New(WithStartupBannerDisabled(true))
	a.HealthCheck("/health", HealthConfig{Timeout: 10 * time.Millisecond, Probes: map[string]HealthProbe{
		"stuck": func() bool { <-block; return true },
	}})
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/health", nil)
	resp, err := a.Test(req, 1000)
	close(block)
	if err != nil || resp.StatusCode != StatusServiceUnavailable {
		t.Fatalf("legacy health = %v, %v", resp, err)
	}
	b := New(WithStartupBannerDisabled(true))
	b.Use(func(c Ctx) error { b.draining.Store(true); return c.Next() })
	b.EnableHealth("/probe")
	req, _ = http.NewRequest(http.MethodGet, "http://localhost/probe/ready", nil)
	resp, err = b.Test(req, 1000)
	if err != nil || resp.StatusCode != StatusServiceUnavailable {
		t.Fatalf("draining ready = %v, %v", resp, err)
	}
}

func TestShutdownHookDeadlineAndError(t *testing.T) {
	a := New(WithStartupBannerDisabled(true))
	a.OnShutdownContext(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := a.ShutdownWithContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v", err)
	}
	b := New(WithStartupBannerDisabled(true))
	want := errors.New("cleanup failed")
	b.OnShutdownContext(func(context.Context) error { return want })
	if err := b.Shutdown(); !errors.Is(err, want) {
		t.Fatalf("cleanup error = %v", err)
	}
	c := New(WithStartupBannerDisabled(true))
	var continued atomic.Bool
	c.OnShutdownContext(func(context.Context) error { panic("hook failure") })
	c.OnShutdownContext(func(context.Context) error { continued.Store(true); return nil })
	if err := c.Shutdown(); err == nil || !strings.Contains(err.Error(), "hook failure") || !continued.Load() {
		t.Fatalf("panic cleanup = %v, continued=%v", err, continued.Load())
	}
}

func TestStartupReadyWaitsForListenHooks(t *testing.T) {
	a := New(WithStartupBannerDisabled(true))
	ready := make(chan struct{})
	a.serveReady = ready
	entered := make(chan struct{})
	release := make(chan struct{})
	want := errors.New("startup failed")
	a.OnListen(func() error { close(entered); <-release; return want })
	client, server := net.Pipe()
	defer client.Close()
	ln := newPipeListener(server)
	result := make(chan error, 1)
	go func() { result <- a.startServing(ln, false) }()
	<-entered
	select {
	case <-ready:
		t.Fatal("startup signaled before hooks finished")
	default:
	}
	close(release)
	if err := <-result; !errors.Is(err, want) {
		t.Fatalf("startup error = %v", err)
	}
	select {
	case <-ready:
		t.Fatal("failed startup signaled ready")
	default:
	}
	b := New(WithStartupBannerDisabled(true))
	b.OnListen(func() error { panic("startup panic") })
	c, s := net.Pipe()
	defer c.Close()
	if err := b.Serve(newPipeListener(s)); err == nil || !strings.Contains(err.Error(), "startup panic") {
		t.Fatalf("listen hook panic = %v", err)
	}
}

func TestDeploymentSecurityGateRequiresTLSAndSharedState(t *testing.T) {
	gate := DeploymentSecurityConfig{Mode: DeploymentDirectOrigin, ExpectedReplicas: 2, UsesReplay: true, UsesSessions: true}
	a := New(WithSecureByDefault(true), WithAllowedHosts("example.test"), WithMaxBufferedRequestBytes(1<<20), WithDeploymentSecurityGate(gate))
	client, server := net.Pipe()
	defer client.Close()
	err := a.Serve(newPipeListener(server))
	if err == nil || !strings.Contains(err.Error(), "TLS") || !strings.Contains(err.Error(), "shared replay") {
		t.Fatalf("gate error = %v", err)
	}
}

func TestDeploymentSecurityGateRejectsLocalReliabilityStores(t *testing.T) {
	a := New(WithSecureByDefault(true), WithAllowedHosts("example.test"), WithMaxBufferedRequestBytes(1<<20), WithReliability(ReliabilityConfig{Enabled: true, DataDir: t.TempDir()}), WithDeploymentSecurityGate(DeploymentSecurityConfig{Mode: DeploymentDirectOrigin, ExpectedReplicas: 2, SharedQueue: true, SharedIdempotency: true}))
	defer a.Shutdown()
	err := a.validateDeployment(nil, true)
	if err == nil || !strings.Contains(err.Error(), "file queue") || !strings.Contains(err.Error(), "file idempotency") {
		t.Fatalf("local stores gate = %v", err)
	}
}

func TestDeploymentSecurityGateDirectAndTrustedEdge(t *testing.T) {
	base := []Option{WithSecureByDefault(true), WithAllowedHosts("example.test"), WithMaxBufferedRequestBytes(1 << 20)}
	direct := New(append(base, WithDeploymentSecurityGate(DeploymentSecurityConfig{Mode: DeploymentDirectOrigin, ExpectedReplicas: 1}))...)
	if err := direct.validateDeployment(nil, true); err != nil {
		t.Fatalf("direct TLS gate: %v", err)
	}
	edge := New(append(base, WithDeploymentSecurityGate(DeploymentSecurityConfig{Mode: DeploymentTrustedEdge, ExpectedReplicas: 1, TrustedProxyCIDRs: []string{"10.2.0.0/16"}}))...)
	if err := edge.validateDeployment(nil, false); err != nil {
		t.Fatalf("trusted edge gate: %v", err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	raw := &remoteAddrConn{Conn: right, addr: &net.TCPAddr{IP: net.ParseIP("10.2.1.3"), Port: 1234}}
	if !edge.allowedEdgePeer(raw) {
		t.Fatal("trusted edge peer rejected")
	}
	proxied := &proxyConn{Conn: raw, remoteAddr: &net.TCPAddr{IP: net.ParseIP("203.0.113.5"), Port: 5555}}
	if !edge.allowedEdgePeer(proxied) {
		t.Fatal("raw trusted peer rejected after PROXY rewrite")
	}
	if edge.allowedEdgePeer(&remoteAddrConn{Conn: right, addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.5"), Port: 5555}}) {
		t.Fatal("untrusted direct peer admitted")
	}
	unsafe := New(append(base, WithDeploymentSecurityGate(DeploymentSecurityConfig{Mode: DeploymentTrustedEdge, ExpectedReplicas: 1, TrustedProxyCIDRs: []string{"0.0.0.0/0"}}))...)
	if err := unsafe.validateDeployment(nil, false); err == nil {
		t.Fatal("unrestricted proxy CIDR passed")
	}
}

type remoteAddrConn struct {
	net.Conn
	addr net.Addr
}

func (c *remoteAddrConn) RemoteAddr() net.Addr { return c.addr }

func TestDeploymentSecurityGateServesDirectTLS(t *testing.T) {
	cert := testTLSCertificate(t)
	app := New(WithStartupBannerDisabled(true), WithSecureByDefault(true), WithAllowedHosts("localhost"), WithMaxBufferedRequestBytes(1<<20), WithDeploymentSecurityGate(DeploymentSecurityConfig{Mode: DeploymentDirectOrigin, ExpectedReplicas: 1}))
	app.Get("/", func(c Ctx) error { return c.SendString("secure") })
	client, server := net.Pipe()
	ln := newPipeListener(server)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- app.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	}()
	tlsClient := tls.Client(client, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}) // test certificate
	defer tlsClient.Close()
	_ = tlsClient.SetDeadline(time.Now().Add(2 * time.Second))
	if err := tlsClient.Handshake(); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsClient.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tlsClient), nil)
	if err != nil || resp.StatusCode != StatusOK {
		t.Fatalf("direct TLS response = %v, %v", resp, err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "secure" {
		t.Fatalf("direct TLS body = %q, %v", body, err)
	}
	_ = resp.Body.Close()
	_ = tlsClient.Close()
	_ = app.ShutdownWithTimeout(time.Second)
	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("TLS serve did not stop")
	}
}

type hardeningListener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (l *hardeningListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *hardeningListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *hardeningListener) Addr() net.Addr { return &net.IPAddr{} }

func TestProxyProtocolBadAndSlowPeersDoNotStopServing(t *testing.T) {
	base := &hardeningListener{conns: make(chan net.Conn, 3), done: make(chan struct{})}
	listener := NewProxyProtocolListener(base, ProxyProtocolConfig{TrustAll: true, Timeout: time.Second, MaxPendingHandshakes: 2})
	app := New(WithStartupBannerDisabled(true))
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Serve(listener) }()
	slowClient, slowServer := net.Pipe()
	defer slowClient.Close()
	base.conns <- slowServer
	badClient, badServer := net.Pipe()
	base.conns <- badServer
	_, _ = badClient.Write([]byte("invalid protocol header\r\n"))
	_ = badClient.Close()
	goodClient, goodServer := net.Pipe()
	base.conns <- goodServer
	go func() {
		_, _ = goodClient.Write([]byte("PROXY TCP4 203.0.113.1 198.51.100.1 42 443\r\nGET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"))
	}()
	_ = goodClient.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	response, err := io.ReadAll(goodClient)
	_ = goodClient.Close()
	if err != nil || !strings.Contains(string(response), "200 OK") {
		t.Fatalf("healthy peer: %v %q", err, response)
	}
	_ = app.ShutdownWithTimeout(time.Second)
	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("serve did not stop")
	}
}

func TestBufferedBodyBudgetRelease(t *testing.T) {
	a := New(WithMaxBufferedRequestBytes(5))
	if !a.reserveBufferedBody(5) || a.reserveBufferedBody(1) {
		t.Fatal("budget admission incorrect")
	}
	a.releaseBufferedBody(5)
	if !a.reserveBufferedBody(5) {
		t.Fatal("budget was not released")
	}
	a.releaseBufferedBody(5)
}

func TestHTTP1BufferedBodyBudgetUnderConcurrentRequests(t *testing.T) {
	base := &hardeningListener{conns: make(chan net.Conn, 3), done: make(chan struct{})}
	app := New(WithStartupBannerDisabled(true), WithMaxBufferedRequestBytes(4))
	started := make(chan struct{})
	release := make(chan struct{})
	app.Post("/", func(c Ctx) error {
		if string(c.Body()) == "hold" {
			close(started)
			<-release
		}
		return c.SendString("ok")
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Serve(base) }()
	firstClient, firstServer := net.Pipe()
	base.conns <- firstServer
	go func() {
		_, _ = firstClient.Write([]byte("POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nConnection: close\r\n\r\nhold"))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach handler")
	}
	secondClient, secondServer := net.Pipe()
	base.conns <- secondServer
	go func() {
		_, _ = secondClient.Write([]byte("POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnext"))
	}()
	_ = secondClient.SetReadDeadline(time.Now().Add(time.Second))
	secondResponse, err := io.ReadAll(secondClient)
	_ = secondClient.Close()
	if err != nil || !strings.Contains(string(secondResponse), "503 Service Unavailable") {
		t.Fatalf("over-budget response: %v %q", err, secondResponse)
	}
	close(release)
	_ = firstClient.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = io.ReadAll(firstClient)
	_ = firstClient.Close()
	thirdClient, thirdServer := net.Pipe()
	base.conns <- thirdServer
	go func() {
		_, _ = thirdClient.Write([]byte("POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnext"))
	}()
	_ = thirdClient.SetReadDeadline(time.Now().Add(time.Second))
	thirdResponse, err := io.ReadAll(thirdClient)
	_ = thirdClient.Close()
	if err != nil || !strings.Contains(string(thirdResponse), "200 OK") {
		t.Fatalf("post-release response: %v %q", err, thirdResponse)
	}
	_ = app.ShutdownWithTimeout(time.Second)
	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("serve did not stop")
	}
}

func TestH2BufferedBodyBudgetReleasedOnReset(t *testing.T) {
	app := New(WithMaxBufferedRequestBytes(4))
	h := newH2Conn(app, h2discardConn{})
	s := &h2Stream{id: 1, recvWindow: h2InitialWindow}
	s.state.Store(stateOpen)
	h.streams[1] = s
	h.lastStream = 1
	if err := h.handleData(h2Frame{typ: h2Data, streamID: 1, payload: []byte("four")}); err != nil {
		t.Fatal(err)
	}
	if got := app.bufferedRequestBytes.Load(); got != 4 {
		t.Fatalf("buffered bytes = %d", got)
	}
	other := &h2Stream{id: 3, recvWindow: h2InitialWindow}
	other.state.Store(stateOpen)
	h.streams[3] = other
	h.lastStream = 3
	if err := h.handleData(h2Frame{typ: h2Data, streamID: 3, payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if !other.reset.Load() || app.bufferedRequestBytes.Load() != 4 {
		t.Fatal("over-budget stream was not reset independently")
	}
	h.resetStream(1)
	if got := app.bufferedRequestBytes.Load(); got != 0 {
		t.Fatalf("reset retained %d bytes", got)
	}
}

func TestHTTP1BufferedBodyBudgetReleasedOnAbortedUpload(t *testing.T) {
	base := &hardeningListener{conns: make(chan net.Conn, 1), done: make(chan struct{})}
	app := New(WithStartupBannerDisabled(true), WithMaxBufferedRequestBytes(8))
	app.Post("/", func(c Ctx) error { return c.SendString("ok") })
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Serve(base) }()
	client, server := net.Pipe()
	base.conns <- server
	_, err := client.Write([]byte("POST / HTTP/1.1\r\nHost: local\r\nContent-Length: 8\r\nConnection: close\r\n\r\npart"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for app.bufferedRequestBytes.Load() != 8 {
		select {
		case <-deadline:
			t.Fatal("upload did not reserve body capacity")
		case <-time.After(time.Millisecond):
		}
	}
	_ = client.Close()
	deadline = time.After(time.Second)
	for app.bufferedRequestBytes.Load() != 0 {
		select {
		case <-deadline:
			t.Fatal("aborted upload retained buffered body capacity")
		case <-time.After(time.Millisecond):
		}
	}
	if err := app.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("serve did not stop")
	}
}

func TestChunkedBodyBudgetReleaseAndRecovery(t *testing.T) {
	base := &hardeningListener{conns: make(chan net.Conn, 2), done: make(chan struct{})}
	app := New(WithStartupBannerDisabled(true), WithMaxBufferedRequestBytes(4))
	app.Post("/", func(c Ctx) error { return c.SendString("ok") })
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Serve(base) }()
	request := func(body string) string {
		client, server := net.Pipe()
		base.conns <- server
		go func() {
			_, _ = client.Write([]byte("POST / HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" + body))
		}()
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		response, _ := io.ReadAll(client)
		_ = client.Close()
		return string(response)
	}
	if response := request("4\r\nhold\r\n1\r\n"); !strings.Contains(response, "503 Service Unavailable") {
		t.Fatalf("over-budget chunked response = %q", response)
	}
	if got := app.bufferedRequestBytes.Load(); got != 0 {
		t.Fatalf("rejected chunked body retained %d bytes", got)
	}
	if response := request("4\r\nokay\r\n0\r\n\r\n"); !strings.Contains(response, "200 OK") {
		t.Fatalf("post-rejection chunked response = %q", response)
	}
	if got := app.bufferedRequestBytes.Load(); got != 0 {
		t.Fatalf("completed chunked body retained %d bytes", got)
	}
	_ = app.ShutdownWithTimeout(time.Second)
	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("serve did not stop")
	}
}
