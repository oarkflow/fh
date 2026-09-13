// Package loadtest is a separate Go module (see go.mod) so these load and
// failure tests never run as part of the root fh module's `go test ./...`.
// It requires the root module through a relative `replace` directive and
// drives real *fh.App instances over real net.Listen/net.Dial sockets.
//
// See README.md for what each file covers, how to run the short (default,
// CI-bound) suite, how to opt into a long soak run, and an honest list of
// coverage gaps.
package loadtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// isSoak reports whether the long-running soak variants should execute their
// extended duration. Short, CI-bounded behavior is the default; set
// FH_LOADTEST_SOAK=1 to opt in. See README.md for the full explanation and
// how to run a genuine multi-hour soak.
func isSoak() bool {
	return os.Getenv("FH_LOADTEST_SOAK") == "1"
}

// soakDuration returns short for a normal `go test ./...` run inside this
// module, or a much longer duration when FH_LOADTEST_SOAK=1 is set. The long
// duration itself defaults to 2h and can be overridden with
// FH_LOADTEST_SOAK_DURATION (e.g. "72h") for an actual multi-day run — pair
// that with `go test -run TestSoak -timeout 0` so the test binary's own
// timeout does not cut the run short.
func soakDuration(short time.Duration) time.Duration {
	if !isSoak() {
		return short
	}
	if v := os.Getenv("FH_LOADTEST_SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 2 * time.Hour
}

// startApp binds app to a random localhost port, serves it in the
// background, waits until it accepts connections, and registers a shutdown
// on test cleanup. It returns the address to dial.
func startApp(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.Serve(ln) }()
	t.Cleanup(func() {
		_ = app.ShutdownWithTimeout(5 * time.Second)
		select {
		case <-serveErr:
		case <-time.After(5 * time.Second):
		}
	})
	if !waitDialable(addr, 5*time.Second) {
		t.Fatalf("server did not become dialable at %s", addr)
	}
	return addr
}

// isTransientLocalDialErr reports whether err looks like momentary local
// resource pressure (most commonly "can't assign requested address" from a
// temporarily exhausted ephemeral port range) rather than the remote server
// actually refusing or being unreachable. Only errors matching this get
// retried — a real "connection refused" still fails immediately.
func isTransientLocalDialErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "can't assign requested address") ||
		strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "too many open files")
}

// dialRetry dials addr, retrying on transient local resource pressure (see
// isTransientLocalDialErr) with a short backoff. This suite dials a lot of
// real, short-lived loopback connections by design; on a busy machine
// (including, empirically, this module's own prior run still draining
// TIME_WAIT — see README.md) the local ephemeral port range can be
// momentarily exhausted for a few hundred milliseconds. That is not the
// failure mode any of these tests are trying to prove, so it's worth
// absorbing with bounded retries — a real, persistent failure to reach the
// server still surfaces promptly, since non-transient errors are not
// retried at all.
func dialRetry(addr string, timeout time.Duration) (net.Conn, error) {
	return dialRetryContext(context.Background(), addr, timeout)
}

func dialRetryContext(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if !isTransientLocalDialErr(err) {
			return nil, err
		}
	}
	return nil, lastErr
}

// waitDialable polls addr until a TCP dial succeeds or timeout elapses.
//
// It backs off longer on a transient local dial error (see
// isTransientLocalDialErr) than on an ordinary "connection refused" (the
// expected condition while the server is still binding/starting): a tight
// 5ms retry loop is fine for "not listening yet" but only makes local
// ephemeral-port/fd pressure worse by immediately trying to grab another one,
// exactly the failure mode dialRetry/dialRetryContext already guard against
// for the tests that dial the server directly. Without this distinction, a
// busy machine can burn this function's entire timeout budget hammering the
// same transient local condition instead of giving it time to clear.
func waitDialable(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		if isTransientLocalDialErr(err) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// waitUntil polls cond until it returns true or timeout elapses, returning
// whether it succeeded.
func waitUntil(timeout, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(interval)
	}
	return cond()
}

func parseStatusAndBody(resp []byte) (status int, body string, err error) {
	s := string(resp)
	lines := strings.SplitN(s, "\r\n", 2)
	if len(lines) == 0 || lines[0] == "" {
		return 0, "", fmt.Errorf("empty response")
	}
	var proto, code string
	fmt.Sscan(lines[0], &proto, &code)
	fmt.Sscan(code, &status)
	if idx := strings.Index(s, "\r\n\r\n"); idx >= 0 {
		body = s[idx+4:]
	}
	return status, body, nil
}

// closeFast closes conn, asking the kernel to abort it with RST instead of
// going through an orderly FIN/TIME_WAIT close when conn is a *net.TCPConn.
// Tests here dial thousands of short-lived loopback connections (connection
// churn, descriptor-ceiling probes, TLS handshake floods); an orderly close
// leaves each one's client-side ephemeral port stuck in TIME_WAIT for tens of
// seconds (2*MSL), and a high enough churn rate — or the same suite rerun
// again shortly after — can exhaust the local ephemeral port range and start
// failing unrelated dials with "can't assign requested address". None of
// these tests care about a graceful close on the client side, so skipping
// TIME_WAIT here is safe and keeps the suite's own footprint on the host's
// networking stack small.
func closeFast(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

// numGoroutine samples runtime.NumGoroutine after letting the scheduler settle.
func numGoroutine() int {
	runtime.Gosched()
	return runtime.NumGoroutine()
}

func heapAlloc() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// generateSelfSignedCert creates a fresh ECDSA P-256 self-signed certificate
// valid for 127.0.0.1/localhost, used so TLS tests never depend on a
// checked-in cert file. It returns the server-usable tls.Certificate plus an
// x509.CertPool a test client can use as RootCAs instead of skipping
// verification.
func generateSelfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "fh-loadtest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return cert, pool
}

// startTLSApp serves app over TLS on a random localhost port using a freshly
// generated self-signed certificate and returns the address plus a
// *tls.Config a client can use to verify the server (RootCAs set, no
// InsecureSkipVerify).
func startTLSApp(t *testing.T, app *fh.App) (addr string, clientTLSConfig *tls.Config) {
	t.Helper()
	cert, pool := generateSelfSignedCert(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = ln.Addr().String()
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	serveErr := make(chan error, 1)
	go func() { serveErr <- app.ServeTLS(ln, tlsCfg) }()
	t.Cleanup(func() {
		_ = app.ShutdownWithTimeout(5 * time.Second)
		select {
		case <-serveErr:
		case <-time.After(5 * time.Second):
		}
	})
	if !waitDialable(addr, 5*time.Second) {
		t.Fatalf("TLS server did not become dialable at %s", addr)
	}
	return addr, &tls.Config{RootCAs: pool, ServerName: "localhost"}
}

// fastCloseDialContext is an http.Transport.DialContext that retries on
// transient local dial pressure exactly like dialRetry (see its doc comment)
// and sets SO_LINGER=0 on every returned TCP connection, so the transport's
// later Close() aborts it with RST instead of going through an orderly close
// and leaving the local ephemeral port in TIME_WAIT — see closeFast's doc
// comment for why that matters for a suite that dials this many short-lived
// loopback connections.
func fastCloseDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := dialRetryContext(ctx, addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	return conn, nil
}

// httpClientFor builds a net/http client that talks HTTP/1.1 (or, when
// alpnH2 is true, negotiates real HTTP/2 via ALPN) to a TLS test server using
// the given verified client TLS config.
func httpClientFor(clientTLSConfig *tls.Config, alpnH2 bool) *http.Client {
	cfg := clientTLSConfig.Clone()
	if alpnH2 {
		cfg.NextProtos = []string{"h2"}
	} else {
		cfg.NextProtos = []string{"http/1.1"}
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: cfg,
			DialContext:     fastCloseDialContext,
		},
	}
}
