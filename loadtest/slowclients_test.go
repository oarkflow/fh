package loadtest

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// slowClientApp returns an app configured with short, deterministic timeouts
// so the slow-client tests below stay in the seconds range instead of
// waiting out fh's production defaults (which are tens of seconds, correctly
// so for real deployments).
func slowClientApp() *fh.App {
	app := fh.New(
		fh.WithReadHeaderTimeout(300*time.Millisecond),
		fh.WithReadTimeout(600*time.Millisecond),
		fh.WithRequestBodyTimeout(600*time.Millisecond),
		fh.WithIdleTimeout(2*time.Second),
	)
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	app.Post("/echo", func(c fh.Ctx) error { return c.Send(c.Body()) })
	return app
}

// assertConnDropped fails the test if reading from conn does not observe the
// connection being closed by the server (EOF or a reset) within timeout. It
// never blocks past timeout, so a server that hangs instead of enforcing its
// timeout fails loudly rather than stalling the test run.
func assertConnDropped(t *testing.T, conn net.Conn, timeout time.Duration, what string) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	_, err := conn.Read(buf)
	if err == nil {
		// The server might have written a response (e.g. 408) before closing;
		// drain once more, bounded, to reach the actual close.
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		_, err = conn.Read(buf)
	}
	if err == nil {
		t.Fatalf("%s: server neither closed the connection nor errored within %s", what, timeout)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: server did not enforce its timeout — connection still open after %s", what, timeout)
	}
	// Any other error (EOF, reset, closed) means the server dropped the
	// connection as expected.
}

// TestSlowHeaderTrickle sends a request one byte at a time with per-byte
// delay slow enough that the full header would not arrive until well after
// ReadHeaderTimeout. Asserts the server enforces the header timeout instead
// of waiting for the client indefinitely (concern #2).
func TestSlowHeaderTrickle(t *testing.T) {
	app := slowClientApp()
	addr := startApp(t, app)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := "GET /ping HTTP/1.1\r\nHost: localhost\r\nX-Pad: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n"
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < len(req); i++ {
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, err := conn.Write([]byte{req[i]}); err != nil {
				return
			}
			time.Sleep(15 * time.Millisecond) // ~15ms/byte, header timeout is 300ms
		}
	}()

	assertConnDropped(t, conn, 3*time.Second, "slow header trickle")
	<-done

	assertServerStillHealthy(t, addr)
}

// TestSlowlorisNoBytes opens a connection and sends nothing at all. Asserts
// the server's header-read timeout closes it rather than holding it (and its
// goroutine) open forever — the classic slowloris resource-exhaustion vector
// (concern #2).
func TestSlowlorisNoBytes(t *testing.T) {
	app := slowClientApp()
	addr := startApp(t, app)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	assertConnDropped(t, conn, 3*time.Second, "slowloris (no bytes sent)")
	assertServerStillHealthy(t, addr)
}

// TestAbortedUploadMidBody sends valid headers announcing a large
// Content-Length, writes a small amount of the body, then closes the
// connection outright (simulating a client that aborts an upload). Asserts
// the server notices the closed connection rather than hanging on the
// handler/body read forever (concern #2).
func TestAbortedUploadMidBody(t *testing.T) {
	app := slowClientApp()
	addr := startApp(t, app)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	req := fmt.Sprintf("POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", 1<<20)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write headers: %v", err)
	}
	if _, err := conn.Write([]byte("only-ten-")); err != nil {
		t.Fatalf("write partial body: %v", err)
	}
	// Abort: close without sending the rest of the announced body.
	conn.Close()

	// The server-side effect of an aborted upload isn't observable from this
	// closed connection; what matters is that the server doesn't wedge a
	// goroutine or a connection slot on it. Prove that indirectly: the server
	// must still accept and serve fresh requests promptly afterward.
	if !waitUntil(3*time.Second, 50*time.Millisecond, func() bool {
		return probeHealthy(addr)
	}) {
		t.Fatalf("server did not recover after an aborted upload")
	}
}

// TestBodyNeverCompletes sends valid headers with a Content-Length, writes
// part of the body, then goes silent without closing the connection. Asserts
// the server's body/read timeout eventually drops the connection instead of
// blocking the handler (and the goroutine/connection slot behind it)
// forever (concern #2).
func TestBodyNeverCompletes(t *testing.T) {
	app := slowClientApp()
	addr := startApp(t, app)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	req := fmt.Sprintf("POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", 1<<20)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("write headers: %v", err)
	}
	if _, err := conn.Write([]byte("partial-body-then-silence")); err != nil {
		t.Fatalf("write partial body: %v", err)
	}
	// Now go silent — never send the rest, never close.

	assertConnDropped(t, conn, 4*time.Second, "body that never completes")
	assertServerStillHealthy(t, addr)
}

// TestHalfClosedSocket opens a connection, sends a valid pipelined request,
// then half-closes the write side (TCP FIN on writes only, per CloseWrite)
// while still able to read the response. Asserts the server completes the
// in-flight request and responds normally rather than treating the
// half-close as reason to hang (concern #2).
func TestHalfClosedSocket(t *testing.T) {
	app := slowClientApp()
	addr := startApp(t, app)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("expected *net.TCPConn")
	}

	req := "GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	if _, err := tcp.Write([]byte(req)); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Half-close: no more writes are coming, but we still expect to read the
	// response on this same socket.
	if err := tcp.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	_ = tcp.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := io.ReadAll(tcp)
	if err != nil && len(resp) == 0 {
		t.Fatalf("read response after half-close: %v", err)
	}
	if status, _, perr := parseStatusAndBody(resp); perr != nil || status != 200 {
		t.Fatalf("expected 200 after half-close, got status=%d err=%v raw=%q", status, perr, string(resp))
	}
}

// assertServerStillHealthy is a stricter, immediate-fail version of the
// health probe used after tests where the server should already be idle.
func assertServerStillHealthy(t *testing.T, addr string) {
	t.Helper()
	if !waitUntil(3*time.Second, 50*time.Millisecond, func() bool { return probeHealthy(addr) }) {
		t.Fatalf("server did not recover to a healthy state at %s", addr)
	}
}

func probeHealthy(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte("GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")); err != nil {
		return false
	}
	resp, err := io.ReadAll(conn)
	if err != nil && len(resp) == 0 {
		return false
	}
	status, _, perr := parseStatusAndBody(resp)
	return perr == nil && status == 200
}
