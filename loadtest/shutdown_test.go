package loadtest

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/websocket"
)

// assertListenerRejectsNewConns polls for new TCP dials to addr to start
// failing, which is what fh's graceful shutdown does immediately (it closes
// the listener at the top of beginShutdown before waiting for in-flight work
// to drain).
func assertListenerRejectsNewConns(t *testing.T, addr string, within time.Duration) {
	t.Helper()
	if !waitUntil(within, 20*time.Millisecond, func() bool {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
		}
		return err != nil
	}) {
		t.Fatalf("listener still accepted new connections %s after shutdown began", within)
	}
}

// TestGracefulShutdownHTTP1CompletesInFlightAndRejectsNew starts a real
// HTTP/1 keep-alive-capable server, puts one request mid-flight in a slow
// handler, begins a graceful shutdown while it is running, and asserts: the
// in-flight request completes normally rather than being cut off, new
// connections are rejected once shutdown begins, and Shutdown itself returns
// within its timeout rather than hanging (concern #7).
func TestGracefulShutdownHTTP1CompletesInFlightAndRejectsNew(t *testing.T) {
	app := fh.New()
	started := make(chan struct{})
	var once sync.Once
	app.Get("/slow", func(c fh.Ctx) error {
		once.Do(func() { close(started) })
		time.Sleep(400 * time.Millisecond)
		return c.SendString("done")
	})
	addr := startApp(t, app)

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("in-flight request never reached the handler")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- app.ShutdownWithTimeout(3 * time.Second) }()

	assertListenerRejectsNewConns(t, addr, time.Second)

	select {
	case err := <-errCh:
		t.Fatalf("in-flight request was cut off during shutdown: %v", err)
	case resp := <-respCh:
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != "done" {
			t.Fatalf("in-flight request during shutdown = status %d body %q, want 200 %q", resp.StatusCode, body, "done")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("in-flight request did not complete during graceful shutdown")
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Shutdown did not return within its timeout")
	}
}

// TestGracefulShutdownHTTP2CompletesInFlightStream is the HTTP/2 counterpart:
// a real golang.org/x/net/http2 client over real TLS (ALPN-negotiated h2),
// mid-request against a slow handler, while the server begins a graceful
// shutdown. Asserts the in-flight HTTP/2 stream completes and Shutdown
// returns within its timeout (concern #7).
func TestGracefulShutdownHTTP2CompletesInFlightStream(t *testing.T) {
	app := fh.New()
	started := make(chan struct{})
	var once sync.Once
	app.Get("/slow", func(c fh.Ctx) error {
		once.Do(func() { close(started) })
		time.Sleep(400 * time.Millisecond)
		return c.SendString("done")
	})
	addr, clientTLSConfig := startTLSApp(t, app)

	h2Transport := &http2.Transport{TLSClientConfig: clientTLSConfig.Clone()}
	client := &http.Client{Transport: h2Transport, Timeout: 5 * time.Second}

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := client.Get("https://" + addr + "/slow")
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("in-flight HTTP/2 request never reached the handler")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- app.ShutdownWithTimeout(3 * time.Second) }()

	select {
	case err := <-errCh:
		t.Fatalf("in-flight HTTP/2 stream was cut off during shutdown: %v", err)
	case resp := <-respCh:
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 || string(body) != "done" {
			t.Fatalf("in-flight HTTP/2 request during shutdown = proto %d status %d body %q, want HTTP/2 200 %q", resp.ProtoMajor, resp.StatusCode, body, "done")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("in-flight HTTP/2 stream did not complete during graceful shutdown")
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Shutdown did not return within its timeout")
	}
}

// TestGracefulShutdownDrainsActiveSSEStream starts a real Server-Sent Events
// stream (fh's Ctx.SSE), begins a graceful shutdown while the stream is still
// writing events, and asserts the stream is allowed to finish and deliver
// all its events to the client rather than being severed mid-stream, and that
// Shutdown still returns within its timeout (concern #7).
func TestGracefulShutdownDrainsActiveSSEStream(t *testing.T) {
	app := fh.New()
	const events = 5
	started := make(chan struct{})
	var once sync.Once
	app.Get("/events", func(c fh.Ctx) error {
		return c.SSE(func(s *fh.SSE) error {
			once.Do(func() { close(started) })
			for i := 0; i < events; i++ {
				if err := s.SendEvent("tick", i); err != nil {
					return err
				}
				time.Sleep(60 * time.Millisecond)
			}
			return nil
		})
	})
	addr := startApp(t, app)

	bodyCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/events")
		if err != nil {
			errCh <- err
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			errCh <- err
			return
		}
		bodyCh <- string(b)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("SSE stream never started")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- app.ShutdownWithTimeout(3 * time.Second) }()

	select {
	case err := <-errCh:
		t.Fatalf("SSE stream errored instead of draining during shutdown: %v", err)
	case body := <-bodyCh:
		if got := strings.Count(body, "event: tick"); got != events {
			t.Fatalf("SSE stream delivered %d/%d events before being cut off during shutdown, body=%q", got, events, body)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("SSE stream did not finish draining during graceful shutdown")
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Shutdown did not return within its timeout")
	}
}

// TestGracefulShutdownForcesClosedWebSocketWithinTimeout opens a real
// WebSocket connection (raw handshake + masked client frames against
// pkg/websocket), proves live traffic with an echo round-trip, then begins a
// graceful shutdown while the socket is idle-but-open. A long-lived
// full-duplex connection like this has no natural "response complete" point
// the way a request/response or a stream does, so fh's contract is: shutdown
// must still return within its configured timeout (forcing the connection
// closed if it has to) rather than hanging forever waiting for the client to
// leave (concern #7).
func TestGracefulShutdownForcesClosedWebSocketWithinTimeout(t *testing.T) {
	app := fh.New()
	app.Get("/ws", websocket.New(func(c *websocket.Conn) error {
		for {
			op, payload, err := c.ReadMessage()
			if err != nil {
				return err
			}
			if op == websocket.Text {
				if err := c.WriteMessage(websocket.Text, payload); err != nil {
					return err
				}
			}
		}
	}))
	addr := startApp(t, app)

	conn, err := dialWebSocket(t, addr, "/ws")
	if err != nil {
		t.Fatalf("websocket handshake: %v", err)
	}
	defer conn.Close()

	if err := writeWSTextFrame(conn, "hello"); err != nil {
		t.Fatalf("write ws frame: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	op, payload, err := readWSFrame(conn)
	if err != nil || op != websocket.Text || string(payload) != "hello" {
		t.Fatalf("echo round-trip = op=%d payload=%q err=%v, want text %q", op, payload, err, "hello")
	}

	const shutdownTimeout = 800 * time.Millisecond
	shutdownDone := make(chan error, 1)
	start := time.Now()
	go func() { shutdownDone <- app.ShutdownWithTimeout(shutdownTimeout) }()

	select {
	case <-shutdownDone:
		elapsed := time.Since(start)
		if elapsed > shutdownTimeout+2*time.Second {
			t.Fatalf("Shutdown took %s, far beyond its %s timeout", elapsed, shutdownTimeout)
		}
	case <-time.After(shutdownTimeout + 3*time.Second):
		t.Fatalf("Shutdown did not return within a generous margin of its %s timeout — it hung", shutdownTimeout)
	}

	// The websocket connection must actually be gone now, not left dangling.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err == nil {
		t.Fatalf("websocket connection is still open after Shutdown returned")
	}
}

// ── minimal raw WebSocket client (handshake + unfragmented text frames) ────
// fh has no client-side WebSocket helper; this is deliberately small and
// only supports what these tests need: a masked client->server text frame
// and an unmasked, unfragmented server->client text frame reply.

func dialWebSocket(t *testing.T, addr, path string) (net.Conn, error) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)

	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n\r\n"
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}

	// Read just the status line and headers (up to \r\n\r\n); no body is
	// expected on a 101 response.
	buf := make([]byte, 4096)
	total := 0
	for {
		n, err := conn.Read(buf[total:])
		if err != nil {
			conn.Close()
			return nil, err
		}
		total += n
		if idx := strings.Index(string(buf[:total]), "\r\n\r\n"); idx >= 0 {
			break
		}
		if total == len(buf) {
			conn.Close()
			return nil, io.ErrShortBuffer
		}
	}
	statusLine := strings.SplitN(string(buf[:total]), "\r\n", 2)[0]
	if !strings.Contains(statusLine, "101") {
		conn.Close()
		return nil, &net.OpError{Op: "handshake", Err: errString(statusLine)}
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

type errString string

func (e errString) Error() string { return string(e) }

// writeWSTextFrame writes a single, final, masked text frame — required for
// client-to-server frames per RFC 6455.
func writeWSTextFrame(conn net.Conn, msg string) error {
	payload := []byte(msg)
	var mask [4]byte
	_, _ = rand.Read(mask[:])
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	frame := []byte{0x81} // FIN + text opcode
	switch {
	case len(payload) < 126:
		frame = append(frame, 0x80|byte(len(payload)))
	case len(payload) <= 65535:
		frame = append(frame, 0x80|126, byte(len(payload)>>8), byte(len(payload)))
	default:
		return errString("payload too large for this test helper")
	}
	frame = append(frame, mask[:]...)
	frame = append(frame, masked...)
	_, err := conn.Write(frame)
	return err
}

// readWSFrame reads a single, unfragmented, unmasked (server->client) frame.
func readWSFrame(conn net.Conn) (opcode byte, payload []byte, err error) {
	head := make([]byte, 2)
	if _, err = io.ReadFull(conn, head); err != nil {
		return 0, nil, err
	}
	opcode = head[0] & 0x0f
	length := int(head[1] & 0x7f)
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err = io.ReadFull(conn, ext); err != nil {
			return 0, nil, err
		}
		length = int(ext[0])<<8 | int(ext[1])
	case 127:
		ext := make([]byte, 8)
		if _, err = io.ReadFull(conn, ext); err != nil {
			return 0, nil, err
		}
		length = int(ext[6])<<8 | int(ext[7])
	}
	payload = make([]byte, length)
	if length > 0 {
		if _, err = io.ReadFull(conn, payload); err != nil {
			return 0, nil, err
		}
	}
	return opcode, payload, nil
}
