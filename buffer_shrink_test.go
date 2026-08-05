package fh

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
	"time"
)

// TestKeepAliveWriteBufferShrinksAfterLargeResponse proves that a keep-alive
// HTTP/1 connection's response buffer (connState.writeBuf) is not pinned at
// its largest-ever size for the rest of the connection's life. That buffer
// is intentionally connection-owned (not returned to the shared bytesPool)
// so ordinary requests avoid a sync.Pool round trip, but without a size
// ceiling one big response would permanently inflate memory retained by an
// otherwise-idle keep-alive connection — the same "slice retains huge
// backing array" pattern putBytes already guards against for the shared
// pool. This proves the connection-owned buffer gets the same treatment.
func TestKeepAliveWriteBufferShrinksAfterLargeResponse(t *testing.T) {
	app := New()
	bigBody := bytes.Repeat([]byte("x"), maxPooledBytesCap+4096)
	app.Get("/big", func(c Ctx) error {
		return c.SendBytes(bigBody)
	})
	app.Get("/small", func(c Ctx) error {
		return c.SendString("ok")
	})

	connCh := make(chan net.Conn, 1)
	app.OnConnect(func(c net.Conn) { connCh <- c })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var serverConn net.Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the connection")
	}

	reader := bufio.NewReader(conn)

	if _, err := conn.Write([]byte("GET /big HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	readFullResponse(t, reader, len(bigBody))

	// Give serveConn a moment to finish releasing the request and loop back
	// to waiting for the next one — the shrink happens right after release,
	// before the connection blocks on the next read.
	deadline := time.Now().Add(2 * time.Second)
	var shrunk bool
	for time.Now().Before(deadline) {
		app.connMu.Lock()
		state := app.conns[serverConn]
		var cap0 int
		if state != nil {
			cap0 = cap(state.writeBuf)
		}
		app.connMu.Unlock()
		if state != nil && cap0 <= maxPooledBytesCap {
			shrunk = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !shrunk {
		t.Fatal("connection-owned write buffer was not shrunk back after a large response")
	}

	if _, err := conn.Write([]byte("GET /small HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	readFullResponse(t, reader, len("ok"))
}

// readFullResponse reads one HTTP response (status line, headers, and a
// body of the given length) off r, failing the test on error or a non-200
// status.
func readFullResponse(t *testing.T, r *bufio.Reader, bodyLen int) {
	t.Helper()
	status, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("reading status line: %v", err)
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200") {
		t.Fatalf("unexpected status line: %q", status)
	}
	contentLength := -1
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(trimmed), "content-length:") {
			parts := strings.SplitN(trimmed, ":", 2)
			if len(parts) == 2 {
				var n int
				_, _ = sscanTrim(strings.TrimSpace(parts[1]), &n)
				contentLength = n
			}
		}
	}
	if contentLength >= 0 && contentLength != bodyLen {
		t.Fatalf("expected content-length %d, got %d", bodyLen, contentLength)
	}
	buf := make([]byte, bodyLen)
	if _, err := readFullN(r, buf); err != nil {
		t.Fatalf("reading body: %v", err)
	}
}

func sscanTrim(s string, n *int) (int, error) {
	v := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		v = v*10 + int(s[i]-'0')
	}
	*n = v
	return 1, nil
}

func readFullN(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
