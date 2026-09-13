package loadtest

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// TestTLSHandshakeTimeoutEnforcedUnderFlood serves a real TLS listener (a
// freshly generated self-signed certificate, not a checked-in file) and
// floods it with two kinds of concurrent connections: many that open the TCP
// socket and never send a single TLS byte, and several that perform a real,
// legitimate handshake and HTTP request. It asserts fh's configured
// TLSHandshakeTimeout drops the stalled connections instead of holding them
// (and their goroutines) forever, and that legitimate concurrent handshakes
// still succeed while the flood is in progress (concern #5).
func TestTLSHandshakeTimeoutEnforcedUnderFlood(t *testing.T) {
	const handshakeTimeout = 300 * time.Millisecond
	app := fh.New(fh.WithTLSHandshakeTimeout(handshakeTimeout))
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr, clientTLSConfig := startTLSApp(t, app)

	const stalledCount = 25
	const legitCount = 10

	var wg sync.WaitGroup
	stalledClosedWithin := make([]bool, stalledCount)
	stalledConns := make([]net.Conn, stalledCount)

	// Open the stalled connections up front (raw TCP, no TLS bytes at all).
	for i := 0; i < stalledCount; i++ {
		conn, err := dialRetry(addr, 2*time.Second)
		if err != nil {
			t.Fatalf("dial stalled connection %d: %v", i, err)
		}
		stalledConns[i] = conn
	}
	defer func() {
		for _, c := range stalledConns {
			if c != nil {
				closeFast(c)
			}
		}
	}()

	// While those sit stalled, drive real, legitimate concurrent TLS
	// handshakes + HTTP requests against the same server.
	var legitOK atomic.Int64
	client := httpClientFor(clientTLSConfig, false)
	for i := 0; i < legitCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get("https://" + addr + "/ping")
			if err != nil {
				t.Logf("legitimate handshake/request failed: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				legitOK.Add(1)
			}
		}()
	}
	wg.Wait()

	if int(legitOK.Load()) < legitCount {
		t.Errorf("legitimate concurrent handshakes succeeded %d/%d while stalled handshakes were flooding the listener", legitOK.Load(), legitCount)
	}

	// Now confirm every stalled connection gets dropped within a bounded
	// window around the configured handshake timeout — not held forever.
	deadline := 3 * time.Second
	var wg2 sync.WaitGroup
	for i, conn := range stalledConns {
		wg2.Add(1)
		go func(i int, conn net.Conn) {
			defer wg2.Done()
			_ = conn.SetReadDeadline(time.Now().Add(deadline))
			buf := make([]byte, 16)
			_, err := conn.Read(buf)
			// Any error (EOF, reset) means the server closed it. A timeout
			// error means it is still open — that's the failure case.
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				stalledClosedWithin[i] = false
				return
			}
			stalledClosedWithin[i] = err != nil
		}(i, conn)
	}
	wg2.Wait()

	notClosed := 0
	for _, closed := range stalledClosedWithin {
		if !closed {
			notClosed++
		}
	}
	if notClosed > 0 {
		t.Errorf("%d/%d stalled (handshake-never-started) connections were still open after %s (TLSHandshakeTimeout=%s not enforced)", notClosed, stalledCount, deadline, handshakeTimeout)
	}
}

// TestTLSManyConcurrentLegitimateHandshakesSucceed is a simpler companion
// check with no stalled traffic: a burst of concurrent, well-behaved TLS
// clients against the same self-signed server should all complete their
// handshake and request successfully (concern #5, the non-adversarial half).
func TestTLSManyConcurrentLegitimateHandshakesSucceed(t *testing.T) {
	app := fh.New(fh.WithTLSHandshakeTimeout(2 * time.Second))
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr, clientTLSConfig := startTLSApp(t, app)

	const n = 30
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := httpClientFor(clientTLSConfig, false)
			resp, err := client.Get("https://" + addr + "/ping")
			if err != nil {
				t.Logf("handshake/request failed: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if int(ok.Load()) != n {
		t.Errorf("concurrent legitimate handshakes: %d/%d succeeded", ok.Load(), n)
	}
}
