package loadtest

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

// safeConnectionCount picks a connection count for descriptor-pressure tests
// that stays well below the process's real RLIMIT_NOFILE ceiling, so this
// test never risks exhausting actual OS descriptors in CI — it only exercises
// fh's own configured MaxConnections/MaxConnectionsPerIP ceilings, which are
// set far below that.
func safeConnectionCount(t *testing.T) int {
	t.Helper()
	limit, err := currentNoFileLimit()
	if err != nil || limit == 0 {
		t.Logf("could not read RLIMIT_NOFILE (%v); assuming a conservative default", err)
		limit = 1024
	}
	n := int(limit / 8)
	if n > 40 {
		n = 40
	}
	if n < 8 {
		n = 8
	}
	t.Logf("RLIMIT_NOFILE=%d, using %d connections for the configured ceiling", limit, n)
	return n
}

// isAccepted opens a bounded read on conn to tell whether the server kept the
// connection open (a blocking read that times out — the server is waiting
// for a request we never sent) or rejected/dropped it (the read observes
// EOF/reset almost immediately, well before the deadline).
func isAccepted(conn net.Conn, wait time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		return true
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

// dialAllConcurrently opens n connections to addr at (as close to) the same
// time and returns them all, or fails the test. Dialing concurrently instead
// of one-at-a-time matters here: this test needs every connection to still
// be within the server's read-header grace period by the time it checks the
// (n+1)th, and a sequential dial-then-block-on-read loop over dozens of
// connections can itself burn seconds, letting the earliest ones legitimately
// time out before the test gets around to the interesting assertion.
func dialAllConcurrently(t *testing.T, addr string, n int) []net.Conn {
	t.Helper()
	conns := make([]net.Conn, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conns[i], errs[i] = dialRetry(addr, 2*time.Second)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("dial %d/%d: %v", i+1, n, err)
		}
	}
	return conns
}

// checkAllAccepted checks isAccepted on every conn concurrently (so the
// check itself takes ~wait, not len(conns)*wait) and returns how many were
// accepted.
func checkAllAccepted(conns []net.Conn, wait time.Duration) (acceptedCount int) {
	results := make([]bool, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			results[i] = isAccepted(c, wait)
		}(i, c)
	}
	wg.Wait()
	for _, ok := range results {
		if ok {
			acceptedCount++
		}
	}
	return acceptedCount
}

// TestMaxConnectionsRejectsBeyondCeilingAndRecovers drives real concurrent
// connections up to a configured MaxConnections ceiling (safely below the
// process's actual descriptor limit — see safeConnectionCount) and asserts
// connections beyond it are rejected cleanly rather than hung, panicking, or
// silently admitted past the bound. It also asserts the server accepts new
// connections again once load drops (concern #3).
func TestMaxConnectionsRejectsBeyondCeilingAndRecovers(t *testing.T) {
	n := safeConnectionCount(t)
	app := fh.New(
		fh.WithMaxConnections(n),
		// Generous relative to how long this whole test takes to run (well
		// under a second even with dozens of connections, since dialing and
		// checking happen concurrently) — this timeout exists so the
		// held-open connections don't get reclaimed by an unrelated timeout
		// while this test is busy asserting on the ceiling itself.
		fh.WithReadHeaderTimeout(30*time.Second),
		fh.WithIdleTimeout(30*time.Second),
	)
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr := startApp(t, app)

	conns := dialAllConcurrently(t, addr, n)
	defer func() {
		for _, c := range conns {
			closeFast(c)
		}
	}()

	if accepted := checkAllAccepted(conns, 300*time.Millisecond); accepted != n {
		t.Fatalf("only %d/%d connections within MaxConnections=%d were accepted", accepted, n, n)
	}

	// One more, beyond the ceiling: must be rejected cleanly, not hang.
	extra, err := dialRetry(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial beyond ceiling: %v", err)
	}
	defer closeFast(extra)
	if isAccepted(extra, 500*time.Millisecond) {
		t.Fatalf("connection beyond MaxConnections=%d was accepted, want clean rejection", n)
	}

	// Drop half the held connections and confirm the server recovers — it
	// accepts fresh connections again once load falls back under the
	// ceiling, rather than staying wedged at capacity forever.
	freed := n / 2
	for i := 0; i < freed; i++ {
		closeFast(conns[i])
	}
	conns = conns[freed:]

	if !waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		c, err := dialRetry(addr, time.Second)
		if err != nil {
			return false
		}
		ok := isAccepted(c, 200*time.Millisecond)
		closeFast(c)
		return ok
	}) {
		t.Fatalf("server did not accept new connections again after load dropped below MaxConnections=%d", n)
	}
}

// TestMaxConnectionsPerIPRejectsBeyondCeilingAndRecovers is the per-peer
// counterpart: every dial in this process comes from the same loopback
// address, so a small MaxConnectionsPerIP ceiling (with a much larger global
// MaxConnections) isolates that specific gate (concern #3).
func TestMaxConnectionsPerIPRejectsBeyondCeilingAndRecovers(t *testing.T) {
	const perIP = 6
	app := fh.New(
		fh.WithMaxConnections(1000),
		fh.WithMaxConnectionsPerIP(perIP),
		fh.WithReadHeaderTimeout(30*time.Second),
		fh.WithIdleTimeout(30*time.Second),
	)
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr := startApp(t, app)

	conns := dialAllConcurrently(t, addr, perIP)
	defer func() {
		for _, c := range conns {
			closeFast(c)
		}
	}()

	if accepted := checkAllAccepted(conns, 300*time.Millisecond); accepted != perIP {
		t.Fatalf("only %d/%d connections within MaxConnectionsPerIP=%d were accepted", accepted, perIP, perIP)
	}

	extra, err := dialRetry(addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial beyond per-IP ceiling: %v", err)
	}
	defer closeFast(extra)
	if isAccepted(extra, 500*time.Millisecond) {
		t.Fatalf("connection beyond MaxConnectionsPerIP=%d was accepted, want clean rejection", perIP)
	}

	closeFast(conns[0])
	conns = conns[1:]

	if !waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		c, err := dialRetry(addr, time.Second)
		if err != nil {
			return false
		}
		ok := isAccepted(c, 200*time.Millisecond)
		closeFast(c)
		return ok
	}) {
		t.Fatalf("server did not accept new connections again from the same IP after load dropped below MaxConnectionsPerIP=%d", perIP)
	}
}
