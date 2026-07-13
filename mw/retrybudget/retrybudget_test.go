package retrybudget

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

func doRequest(t *testing.T, addr, path string, headers map[string]string) int {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost\r\n", path)
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	req += "\r\n"
	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	var code int
	fmt.Sscan(status, &code)
	return code
}

// TestRetryBudgetRejectsAfterMaxRetries proves the budget still enforces its
// limit for a single repeatedly-retrying caller.
func TestRetryBudgetRejectsAfterMaxRetries(t *testing.T) {
	l := &limiter{buckets: map[string]bucket{}, cfg: Config{MaxRetries: 2, Window: time.Minute, MaxKeys: defaultMaxKeys, Header: "X-Retry-Attempt", Error: func(c fh.Ctx) error {
		return c.Status(fh.StatusTooManyRequests).SendString("rejected")
	}}}
	l.nextCleanup.Store(time.Now().Add(time.Minute).UnixNano())

	app := fh.New()
	app.Use(l.Handle)
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	headers := map[string]string{"X-Retry-Attempt": "1"}
	var lastCode int
	for i := 0; i < 4; i++ {
		lastCode = doRequest(t, addr, "/x", headers)
	}
	if lastCode != fh.StatusTooManyRequests {
		t.Fatalf("expected 429 after exceeding MaxRetries, got %d", lastCode)
	}
}

// TestRetryBudgetMapIsBoundedByMaxKeys proves that many distinct
// attacker-controlled keys (e.g. spoofed/rotating client identity sent via
// a custom Key func reading a client-supplied header) cannot grow the
// tracked-key map past MaxKeys, closing the unbounded-memory-growth vector
// where a remote client could otherwise allocate one permanent bucket per
// distinct key with no eviction.
func TestRetryBudgetMapIsBoundedByMaxKeys(t *testing.T) {
	const maxKeys = 50
	l := &limiter{buckets: map[string]bucket{}, cfg: Config{
		MaxRetries: 5,
		Window:     time.Hour,
		MaxKeys:    maxKeys,
		Header:     "X-Retry-Attempt",
		Key:        func(c fh.Ctx) string { return c.Get("X-Client-Key") },
	}}
	l.nextCleanup.Store(time.Now().Add(time.Hour).UnixNano())

	app := fh.New()
	app.Use(l.Handle)
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	for i := 0; i < 2000; i++ {
		doRequest(t, addr, "/x", map[string]string{
			"X-Retry-Attempt": "1",
			"X-Client-Key":    fmt.Sprintf("attacker-key-%d", i),
		})
	}

	l.mu.Lock()
	size := len(l.buckets)
	l.mu.Unlock()

	if size > maxKeys {
		t.Fatalf("expected bucket map bounded at %d entries after 2000 distinct keys, got %d", maxKeys, size)
	}
}
