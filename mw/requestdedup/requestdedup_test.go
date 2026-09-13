package requestdedup

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

func doRequest(t *testing.T, addr, method, path, body string) (statusCode int, respBody string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n", method, path)
	if body != "" {
		req += fmt.Sprintf("Content-Length: %d\r\n", len(body))
	}
	req += "\r\n" + body

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	fmt.Sscan(status, &statusCode)
	idx := strings.Index(string(resp), "\r\n\r\n")
	if idx >= 0 {
		respBody = string(resp)[idx+4:]
	}
	return
}

// TestDuplicateRequestWithinWindowIsRejected is the core behavioral contract:
// an identical request (same method+URL+body) sent again inside the window
// must not reach the handler a second time, and must get 409 Conflict.
func TestDuplicateRequestWithinWindowIsRejected(t *testing.T) {
	var calls atomic.Int64
	app := fh.New()
	dd := New(Config{Window: time.Minute})
	app.Post("/payments", dd.Handler(), func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("processed")
	})
	addr := testServer(t, app)

	status1, body1 := doRequest(t, addr, "POST", "/payments", `{"amount":10}`)
	if status1 != fh.StatusOK || body1 != "processed" {
		t.Fatalf("first request: expected 200/processed, got %d/%q", status1, body1)
	}

	status2, body2 := doRequest(t, addr, "POST", "/payments", `{"amount":10}`)
	if status2 != fh.StatusConflict {
		t.Fatalf("duplicate request: expected 409, got %d (body=%q)", status2, body2)
	}
	if !strings.Contains(body2, "duplicate_request") {
		t.Fatalf("expected duplicate_request error body, got %q", body2)
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler must run exactly once for the deduped request, ran %d times", got)
	}
}

// TestDistinctBodiesAreNotDeduplicated proves the default key includes the
// request body, so two requests to the same endpoint with different
// payloads are treated as distinct (a naive method+URL-only key would
// wrongly collapse them).
func TestDistinctBodiesAreNotDeduplicated(t *testing.T) {
	var calls atomic.Int64
	app := fh.New()
	dd := New(Config{Window: time.Minute})
	app.Post("/payments", dd.Handler(), func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("processed")
	})
	addr := testServer(t, app)

	if status, _ := doRequest(t, addr, "POST", "/payments", `{"amount":10}`); status != fh.StatusOK {
		t.Fatalf("expected first request to succeed, got %d", status)
	}
	if status, _ := doRequest(t, addr, "POST", "/payments", `{"amount":20}`); status != fh.StatusOK {
		t.Fatalf("expected differently-bodied request to succeed (not deduped), got %d", status)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected handler to run twice for distinct bodies, ran %d times", got)
	}
}

// TestDistinctMethodsAreNotDeduplicated proves the key includes the HTTP
// method, so GET and POST to the same URL are never conflated.
func TestDistinctMethodsAreNotDeduplicated(t *testing.T) {
	app := fh.New()
	dd := New(Config{Window: time.Minute})
	app.Get("/resource", dd.Handler(), func(c fh.Ctx) error { return c.SendString("get-ok") })
	app.Post("/resource", dd.Handler(), func(c fh.Ctx) error { return c.SendString("post-ok") })
	addr := testServer(t, app)

	if status, body := doRequest(t, addr, "GET", "/resource", ""); status != fh.StatusOK || body != "get-ok" {
		t.Fatalf("GET: expected 200/get-ok, got %d/%q", status, body)
	}
	if status, body := doRequest(t, addr, "POST", "/resource", ""); status != fh.StatusOK || body != "post-ok" {
		t.Fatalf("POST should not be treated as a duplicate of GET, got %d/%q", status, body)
	}
}

// TestRequestAfterWindowExpiryIsNotDuplicate proves the window is a sliding
// TTL, not a permanent block: once ExpiresAt has passed, an identical
// request must be processed again as new.
func TestRequestAfterWindowExpiryIsNotDuplicate(t *testing.T) {
	var calls atomic.Int64
	app := fh.New()
	dd := New(Config{Window: 30 * time.Millisecond})
	app.Post("/payments", dd.Handler(), func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("processed")
	})
	addr := testServer(t, app)

	if status, _ := doRequest(t, addr, "POST", "/payments", `{"amount":10}`); status != fh.StatusOK {
		t.Fatalf("expected first request to succeed, got %d", status)
	}
	if status, _ := doRequest(t, addr, "POST", "/payments", `{"amount":10}`); status != fh.StatusConflict {
		t.Fatalf("expected immediate repeat to be deduped, got %d", status)
	}

	time.Sleep(60 * time.Millisecond)

	if status, _ := doRequest(t, addr, "POST", "/payments", `{"amount":10}`); status != fh.StatusOK {
		t.Fatalf("expected request after window expiry to succeed, got %d", status)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("expected handler to run twice (before and after expiry), ran %d times", got)
	}
}

// TestEmptyKeyFuncBypassesDeduplication documents and locks in the
// intentional fail-open escape hatch: a KeyFunc that returns "" for a
// request disables deduplication for that request rather than colliding
// every empty-keyed request together.
func TestEmptyKeyFuncBypassesDeduplication(t *testing.T) {
	var calls atomic.Int64
	app := fh.New()
	dd := New(Config{Window: time.Minute, KeyFunc: func(c fh.Ctx) string { return "" }})
	app.Post("/payments", dd.Handler(), func(c fh.Ctx) error {
		calls.Add(1)
		return c.SendString("processed")
	})
	addr := testServer(t, app)

	for i := 0; i < 3; i++ {
		if status, _ := doRequest(t, addr, "POST", "/payments", `{"amount":10}`); status != fh.StatusOK {
			t.Fatalf("iteration %d: expected 200 (dedup bypassed via empty key), got %d", i, status)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected handler to run for every request when KeyFunc returns empty key, ran %d times", got)
	}
}

// TestCustomOnDuplicateIsInvoked verifies the configurable duplicate
// responder replaces the default 409 behavior end-to-end.
func TestCustomOnDuplicateIsInvoked(t *testing.T) {
	app := fh.New()
	dd := New(Config{
		Window: time.Minute,
		OnDuplicate: func(c fh.Ctx, e *Entry) error {
			return c.Status(fh.StatusTooManyRequests).SendString("custom-dup:" + e.Key)
		},
	})
	app.Post("/payments", dd.Handler(), func(c fh.Ctx) error { return c.SendString("processed") })
	addr := testServer(t, app)

	doRequest(t, addr, "POST", "/payments", `{"amount":10}`)
	status, body := doRequest(t, addr, "POST", "/payments", `{"amount":10}`)
	if status != fh.StatusTooManyRequests {
		t.Fatalf("expected custom OnDuplicate status 429, got %d", status)
	}
	if !strings.HasPrefix(body, "custom-dup:") {
		t.Fatalf("expected custom OnDuplicate body, got %q", body)
	}
}

// TestStatsReflectsActiveKeys exercises Stats() directly against the
// Deduplicator (no HTTP layer) to check accounting without relying on
// timing-sensitive HTTP round-trips.
func TestStatsReflectsActiveKeys(t *testing.T) {
	dd := New(Config{Window: time.Minute, MaxKeys: 2})
	app := fh.New()
	app.Post("/x", dd.Handler(), func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	doRequest(t, addr, "POST", "/x", "a")
	doRequest(t, addr, "POST", "/x", "b")
	if active, _ := dd.Stats(); active != 2 {
		t.Fatalf("expected 2 active keys after 2 distinct requests, got %d", active)
	}

	// A third distinct key must evict the oldest (MaxKeys=2), keeping the
	// tracked set bounded rather than growing without limit.
	doRequest(t, addr, "POST", "/x", "c")
	if active, _ := dd.Stats(); active > 2 {
		t.Fatalf("expected active keys to stay bounded at MaxKeys=2, got %d", active)
	}

	// The evicted key ("a") must now be treated as a brand-new request, not
	// a duplicate, proving eviction actually removes tracking state rather
	// than just failing to grow Stats().
	status, _ := doRequest(t, addr, "POST", "/x", "a")
	if status != fh.StatusOK {
		t.Fatalf("expected evicted key to be re-admitted as new, got status %d", status)
	}
}

// TestConcurrentIdenticalRequestsDeduplicateExactlyOnce is the
// concurrency-safety guard: firing simultaneous identical requests must
// never let more than one through concurrently, and must never race
// (run with -race). The first request is held open until a second,
// identical request has definitely reached the deduplicator, proving the
// entry is recorded before the handler runs (so a racing duplicate cannot
// slip through while the original is still in flight).
func TestConcurrentIdenticalRequestsDeduplicateExactlyOnce(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})

	app := fh.New()
	dd := New(Config{Window: time.Minute})
	app.Post("/payments", dd.Handler(), func(c fh.Ctx) error {
		calls.Add(1)
		close(entered)
		<-release
		return c.SendString("processed")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0], _ = doRequest(t, addr, "POST", "/payments", `{"amount":10}`)
	}()

	<-entered // first request is now inside the handler; entry is committed
	statuses[1], _ = doRequest(t, addr, "POST", "/payments", `{"amount":10}`)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusConflict {
		t.Fatalf("expected the racing duplicate to get 409 while the original was in flight, got %d", statuses[1])
	}
	if statuses[0] != fh.StatusOK {
		t.Fatalf("expected the original in-flight request to complete with 200, got %d", statuses[0])
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected the handler to run exactly once under concurrent duplicates, ran %d times", got)
	}
}

// TestConcurrentDistinctRequestsAreRaceFree hammers the deduplicator with
// many distinct concurrent requests (run with -race) to catch data races
// on the shared map/slice/mutex state.
func TestConcurrentDistinctRequestsAreRaceFree(t *testing.T) {
	app := fh.New()
	dd := New(Config{Window: time.Minute, MaxKeys: 10000})
	app.Post("/x", dd.Handler(), func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			doRequest(t, addr, "POST", "/x", fmt.Sprintf("body-%d", i))
		}(i)
	}
	wg.Wait()

	active, _ := dd.Stats()
	if active <= 0 {
		t.Fatalf("expected some active entries after concurrent distinct requests, got %d", active)
	}
}
