package adaptiveconcurrency

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

// doRequest issues a raw HTTP/1.1 request and returns the status code, the
// response headers (lower-cased keys), and the body.
func doRequest(t *testing.T, addr, path string, headers map[string]string) (statusCode int, respHeaders map[string]string, body string) {
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
	req += "Connection: close\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	raw := string(resp)
	head, rest, found := strings.Cut(raw, "\r\n\r\n")
	if found {
		body = rest
	} else {
		head = raw
	}

	lines := strings.Split(head, "\r\n")
	if len(lines) > 0 {
		var proto, status string
		fmt.Sscan(lines[0], &proto, &status)
		fmt.Sscan(status, &statusCode)
	}

	respHeaders = make(map[string]string)
	for _, line := range lines[1:] {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		respHeaders[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return
}

// blockingApp builds an app whose "/work" handler blocks until release is
// closed, incrementing started as soon as it enters (so callers can wait
// for exactly N requests to be truly in-flight before probing the limit).
// A request carrying "X-No-Block: 1" returns immediately instead, so tests
// can probe whether a slot has freed up without registering new routes
// after the server has started.
func blockingApp(mw fh.HandlerFunc) (*fh.App, *atomic.Int32, chan struct{}) {
	app := fh.New()
	app.Use(mw)
	started := &atomic.Int32{}
	release := make(chan struct{})
	app.Get("/work", func(c fh.Ctx) error {
		if c.Get("X-No-Block") == "1" {
			return c.SendString("done")
		}
		started.Add(1)
		<-release
		return c.SendString("done")
	})
	return app, started, release
}

func waitForStarted(t *testing.T, started *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if started.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d in-flight requests, got %d", want, started.Load())
}

// TestConcurrencyLimitIsEnforcedUnderLoad is the core concurrency-limiting
// check: with InitialLimit==MaxLimit==3, exactly 3 concurrent long-running
// requests must be admitted and a 4th concurrent one must be rejected
// immediately (not queued, not eventually admitted) while the first 3 are
// still in flight.
func TestConcurrencyLimitIsEnforcedUnderLoad(t *testing.T) {
	app, started, release := blockingApp(New(Config{InitialLimit: 3, MaxLimit: 3, MinLimit: 1, Window: 1000}))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 3)
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer wg.Done()
			statuses[i], _, _ = doRequest(t, addr, "/work", nil)
		}(i)
	}
	waitForStarted(t, started, 3)

	// A 4th concurrent request must be rejected immediately, without
	// waiting for any of the in-flight 3 to finish.
	status, headers, body := doRequest(t, addr, "/work", nil)
	if status != fh.StatusServiceUnavailable {
		t.Fatalf("4th concurrent request status = %d, want 503", status)
	}
	if headers["retry-after"] == "" {
		t.Fatal("expected Retry-After header on rejection")
	}
	if !strings.Contains(body, "adaptive_concurrency_limited") {
		t.Fatalf("body = %q, want it to mention adaptive_concurrency_limited", body)
	}

	close(release)
	wg.Wait()
	for i, s := range statuses {
		if s != fh.StatusOK {
			t.Errorf("in-flight request %d status = %d, want 200", i, s)
		}
	}
}

// TestSlotFreesAfterRequestCompletes proves inFlight is correctly
// decremented once a request finishes, so capacity becomes available again
// rather than staying permanently consumed.
func TestSlotFreesAfterRequestCompletes(t *testing.T) {
	app, started, release := blockingApp(New(Config{InitialLimit: 1, MaxLimit: 1, MinLimit: 1, Window: 1000}))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	wg.Add(1)
	var firstStatus int
	go func() {
		defer wg.Done()
		firstStatus, _, _ = doRequest(t, addr, "/work", nil)
	}()
	waitForStarted(t, started, 1)

	if status, _, _ := doRequest(t, addr, "/work", nil); status != fh.StatusServiceUnavailable {
		t.Fatalf("second concurrent request status = %d, want 503 while the slot is occupied", status)
	}

	close(release)
	wg.Wait()
	if firstStatus != fh.StatusOK {
		t.Fatalf("first request status = %d, want 200", firstStatus)
	}

	// The slot must be free now that the first request has completed.
	if status, _, _ := doRequest(t, addr, "/work", map[string]string{"X-No-Block": "1"}); status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 (slot should have freed after the first request completed)", status)
	}
}

// TestConcurrentRequestsHaveNoDataRace stresses the limiter with many
// concurrent short-lived requests to catch any race on the shared inFlight/
// limit/samples/total counters. Run with -race.
func TestConcurrentRequestsHaveNoDataRace(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{InitialLimit: 20, MaxLimit: 20, MinLimit: 1, Window: 5}))
	app.Get("/", func(c fh.Ctx) error {
		time.Sleep(time.Millisecond)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			doRequest(t, addr, "/", nil)
		}()
	}
	wg.Wait()
}

// TestMaxLimitBelowInitialIsRaisedToInitialLimit proves the defaulting
// logic in New (MaxLimit raised to InitialLimit when configured lower)
// actually takes effect: a caller who misconfigures MaxLimit below
// InitialLimit must still get InitialLimit concurrent slots, not be
// clamped down to the too-low MaxLimit at startup.
func TestMaxLimitBelowInitialIsRaisedToInitialLimit(t *testing.T) {
	app, started, release := blockingApp(New(Config{InitialLimit: 3, MaxLimit: 1, MinLimit: 1, Window: 1000}))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 3)
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer wg.Done()
			statuses[i], _, _ = doRequest(t, addr, "/work", nil)
		}(i)
	}
	waitForStarted(t, started, 3)
	close(release)
	wg.Wait()

	for i, s := range statuses {
		if s != fh.StatusOK {
			t.Errorf("request %d status = %d, want 200 (effective limit should be InitialLimit=3, not MaxLimit=1)", i, s)
		}
	}
}

// TestMinLimitDefaultsToOneNotZero proves that when MinLimit is left at its
// zero value, the limiter floors at 1 (never at 0), which would otherwise
// permanently lock out all traffic once enough negative samples accumulate.
func TestMinLimitDefaultsToOneNotZero(t *testing.T) {
	app := fh.New()
	// Window=1 with a near-zero TargetLatency: essentially every request
	// looks "slow" and triggers a decrement attempt.
	app.Use(New(Config{InitialLimit: 2, MaxLimit: 2, Window: 1, TargetLatency: time.Nanosecond}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	for i := 0; i < 20; i++ {
		doRequest(t, addr, "/", nil)
	}

	// The limit should have floored at 1 (the documented default), so a
	// single request must still succeed -- the endpoint must not be fully
	// bricked by decrementing all the way to 0.
	status, _, _ := doRequest(t, addr, "/", nil)
	if status != fh.StatusOK {
		t.Fatalf("status = %d, want 200 (limit must floor at 1, not 0)", status)
	}
}

// TestAdaptiveLimitIncreasesAfterFastSuccessfulWindow proves the limit
// actually grows once a full window of fast, error-free samples has been
// observed.
func TestAdaptiveLimitIncreasesAfterFastSuccessfulWindow(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{InitialLimit: 2, MaxLimit: 5, MinLimit: 1, Window: 4, TargetLatency: time.Second}))
	started := &atomic.Int32{}
	release := make(chan struct{})
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Get("/work", func(c fh.Ctx) error {
		started.Add(1)
		<-release
		return c.SendString("done")
	})
	addr := testServer(t, app)

	// Warm up exactly Window fast, successful requests so observe() fires
	// once and raises the limit from 2 to 3.
	for i := 0; i < 4; i++ {
		if status, _, _ := doRequest(t, addr, "/", nil); status != fh.StatusOK {
			t.Fatalf("warmup request %d status = %d, want 200", i, status)
		}
	}

	// The limit should now be 3: 3 concurrent long requests must all be
	// admitted, and a 4th concurrent one must be rejected.
	var wg sync.WaitGroup
	statuses := make([]int, 3)
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer wg.Done()
			statuses[i], _, _ = doRequest(t, addr, "/work", nil)
		}(i)
	}
	waitForStarted(t, started, 3)

	status, _, _ := doRequest(t, addr, "/work", nil)
	if status != fh.StatusServiceUnavailable {
		t.Fatalf("4th concurrent request status = %d, want 503 (limit should have grown to exactly 3, not unlimited)", status)
	}

	close(release)
	wg.Wait()
	for i, s := range statuses {
		if s != fh.StatusOK {
			t.Errorf("warmed-up concurrent request %d status = %d, want 200", i, s)
		}
	}
}

// TestAdaptiveLimitDecreasesAfterErroringWindow proves that a window of
// erroring samples shrinks the limit, even when the errors are fast (i.e.
// the decrement is driven by err != nil, not only by latency).
func TestAdaptiveLimitDecreasesAfterErroringWindow(t *testing.T) {
	app := fh.New()
	var fail atomic.Bool
	fail.Store(true)
	app.Use(New(Config{InitialLimit: 3, MaxLimit: 5, MinLimit: 1, Window: 4, TargetLatency: time.Second}))
	started := &atomic.Int32{}
	release := make(chan struct{})
	app.Get("/", func(c fh.Ctx) error {
		if fail.Load() {
			return fmt.Errorf("forced failure")
		}
		return c.SendString("ok")
	})
	app.Get("/work", func(c fh.Ctx) error {
		started.Add(1)
		<-release
		return c.SendString("done")
	})
	addr := testServer(t, app)

	// Drive exactly Window erroring requests so observe() fires once and
	// lowers the limit from 3 to 2.
	for i := 0; i < 4; i++ {
		doRequest(t, addr, "/", nil)
	}
	fail.Store(false)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			statuses[i], _, _ = doRequest(t, addr, "/work", nil)
		}(i)
	}
	waitForStarted(t, started, 2)

	status, _, _ := doRequest(t, addr, "/work", nil)
	if status != fh.StatusServiceUnavailable {
		t.Fatalf("3rd concurrent request status = %d, want 503 (limit should have shrunk to 2)", status)
	}

	close(release)
	wg.Wait()
	for i, s := range statuses {
		if s != fh.StatusOK {
			t.Errorf("concurrent request %d status = %d, want 200", i, s)
		}
	}
}

// TestMaxLimitCeilingIsNotExceeded proves the limit stops growing once it
// reaches MaxLimit, even after many more windows of fast successful
// samples.
func TestMaxLimitCeilingIsNotExceeded(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{InitialLimit: 2, MaxLimit: 3, MinLimit: 1, Window: 1, TargetLatency: time.Second}))
	started := &atomic.Int32{}
	release := make(chan struct{})
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Get("/work", func(c fh.Ctx) error {
		started.Add(1)
		<-release
		return c.SendString("done")
	})
	addr := testServer(t, app)

	// Window=1: every fast successful request is its own window, so the
	// limit should climb 2 -> 3 and then stop at MaxLimit=3 despite many
	// more successful windows.
	for i := 0; i < 10; i++ {
		doRequest(t, addr, "/", nil)
	}

	var wg sync.WaitGroup
	statuses := make([]int, 3)
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer wg.Done()
			statuses[i], _, _ = doRequest(t, addr, "/work", nil)
		}(i)
	}
	waitForStarted(t, started, 3)

	status, _, _ := doRequest(t, addr, "/work", nil)
	if status != fh.StatusServiceUnavailable {
		t.Fatalf("4th concurrent request status = %d, want 503 (limit must cap at MaxLimit=3)", status)
	}

	close(release)
	wg.Wait()
	for i, s := range statuses {
		if s != fh.StatusOK {
			t.Errorf("concurrent request %d status = %d, want 200", i, s)
		}
	}
}

// TestCustomErrorHandlerInvokedOnRejection proves a configured Error
// callback is used instead of the default JSON rejection response.
func TestCustomErrorHandlerInvokedOnRejection(t *testing.T) {
	var calls atomic.Int32
	var gotLimit int
	app, started, release := blockingApp(New(Config{
		InitialLimit: 1, MaxLimit: 1, MinLimit: 1, Window: 1000,
		Error: func(c fh.Ctx, limit int) error {
			calls.Add(1)
			gotLimit = limit
			return c.Status(fh.StatusTeapot).SendString("custom-reject")
		},
	}))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		doRequest(t, addr, "/work", nil)
	}()
	waitForStarted(t, started, 1)

	status, _, body := doRequest(t, addr, "/work", nil)
	if status != fh.StatusTeapot {
		t.Fatalf("status = %d, want %d", status, fh.StatusTeapot)
	}
	if body != "custom-reject" {
		t.Fatalf("body = %q, want %q", body, "custom-reject")
	}
	if calls.Load() != 1 {
		t.Fatalf("custom error handler invoked %d times, want 1", calls.Load())
	}
	if gotLimit != 1 {
		t.Fatalf("error handler received limit=%d, want 1", gotLimit)
	}

	close(release)
	wg.Wait()
}
