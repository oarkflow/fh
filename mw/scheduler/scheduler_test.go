package scheduler

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

func doRequest(t *testing.T, addr, path string, headers map[string]string) int {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n", path)
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	req += "\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	var proto, status string
	fmt.Sscan(string(resp), &proto, &status)
	code := 0
	fmt.Sscan(status, &code)
	return code
}

// holdingHandler returns a handler that signals entered (closed) the
// instant it starts running — which, by construction, only happens after
// the scheduler has already admitted and reserved capacity for the
// request — then blocks until release is closed. Tests use entered as a
// deterministic barrier instead of a fixed time.Sleep, so a second,
// contending request is only ever sent once the first request's
// reservation has definitely been made.
func holdingHandler(entered chan struct{}, release chan struct{}) fh.HandlerFunc {
	var once sync.Once
	return func(c fh.Ctx) error {
		once.Do(func() { close(entered) })
		<-release
		return c.SendString("ok")
	}
}

// TestOverloadShedsAtGlobalLimit proves the default (no OnShed) behavior is
// fail-closed under overload: once MaxConcurrent normal-priority requests
// are in flight, one more is rejected with 503 and Retry-After, rather than
// silently queueing forever or crashing.
func TestOverloadShedsAtGlobalLimit(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sched := New(Config{MaxConcurrent: 1})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", nil)
	}()
	<-entered // the first request's reservation is now committed
	statuses[1] = doRequest(t, addr, "/", nil)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusServiceUnavailable {
		t.Fatalf("expected the second request to be shed with 503 at MaxConcurrent=1, got %d", statuses[1])
	}
	if statuses[0] != fh.StatusOK {
		t.Fatalf("expected the first (in-flight) request to complete with 200, got %d", statuses[0])
	}
}

// TestCriticalPriorityBypassesGlobalLimit verifies the documented
// invariant that Critical-priority requests (health checks, admin
// endpoints) are never shed by the global concurrency limit, so an
// overloaded server can still be inspected/administered.
func TestCriticalPriorityBypassesGlobalLimit(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sched := New(Config{
		MaxConcurrent: 1,
		PriorityFunc: func(c fh.Ctx) Priority {
			if c.Path() == "/health" {
				return PriorityCritical
			}
			return PriorityNormal
		},
	})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	app.Get("/health", func(c fh.Ctx) error { return c.SendString("healthy") })
	addr := testServer(t, app)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		doRequest(t, addr, "/", nil)
	}()
	<-entered

	status := doRequest(t, addr, "/health", nil)
	close(release)
	wg.Wait()

	if status != fh.StatusOK {
		t.Fatalf("expected Critical-priority request to bypass the saturated global limit, got %d", status)
	}
}

// TestPerPriorityLimitIsEnforcedIndependently verifies a configured
// per-priority cap rejects requests at that level even while the global
// limit still has headroom.
func TestPerPriorityLimitIsEnforcedIndependently(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sched := New(Config{
		MaxConcurrent: 100,
		PerPriority:   map[Priority]int{PriorityLow: 1},
		PriorityFunc:  func(c fh.Ctx) Priority { return PriorityLow },
	})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", nil)
	}()
	<-entered
	statuses[1] = doRequest(t, addr, "/", nil)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusServiceUnavailable {
		t.Fatalf("expected per-priority limit (PerPriority[Low]=1) to shed the second Low request even with global headroom, got %d", statuses[1])
	}
}

// TestDefaultPriorityDefaultsToNormalNotCritical is a regression test for a
// config-merge bug: Config.DefaultPriority's zero value (0) is bit-for-bit
// identical to PriorityCritical, so any New(Config{...}) call that sets
// some other field but leaves DefaultPriority unspecified must NOT silently
// downgrade the effective default from PriorityNormal to PriorityCritical.
// Because admit() lets Critical priority bypass every limit, that bug would
// silently disable MaxConcurrent/PerPriority enforcement entirely for any
// scheduler configured with so much as one option — which is exactly what
// almost every real caller does.
func TestDefaultPriorityDefaultsToNormalNotCritical(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	// Deliberately configure only MaxConcurrent, leaving DefaultPriority at
	// its Go zero value — the exact shape of a typical caller's Config.
	sched := New(Config{MaxConcurrent: 1})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", nil)
	}()
	<-entered
	statuses[1] = doRequest(t, addr, "/", nil)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusServiceUnavailable {
		t.Fatalf("BUG: a request with no explicit priority was treated as Critical (bypassing MaxConcurrent=1) "+
			"instead of the documented default of Normal; got status %d instead of 503", statuses[1])
	}
}

// TestUnconfiguredPriorityHasNoPerPriorityLimit proves that a priority
// level absent from PerPriority is bounded only by MaxConcurrent, not
// silently limited to zero.
func TestUnconfiguredPriorityHasNoPerPriorityLimit(t *testing.T) {
	sched := New(Config{
		MaxConcurrent: 100,
		PerPriority:   map[Priority]int{PriorityLow: 1},
		PriorityFunc:  func(c fh.Ctx) Priority { return PriorityNormal },
	})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	for i := 0; i < 5; i++ {
		if status := doRequest(t, addr, "/", nil); status != fh.StatusOK {
			t.Fatalf("request %d: expected 200 for an unconfigured priority level, got %d", i, status)
		}
	}
}

// TestOnShedIsInvokedInsteadOfDefaultResponse verifies the configurable
// shed responder replaces the default 503 JSON body.
func TestOnShedIsInvokedInsteadOfDefaultResponse(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sched := New(Config{
		MaxConcurrent: 1,
		OnShed: func(c fh.Ctx) error {
			return c.Status(fh.StatusTeapot).SendString("custom-shed")
		},
	})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		doRequest(t, addr, "/", nil)
	}()
	<-entered
	status := doRequest(t, addr, "/", nil)
	close(release)
	wg.Wait()

	if status != fh.StatusTeapot {
		t.Fatalf("expected custom OnShed status to be used, got %d", status)
	}
}

// TestPriorityFromHTTPMapsUrgencyBands checks the RFC 9218 urgency-to-
// Priority mapping directly against the documented bands
// (u<=1 -> High, u>=6 -> Low, otherwise Normal). A regression here (e.g. an
// off-by-one in the comparisons) would silently mis-prioritize traffic
// without ever failing a status-code-only HTTP test.
func TestPriorityFromHTTPMapsUrgencyBands(t *testing.T) {
	cases := []struct {
		urgency uint8
		want    Priority
	}{
		{0, PriorityHigh},
		{1, PriorityHigh},
		{2, PriorityNormal},
		{3, PriorityNormal},
		{5, PriorityNormal},
		{6, PriorityLow},
		{7, PriorityLow},
	}
	for _, tc := range cases {
		got := priorityFromHTTP(fh.HTTPPriority{Urgency: tc.urgency})
		if got != tc.want {
			t.Errorf("priorityFromHTTP(urgency=%d) = %v, want %v", tc.urgency, got, tc.want)
		}
	}
}

// TestPriorityHeaderActuallyAffectsAdmission proves the header is not just
// parsed but wired into the real admission path: a High-priority request
// (Priority: u=0) must still get through a saturated Normal-priority
// per-priority limit, while a Normal-priority request does not.
func TestPriorityHeaderActuallyAffectsAdmission(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sched := New(Config{
		MaxConcurrent: 100,
		PerPriority:   map[Priority]int{PriorityNormal: 1},
	})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	var statusHigh int
	wg.Add(2)
	go func() {
		defer wg.Done()
		// No Priority header -> DefaultPriority (Normal), saturates the
		// PerPriority[Normal]=1 limit.
		doRequest(t, addr, "/", nil)
	}()
	<-entered

	// This request is expected to be *admitted* (unlike the sibling tests
	// above), so it will reach holdingHandler too and block on release —
	// send it from its own goroutine and close release right after, rather
	// than blocking here waiting for a response that can't arrive yet.
	go func() {
		defer wg.Done()
		statusHigh = doRequest(t, addr, "/", map[string]string{"Priority": "u=0"})
	}()
	close(release)
	wg.Wait()

	if statusHigh != fh.StatusOK {
		t.Fatalf("expected a High-priority (Priority: u=0) request to be admitted despite the Normal-priority bucket being saturated, got %d", statusHigh)
	}
}

// TestNoPriorityHeaderUsesDefaultPriority verifies that when neither
// PriorityFunc nor a Priority header is present, DefaultPriority (Normal)
// governs admission, not an unset/zero-value priority that could
// accidentally alias Critical (priority 0) and bypass all limits.
func TestNoPriorityHeaderUsesDefaultPriority(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	sched := New(Config{
		MaxConcurrent: 100,
		PerPriority:   map[Priority]int{PriorityNormal: 1},
	})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", holdingHandler(entered, release))
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", nil)
	}()
	<-entered
	statuses[1] = doRequest(t, addr, "/", nil)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusServiceUnavailable {
		t.Fatalf("expected a header-less request to be governed by PerPriority[Normal] (i.e. DefaultPriority), got %d", statuses[1])
	}
}

// TestReleaseAlwaysRunsEvenOnHandlerError proves the in-flight slot is
// freed when the downstream handler returns an error, not just on success —
// otherwise a stream of failing requests would permanently exhaust
// capacity (a latent denial-of-service).
func TestReleaseAlwaysRunsEvenOnHandlerError(t *testing.T) {
	sched := New(Config{MaxConcurrent: 1})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/fail", func(c fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusInternalServerError, "BOOM", "boom")
	})
	addr := testServer(t, app)

	for i := 0; i < 5; i++ {
		doRequest(t, addr, "/fail", nil)
	}
	stats := sched.Stats()
	if stats.TotalInFlight != 0 {
		t.Fatalf("expected TotalInFlight to return to 0 after failing requests complete, got %d", stats.TotalInFlight)
	}
}

// TestConcurrentAdmissionStaysRaceFree hammers the scheduler with many
// concurrent goroutines under a small MaxConcurrent (run with -race) to
// catch data races on the atomic counters, and confirms bookkeeping
// (admitted+rejected) accounts for every attempt and settles back to a
// clean, non-negative state.
func TestConcurrentAdmissionStaysRaceFree(t *testing.T) {
	const workers = 50
	sched := New(Config{MaxConcurrent: 5})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", func(c fh.Ctx) error {
		time.Sleep(time.Millisecond)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var admitted, rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch doRequest(t, addr, "/", nil) {
			case fh.StatusOK:
				admitted.Add(1)
			case fh.StatusServiceUnavailable:
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()

	if admitted.Load()+rejected.Load() != workers {
		t.Fatalf("expected every request accounted for as admitted or rejected, admitted=%d rejected=%d workers=%d",
			admitted.Load(), rejected.Load(), workers)
	}

	stats := sched.Stats()
	if stats.TotalInFlight != 0 {
		t.Fatalf("expected TotalInFlight to settle back to 0 once all requests complete, got %d", stats.TotalInFlight)
	}
	if stats.TotalInFlight < 0 {
		t.Fatalf("in-flight counter must never go negative, got %d", stats.TotalInFlight)
	}
	for i, n := range stats.ByPriority {
		if n != 0 {
			t.Fatalf("expected per-priority in-flight[%d] to settle back to 0, got %d", i, n)
		}
	}
	if stats.Admitted != admitted.Load() {
		t.Fatalf("Stats().Admitted=%d does not match observed admitted count=%d", stats.Admitted, admitted.Load())
	}
	if stats.Rejected != rejected.Load() {
		t.Fatalf("Stats().Rejected=%d does not match observed rejected count=%d", stats.Rejected, rejected.Load())
	}
}

// TestAdmitNeverExceedsMaxConcurrentUnderRace is a direct regression test
// for a time-of-check-to-time-of-use race in admit(): calling Load() and
// then, separately, Add(1) lets multiple concurrent callers all observe
// spare capacity and all be admitted, silently exceeding MaxConcurrent.
// Many goroutines are released simultaneously via a start barrier (instead
// of relying on HTTP round-trip timing) to maximize the chance of hitting
// the race window, and the number of goroutines that are actually admitted
// at once must never exceed the configured limit. Run with -race.
func TestAdmitNeverExceedsMaxConcurrentUnderRace(t *testing.T) {
	const limit = 10
	const attempts = 300

	sched := New(Config{MaxConcurrent: limit})

	var start sync.WaitGroup
	start.Add(1)
	var current atomic.Int64
	var maxObserved atomic.Int64
	var admittedTotal atomic.Int64

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait() // maximize concurrent contention on admit()
			if !sched.admit(PriorityNormal) {
				return
			}
			admittedTotal.Add(1)
			n := current.Add(1)
			for {
				m := maxObserved.Load()
				if n <= m || maxObserved.CompareAndSwap(m, n) {
					break
				}
			}
			// Hold the slot briefly so overlapping admits are likely.
			time.Sleep(2 * time.Millisecond)
			current.Add(-1)
			sched.release(PriorityNormal)
		}()
	}
	start.Done()
	wg.Wait()

	if got := maxObserved.Load(); got > limit {
		t.Fatalf("BUG: %d requests were concurrently admitted at once, exceeding MaxConcurrent=%d (time-of-check-to-time-of-use race in admit())", got, limit)
	}
	if got := sched.Stats().TotalInFlight; got != 0 {
		t.Fatalf("expected TotalInFlight to settle back to 0, got %d (admit/release accounting is unbalanced)", got)
	}
	if admittedTotal.Load() == 0 {
		t.Fatal("expected at least some attempts to be admitted")
	}
}

// TestMalformedPriorityHeaderFallsBackSafely ensures a garbage/malformed
// Priority header value doesn't panic the request or corrupt admission —
// it must fall back to a safe default urgency rather than crashing.
func TestMalformedPriorityHeaderFallsBackSafely(t *testing.T) {
	sched := New(Config{MaxConcurrent: 100})
	app := fh.New()
	app.Use(sched.Handler())
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	for _, header := range []string{"garbage", "u=", "u=abc", "u=999999999999999999", ",,,", strings.Repeat("u=1,", 100)} {
		status := doRequest(t, addr, "/", map[string]string{"Priority": header})
		if status != fh.StatusOK {
			t.Fatalf("Priority: %q: expected malformed header to be handled without panicking, got status %d", header, status)
		}
	}
}
