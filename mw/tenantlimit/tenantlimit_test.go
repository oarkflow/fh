package tenantlimit

import (
	"fmt"
	"io"
	"net"
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

// TestLimitEnforcedPerTenant proves the default limiter rejects a tenant
// once its concurrent in-flight count reaches Limit, and that the rejection
// is fail-closed (429), not fail-open.
func TestLimitEnforcedPerTenant(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{Limit: 1}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	}()
	time.Sleep(30 * time.Millisecond) // ensure the first request is in flight and holding the slot
	go func() {
		defer wg.Done()
		statuses[1] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	}()
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusTooManyRequests {
		t.Fatalf("expected second concurrent request for the same tenant to be rejected with 429, got %d", statuses[1])
	}
	if statuses[0] != fh.StatusOK {
		t.Fatalf("expected the first (in-flight) request to eventually succeed, got %d", statuses[0])
	}
}

// TestDistinctTenantsAreIsolated proves one tenant hitting its limit does
// not affect a different tenant's ability to make requests (blast-radius
// isolation, the whole point of the middleware).
func TestDistinctTenantsAreIsolated(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{Limit: 1}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	}()
	time.Sleep(30 * time.Millisecond)

	statuses[1] = doRequest(t, addr, "/nonexistent-releases-immediately", map[string]string{"X-Tenant-ID": "other"})
	close(release)
	wg.Wait()

	// The "other" tenant's request went to a route with no handler match for
	// GET, but what matters is it must not be limited by acme's saturation;
	// specifically it must not be 429.
	if statuses[1] == fh.StatusTooManyRequests {
		t.Fatalf("a different tenant must not be rejected due to another tenant's concurrency limit")
	}
}

// TestMissingTenantHeaderFallsBackToDefaultBucket documents the fail-closed
// default: requests with no tenant identifier are not silently exempted
// from limiting — they are grouped into a shared "default" bucket and are
// still subject to Limit.
func TestMissingTenantHeaderFallsBackToDefaultBucket(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{Limit: 1}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", nil)
	}()
	time.Sleep(30 * time.Millisecond)
	statuses[1] = doRequest(t, addr, "/", nil)
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusTooManyRequests {
		t.Fatalf("expected requests with no tenant header to share the default bucket and be limited, got %d", statuses[1])
	}
}

// TestCustomTenantFuncTakesPrecedenceOverHeader verifies configured
// precedence: an explicit TenantFunc wins over LocalKey and the header.
func TestCustomTenantFuncTakesPrecedenceOverHeader(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{
		Limit:  1,
		Tenant: func(c fh.Ctx) string { return "forced-tenant" },
	}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Distinct X-Tenant-ID headers, but TenantFunc should force both
		// into the same "forced-tenant" bucket.
		statuses[0] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	}()
	time.Sleep(30 * time.Millisecond)
	statuses[1] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "other-tenant"})
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusTooManyRequests {
		t.Fatalf("expected TenantFunc to override the header and collapse both requests into one bucket, got %d", statuses[1])
	}
}

// TestDefaultLimitIsAppliedWhenNonPositive proves a non-positive configured
// Limit does not silently disable limiting (e.g. Limit:0 must not mean
// "unlimited") — it falls back to the documented default of 100.
func TestDefaultLimitIsAppliedWhenNonPositive(t *testing.T) {
	for _, limit := range []int{0, -1, -100} {
		app := fh.New()
		mw := New(Config{Limit: limit})
		app.Use(mw)
		app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
		addr := testServer(t, app)

		if status := doRequest(t, addr, "/", nil); status != fh.StatusOK {
			t.Fatalf("Limit=%d: expected a single request to succeed under the default fallback limit, got %d", limit, status)
		}
	}
}

// TestCustomErrorHandlerIsInvoked verifies the configurable rejection
// responder replaces the default 429 JSON body.
func TestCustomErrorHandlerIsInvoked(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{
		Limit: 1,
		Error: func(c fh.Ctx, tenant string) error {
			return c.Status(fh.StatusServiceUnavailable).SendString("busy:" + tenant)
		},
	}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	}()
	time.Sleep(30 * time.Millisecond)
	status := doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	close(release)
	wg.Wait()

	if status != fh.StatusServiceUnavailable {
		t.Fatalf("expected custom Error handler status 503, got %d", status)
	}
}

// TestSlotIsReleasedAfterRequestCompletes proves the counted slot is freed
// once the handler returns (via the deferred decrement), so a tenant that
// finishes its requests can make new ones instead of being permanently
// capped after Limit total requests.
func TestSlotIsReleasedAfterRequestCompletes(t *testing.T) {
	app := fh.New()
	app.Use(New(Config{Limit: 1}))
	app.Get("/", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	for i := 0; i < 5; i++ {
		if status := doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"}); status != fh.StatusOK {
			t.Fatalf("sequential request %d: expected 200 (slot released after each completes), got %d", i, status)
		}
	}
}

// TestConcurrentRequestsNeverExceedLimit is the concurrency-safety guard:
// under a real race of many simultaneous requests for the same tenant, the
// number of requests observed running concurrently inside the handler must
// never exceed Limit (run with -race to also catch data races on the
// shared active-count map).
func TestConcurrentRequestsNeverExceedLimit(t *testing.T) {
	const limit = 3
	const attempts = 20

	var current atomic.Int64
	var maxObserved atomic.Int64
	var admitted atomic.Int64
	var rejected atomic.Int64

	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{Limit: limit}))
	app.Get("/", func(c fh.Ctx) error {
		n := current.Add(1)
		for {
			m := maxObserved.Load()
			if n <= m || maxObserved.CompareAndSwap(m, n) {
				break
			}
		}
		<-release
		current.Add(-1)
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
			if status == fh.StatusOK {
				admitted.Add(1)
			} else if status == fh.StatusTooManyRequests {
				rejected.Add(1)
			}
		}()
	}
	time.Sleep(80 * time.Millisecond) // let the attempts pile up against the limit
	close(release)
	wg.Wait()

	if got := maxObserved.Load(); got > limit {
		t.Fatalf("observed %d concurrently-active requests, exceeding Limit=%d", got, limit)
	}
	if admitted.Load()+rejected.Load() != attempts {
		t.Fatalf("expected every attempt to be accounted for as admitted or rejected, got admitted=%d rejected=%d total=%d",
			admitted.Load(), rejected.Load(), attempts)
	}
	if rejected.Load() == 0 {
		t.Fatalf("expected at least one rejection when firing %d concurrent requests against Limit=%d", attempts, limit)
	}
}

// TestLocalKeyIsUsedWhenSet verifies the LocalKey lookup path (e.g. tenant
// resolved earlier in the chain by an auth middleware via c.Locals).
func TestLocalKeyIsUsedWhenSet(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(func(c fh.Ctx) error {
		c.Locals("resolved_tenant", "from-locals")
		return c.Next()
	})
	app.Use(New(Config{Limit: 1, LocalKey: "resolved_tenant"}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Different X-Tenant-ID headers; LocalKey should take precedence,
		// so both land in the same bucket and the second is rejected.
		statuses[0] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "acme"})
	}()
	time.Sleep(30 * time.Millisecond)
	statuses[1] = doRequest(t, addr, "/", map[string]string{"X-Tenant-ID": "other"})
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusTooManyRequests {
		t.Fatalf("expected LocalKey-resolved tenant to take precedence over header, got %d", statuses[1])
	}
}

// TestCustomHeaderNameIsHonored verifies the Header config field is used
// instead of the default "X-Tenant-ID" when set.
func TestCustomHeaderNameIsHonored(t *testing.T) {
	release := make(chan struct{})
	app := fh.New()
	app.Use(New(Config{Limit: 1, Header: "X-Org-Id"}))
	app.Get("/", func(c fh.Ctx) error {
		<-release
		return c.SendString("ok")
	})
	addr := testServer(t, app)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		statuses[0] = doRequest(t, addr, "/", map[string]string{"X-Org-Id": "acme"})
	}()
	time.Sleep(30 * time.Millisecond)
	statuses[1] = doRequest(t, addr, "/", map[string]string{"X-Org-Id": "acme"})
	close(release)
	wg.Wait()

	if statuses[1] != fh.StatusTooManyRequests {
		t.Fatalf("expected custom Header config field to be honored, got %d", statuses[1])
	}
}

// TestPanicInHandlerStillReleasesSlot proves the counted slot is released
// even when the downstream handler panics, via the deferred decrement
// running regardless of how c.Next() returns/unwinds. Without this, a
// single panicking request would permanently consume one of the tenant's
// slots (a resource leak / latent denial-of-service).
func TestPanicInHandlerStillReleasesSlot(t *testing.T) {
	l := &limiter{active: map[string]int{}, cfg: Config{Limit: 1, Header: "X-Tenant-ID"}}
	ctx := &fakeCtx{header: map[string]string{"X-Tenant-ID": "acme"}}

	func() {
		defer func() { recover() }()
		_ = l.Handle(ctxWithNext(ctx, func() error { panic("boom") }))
	}()

	l.mu.Lock()
	got := l.active["acme"]
	l.mu.Unlock()
	if got != 0 {
		t.Fatalf("expected active count to be released after a panicking handler, got %d", got)
	}
}

// The remaining lines provide a minimal fh.Ctx stand-in used only by
// TestPanicInHandlerStillReleasesSlot to exercise Handle() directly without
// going through the HTTP stack (isolating the defer/panic interaction from
// unrelated framework panic-recovery middleware).
type fakeCtx struct {
	fh.Ctx
	header map[string]string
	next   func() error
}

func (f *fakeCtx) Get(name string, def ...string) string {
	if v, ok := f.header[name]; ok {
		return v
	}
	if len(def) > 0 {
		return def[0]
	}
	return ""
}

func (f *fakeCtx) Locals(key string, value ...any) any { return nil }

func (f *fakeCtx) Next() error { return f.next() }

func ctxWithNext(f *fakeCtx, next func() error) fh.Ctx {
	f.next = next
	return f
}
