package loadtest

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/replay"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

// errSimulatedOutage is returned by flakyStore in place of whatever a real
// remote backend (Redis, PostgreSQL, ...) would return on a connectivity
// failure, so the test can assert on how fh's middleware actually reacts —
// without needing a real outage-capable external service.
var errSimulatedOutage = errors.New("loadtest: simulated shared-store outage")

// flakyProvider wraps a real kv.Provider (in-memory here — any Provider
// works identically since the fault injection lives at the Store level) and
// hands out flakyStore instances that can simulate an outage (return an
// error on every operation) or artificial latency on demand, controlled from
// the test via atomics shared across every namespace it opens.
type flakyProvider struct {
	inner   kv.Provider
	outage  *atomic.Bool
	latency *atomic.Int64 // nanoseconds
}

func newFlakyProvider() *flakyProvider {
	return &flakyProvider{
		inner:   kv.NewMemoryProvider(),
		outage:  new(atomic.Bool),
		latency: new(atomic.Int64),
	}
}

func (p *flakyProvider) SetOutage(on bool)          { p.outage.Store(on) }
func (p *flakyProvider) SetLatency(d time.Duration) { p.latency.Store(int64(d)) }

func (p *flakyProvider) Store(ctx context.Context, namespace string) (kv.Store, error) {
	inner, err := p.inner.Store(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return &flakyStore{inner: inner, outage: p.outage, latency: p.latency}, nil
}

func (p *flakyProvider) Close() error { return p.inner.Close() }

type flakyStore struct {
	inner   kv.Store
	outage  *atomic.Bool
	latency *atomic.Int64
}

func (s *flakyStore) fault() error {
	if d := time.Duration(s.latency.Load()); d > 0 {
		time.Sleep(d)
	}
	if s.outage.Load() {
		return errSimulatedOutage
	}
	return nil
}

func (s *flakyStore) Get(key string) ([]byte, bool, error) {
	if err := s.fault(); err != nil {
		return nil, false, err
	}
	return s.inner.Get(key)
}

func (s *flakyStore) Set(key string, value []byte, ttl time.Duration) error {
	if err := s.fault(); err != nil {
		return err
	}
	return s.inner.Set(key, value, ttl)
}

func (s *flakyStore) Delete(key string) error {
	if err := s.fault(); err != nil {
		return err
	}
	return s.inner.Delete(key)
}

func (s *flakyStore) Len() (int, error) {
	if err := s.fault(); err != nil {
		return 0, err
	}
	return s.inner.Len()
}

func (s *flakyStore) Close() error { return s.inner.Close() }

func (s *flakyStore) Mutate(key string, fn func([]byte, bool) ([]byte, time.Duration, bool, error)) error {
	if err := s.fault(); err != nil {
		return err
	}
	return s.inner.Mutate(key, fn)
}

var _ kv.Provider = (*flakyProvider)(nil)
var _ kv.Store = (*flakyStore)(nil)

// TestSharedStoreOutageFailsClosedAndRecovers wires mw/ratelimiter (via
// docs/shared-state.md's documented fh.WithSharedState pattern) to a store
// that can simulate a backend outage on demand. It asserts the app does not
// panic or hang when the shared store starts failing — reading
// ratelimiter.go shows it returns the store error straight up the middleware
// chain, which fh's default error handler turns into a 500, i.e. the
// documented behavior here is fail-closed, not fail-open — and that it
// recovers cleanly (200s resume) once the simulated outage ends (concern
// #8).
func TestSharedStoreOutageFailsClosedAndRecovers(t *testing.T) {
	store := newFlakyProvider()
	app := fh.New(fh.WithSharedState(store))
	// Max set high enough that rate limiting itself never trips during this
	// test — the thing under test is outage behavior, not limiting math.
	app.Use(ratelimiter.New(ratelimiter.Config{
		Store:  app.MustStateStore("ratelimit/outage-test"),
		Max:    100000,
		Window: time.Minute,
	}))
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr := startApp(t, app)

	client := &http.Client{Timeout: 2 * time.Second}

	get := func() (int, error) {
		resp, err := client.Get("http://" + addr + "/ping")
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, nil
	}

	// Before the outage: the store is healthy, requests succeed.
	if status, err := get(); err != nil || status != http.StatusOK {
		t.Fatalf("pre-outage request: status=%d err=%v, want 200", status, err)
	}

	// During the outage: must not hang (client.Timeout bounds this) and must
	// not panic the server (asserted implicitly — a panic would surface as a
	// closed connection / transport error here and the health check below
	// would then fail).
	store.SetOutage(true)
	status, err := get()
	if err != nil {
		t.Fatalf("request during outage errored instead of returning a clean HTTP response: %v", err)
	}
	if status == http.StatusOK {
		t.Fatalf("request during simulated outage returned 200 — expected fail-closed (5xx), the store error was silently swallowed")
	}
	if status < 500 {
		t.Fatalf("request during simulated outage returned %d, want a 5xx fail-closed response", status)
	}

	// End the outage: the app must recover without needing a restart.
	store.SetOutage(false)
	if !waitUntil(2*time.Second, 20*time.Millisecond, func() bool {
		status, err := get()
		return err == nil && status == http.StatusOK
	}) {
		t.Fatalf("app did not recover to serving 200s after the simulated outage ended")
	}
}

// TestSharedStoreLatencyDoesNotHangOrError proves that added backend latency
// (as opposed to a hard outage) shows up as request latency, not as errors
// or an indefinite hang, by injecting a bounded delay into every store
// operation and confirming requests still complete correctly, just slower
// (concern #8: shared-store latency).
func TestSharedStoreLatencyDoesNotHangOrError(t *testing.T) {
	store := newFlakyProvider()
	app := fh.New(fh.WithSharedState(store))
	app.Use(ratelimiter.New(ratelimiter.Config{
		Store:  app.MustStateStore("ratelimit/latency-test"),
		Max:    100000,
		Window: time.Minute,
	}))
	app.Get("/ping", func(c fh.Ctx) error { return c.SendString("pong") })
	addr := startApp(t, app)

	store.SetLatency(80 * time.Millisecond)
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < 5; i++ {
		start := time.Now()
		resp, err := client.Get("http://" + addr + "/ping")
		if err != nil {
			t.Fatalf("request %d with injected latency: %v", i, err)
		}
		elapsed := time.Since(start)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d with injected latency: status=%d, want 200", i, resp.StatusCode)
		}
		if elapsed < 80*time.Millisecond {
			t.Fatalf("request %d completed in %s, faster than the injected 80ms store latency — latency injection did not take effect", i, elapsed)
		}
	}
}

// TestSharedStoreOutageReplayMiddlewareFailsClosed repeats the outage check
// against mw/replay, whose source (replay.go) also returns the store error
// directly rather than treating it as "not seen" — confirming the same
// fail-closed behavior holds for a second real consumer of kv.Store, not
// just ratelimiter (concern #8).
func TestSharedStoreOutageReplayMiddlewareFailsClosed(t *testing.T) {
	store := newFlakyProvider()
	app := fh.New(fh.WithSharedState(store))
	app.Use(replay.New(replay.Config{
		Store:      app.MustStateStore("replay/outage-test"),
		MaxEntries: 100000,
		Key:        func(c fh.Ctx) string { return c.Get("X-Nonce") },
	}))
	app.Post("/webhook", func(c fh.Ctx) error { return c.SendString("accepted") })
	addr := startApp(t, app)

	client := &http.Client{Timeout: 2 * time.Second}
	post := func(nonce string) (int, error) {
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/webhook", nil)
		if err != nil {
			return 0, err
		}
		req.Header.Set("X-Nonce", nonce)
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		return resp.StatusCode, nil
	}

	if status, err := post("nonce-1"); err != nil || status != http.StatusOK {
		t.Fatalf("pre-outage request: status=%d err=%v, want 200", status, err)
	}

	store.SetOutage(true)
	status, err := post("nonce-2")
	if err != nil {
		t.Fatalf("request during outage errored instead of a clean HTTP response: %v", err)
	}
	if status < 500 {
		t.Fatalf("replay middleware during simulated outage returned %d, want a 5xx fail-closed response", status)
	}

	store.SetOutage(false)
	if !waitUntil(2*time.Second, 20*time.Millisecond, func() bool {
		status, err := post("nonce-3")
		return err == nil && status == http.StatusOK
	}) {
		t.Fatalf("app did not recover to serving 200s after the simulated outage ended")
	}
}
