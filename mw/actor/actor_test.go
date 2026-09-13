package actor_test

import (
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/actor"
)

// TestNilKeyFuncPassesThrough ensures a middleware misconfigured with no Key
// function degrades to a no-op instead of hanging or panicking.
func TestNilKeyFuncPassesThrough(t *testing.T) {
	app := fh.New()
	app.Use(actor.New(actor.Config{}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
}

// TestEmptyKeyPassesThrough ensures a Key func returning "" (e.g. an
// unauthenticated request with no actor identity) does not try to lock on
// an empty key, and the request still completes.
func TestEmptyKeyPassesThrough(t *testing.T) {
	app := fh.New()
	app.Use(actor.New(actor.Config{Key: func(fh.Ctx) string { return "" }}))
	app.Get("/x", func(c fh.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest("GET", "/x", nil))
	if err != nil || resp.StatusCode != fh.StatusOK {
		t.Fatalf("status=%v err=%v", resp, err)
	}
}

// TestSameKeySerializesConcurrentRequests is the core correctness guarantee:
// two requests that resolve to the same actor key must never run their
// downstream handler concurrently. Each app.Test call spins up its own app
// instance (app.Test tears its app down once its request completes, so
// sharing one app across concurrently in-flight requests would kill the
// others mid-flight); the per-key mutex lives in actor's package-level
// registry, so it is still shared across these independent app instances.
func TestSameKeySerializesConcurrentRequests(t *testing.T) {
	key := uniqueKey(t)

	var mu sync.Mutex
	active := 0
	maxActive := 0
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	newHandler := func() fh.HandlerFunc {
		return func(c fh.Ctx) error {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()

			started <- struct{}{}
			<-release

			mu.Lock()
			active--
			mu.Unlock()
			return c.SendString("ok")
		}
	}

	buildApp := func() *fh.App {
		app := fh.New()
		app.Use(actor.New(actor.Config{Key: func(fh.Ctx) string { return key }}))
		app.Get("/x", newHandler())
		return app
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		app := buildApp()
		if _, err := app.Test(httptest.NewRequest("GET", "/x", nil), 5000); err != nil {
			t.Errorf("request 1: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		app := buildApp()
		if _, err := app.Test(httptest.NewRequest("GET", "/x", nil), 5000); err != nil {
			t.Errorf("request 2: %v", err)
		}
	}()

	<-started // first handler is now inside the critical section
	select {
	case <-started:
		t.Fatal("second handler entered the critical section while the first still holds the per-key lock")
	case <-time.After(75 * time.Millisecond):
		// Expected: second request is blocked waiting for the same-key lock.
	}

	release <- struct{}{} // let the first handler finish and release the lock
	<-started             // second handler can now proceed
	release <- struct{}{} // let the second handler finish

	wg.Wait()

	mu.Lock()
	got := maxActive
	mu.Unlock()
	if got != 1 {
		t.Fatalf("expected at most 1 concurrently active handler for the same key, observed %d", got)
	}
}

// TestDifferentKeysRunConcurrently ensures the per-key locking does not
// degrade into a global lock: two requests with different actor keys must
// be able to run their handlers concurrently.
func TestDifferentKeysRunConcurrently(t *testing.T) {
	keyA := uniqueKey(t) + "-a"
	keyB := uniqueKey(t) + "-b"

	both := make(chan struct{}, 2)
	release := make(chan struct{})

	buildApp := func(key string) *fh.App {
		app := fh.New()
		app.Use(actor.New(actor.Config{Key: func(fh.Ctx) string { return key }}))
		app.Get("/x", func(c fh.Ctx) error {
			both <- struct{}{}
			<-release
			return c.SendString("ok")
		})
		return app
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		app := buildApp(keyA)
		_, _ = app.Test(httptest.NewRequest("GET", "/x", nil), 5000)
	}()
	go func() {
		defer wg.Done()
		app := buildApp(keyB)
		_, _ = app.Test(httptest.NewRequest("GET", "/x", nil), 5000)
	}()

	// Both handlers must be able to reach the rendezvous point without
	// waiting on each other; if locking were global this would deadlock
	// until the test's timeout.
	deadline := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-both:
		case <-deadline:
			t.Fatal("expected both differently-keyed handlers to run concurrently, but they serialized")
		}
	}
	close(release)
	wg.Wait()
}

// TestManyRequestsSameKeyAllComplete is a broader stress/race pass: many
// concurrent requests sharing one key must all complete successfully with
// no deadlock, panic, or data race in the registry's LoadOrStore path.
func TestManyRequestsSameKeyAllComplete(t *testing.T) {
	key := uniqueKey(t)
	var completed atomic.Int64

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app := fh.New()
			app.Use(actor.New(actor.Config{Key: func(fh.Ctx) string { return key }}))
			app.Get("/x", func(c fh.Ctx) error {
				completed.Add(1)
				return c.SendString("ok")
			})
			resp, err := app.Test(httptest.NewRequest("GET", "/x", nil), 5000)
			if err != nil || resp.StatusCode != fh.StatusOK {
				t.Errorf("status=%v err=%v", resp, err)
			}
		}()
	}
	wg.Wait()

	if got := completed.Load(); got != n {
		t.Fatalf("expected all %d requests to complete, got %d", n, got)
	}
}

// TestHandlerPanicDoesNotLeaveLockHeld makes sure a panicking downstream
// handler still unlocks the per-key mutex (via the deferred Unlock), so a
// single bad request cannot permanently wedge every future request for that
// actor key.
func TestHandlerPanicDoesNotLeaveLockHeld(t *testing.T) {
	key := uniqueKey(t)

	app := fh.New()
	app.Use(actor.New(actor.Config{Key: func(fh.Ctx) string { return key }}))
	app.Get("/panic", func(c fh.Ctx) error { panic("boom") })
	resp, err := app.Test(httptest.NewRequest("GET", "/panic", nil), 2000)
	if err != nil {
		t.Fatalf("expected the panic to be converted into an error response, got transport error: %v", err)
	}
	if resp.StatusCode < 500 {
		t.Fatalf("expected a 5xx response for the panicking handler, got %d", resp.StatusCode)
	}

	// A subsequent request for the same key must not be blocked forever by
	// a lock the panicking handler failed to release.
	app2 := fh.New()
	app2.Use(actor.New(actor.Config{Key: func(fh.Ctx) string { return key }}))
	app2.Get("/panic", func(c fh.Ctx) error { return c.SendString("ok") })
	resp2, err2 := app2.Test(httptest.NewRequest("GET", "/panic", nil), 2000)
	if err2 != nil || resp2.StatusCode != fh.StatusOK {
		t.Fatalf("expected the lock to be released after a panic, status=%v err=%v", resp2, err2)
	}
}

var keyCounter atomic.Int64

// uniqueKey returns a key unique to this test run. The actor registry is a
// package-level sync.Map that is never cleaned up, so tests must not reuse
// keys across cases or they would contend on locks left behind by earlier
// tests/subtests.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return t.Name() + "-" + strconv.FormatInt(keyCounter.Add(1), 10)
}
