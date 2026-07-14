package circuitbreaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	ns atomic.Int64
}

func newFakeClock(at time.Time) *fakeClock {
	clock := &fakeClock{}
	clock.ns.Store(at.UnixNano())
	return clock
}

func (c *fakeClock) Now() time.Time      { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

func TestConsecutiveFailuresOpenAndReject(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	breaker := New(Config{
		FailureThreshold: 2,
		ResetAfter:       time.Second,
		Now:              clock.Now,
	})

	first, ok := breaker.admit(clock.Now())
	if !ok {
		t.Fatal("first request rejected")
	}
	breaker.complete(first, true, clock.Now())
	if breaker.State() != StateClosed {
		t.Fatalf("state = %s, want closed", breaker.State())
	}

	second, ok := breaker.admit(clock.Now())
	if !ok {
		t.Fatal("second request rejected")
	}
	breaker.complete(second, true, clock.Now())
	if breaker.State() != StateOpen {
		t.Fatalf("state = %s, want open", breaker.State())
	}
	if _, ok := breaker.admit(clock.Now()); ok {
		t.Fatal("open breaker admitted a request")
	}
}

func TestHalfOpenIsBounded(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	breaker := New(Config{
		FailureThreshold:    1,
		SuccessThreshold:    2,
		HalfOpenMaxRequests: 2,
		ResetAfter:          time.Second,
		Now:                 clock.Now,
	})
	request, _ := breaker.admit(clock.Now())
	breaker.complete(request, true, clock.Now())
	clock.Add(time.Second)

	probe1, ok := breaker.admit(clock.Now())
	if !ok {
		t.Fatal("first probe rejected")
	}
	probe2, ok := breaker.admit(clock.Now())
	if !ok {
		t.Fatal("second probe rejected")
	}
	if _, ok := breaker.admit(clock.Now()); ok {
		t.Fatal("half-open breaker exceeded probe limit")
	}

	breaker.complete(probe1, false, clock.Now())
	if breaker.State() != StateHalfOpen {
		t.Fatalf("closed with an in-flight probe: %s", breaker.State())
	}
	breaker.complete(probe2, false, clock.Now())
	if breaker.State() != StateClosed {
		t.Fatalf("state = %s, want closed", breaker.State())
	}
}

func TestLateClosedCompletionCannotCorruptNewGeneration(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	breaker := New(Config{
		FailureThreshold: 1,
		ResetAfter:       time.Second,
		Now:              clock.Now,
	})

	lateSuccess, _ := breaker.admit(clock.Now())
	failure, _ := breaker.admit(clock.Now())
	breaker.complete(failure, true, clock.Now())
	if breaker.State() != StateOpen {
		t.Fatal("breaker did not open")
	}
	breaker.complete(lateSuccess, false, clock.Now())
	if breaker.State() != StateOpen {
		t.Fatal("late completion changed the current generation")
	}
}

func TestPendingHalfOpenFailureWins(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	breaker := New(Config{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		HalfOpenMaxRequests: 2,
		ResetAfter:          time.Second,
		Now:                 clock.Now,
	})
	request, _ := breaker.admit(clock.Now())
	breaker.complete(request, true, clock.Now())
	clock.Add(time.Second)

	successProbe, _ := breaker.admit(clock.Now())
	failureProbe, _ := breaker.admit(clock.Now())
	breaker.complete(successProbe, false, clock.Now())
	if breaker.State() != StateHalfOpen {
		t.Fatal("breaker closed before all probes completed")
	}
	breaker.complete(failureProbe, true, clock.Now())
	if breaker.State() != StateOpen {
		t.Fatalf("state = %s, want open", breaker.State())
	}
}

func TestRollingFailureRateOpens(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	breaker := New(Config{
		FailureThreshold:     100,
		FailureRateThreshold: 0.5,
		MinimumRequests:      4,
		RollingWindow:        10 * time.Second,
		RollingBuckets:       5,
		Now:                  clock.Now,
	})

	for i, failed := range []bool{true, false, true, false} {
		request, ok := breaker.admit(clock.Now())
		if !ok {
			t.Fatalf("request %d rejected", i)
		}
		breaker.complete(request, failed, clock.Now())
	}
	if breaker.State() != StateOpen {
		t.Fatalf("state = %s, want open", breaker.State())
	}
}

func TestConcurrentClosedTraffic(t *testing.T) {
	breaker := New(Config{FailureThreshold: 1_000_000})
	const goroutines = 32
	const perGoroutine = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				request, ok := breaker.admit(time.Now())
				if !ok {
					t.Errorf("request rejected")
					return
				}
				breaker.complete(request, false, time.Now())
			}
		}()
	}
	wg.Wait()
	snapshot := breaker.Snapshot()
	want := uint64(goroutines * perGoroutine)
	if snapshot.Accepted != want || snapshot.Succeeded != want {
		t.Fatalf("accepted=%d succeeded=%d want=%d", snapshot.Accepted, snapshot.Succeeded, want)
	}
}

func BenchmarkClosedSuccess(b *testing.B) {
	breaker := New(Config{FailureThreshold: 5})
	now := time.Now()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			request, ok := breaker.admit(now)
			if !ok {
				b.Fatal("unexpected rejection")
			}
			breaker.complete(request, false, now)
		}
	})
}
