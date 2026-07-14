package fh

import (
	"testing"
	"time"
)

func newTestTracker() *SLOTracker {
	t := NewSLOTracker(SLOTrackerConfig{CheckInterval: time.Hour})
	t.Register("/api/users", SLO{Availability: 0.999})
	t.Register("/api/users/:id", SLO{Availability: 0.999})
	t.Register("/api/users/:id/posts/:postID", SLO{Availability: 0.999})
	t.Register("/files/*", SLO{Availability: 0.99})
	t.Register(`^/api/v[0-9]+/reports$`, SLO{Availability: 0.995})
	return t
}

func TestSLOTrackerMatch(t *testing.T) {
	tracker := newTestTracker()
	defer tracker.Stop()

	cases := []struct {
		path  string
		route string
		ok    bool
	}{
		{"/api/users", "/api/users", true},
		{"/api/users?page=2", "/api/users", true},
		{"/api/users/42", "/api/users/:id", true},
		{"/api/users/42/posts/7", "/api/users/:id/posts/:postID", true},
		{"/files/a/b/c.txt", "/files/*", true},
		{"/api/v1/reports", `^/api/v[0-9]+/reports$`, true},
		{"/api/v12/reports", `^/api/v[0-9]+/reports$`, true},
		{"/api/vx/reports", "", false},
		{"/unrelated", "", false},
	}
	for _, tc := range cases {
		route, ok := tracker.Match(tc.path)
		if ok != tc.ok || route != tc.route {
			t.Errorf("Match(%q) = (%q, %v), want (%q, %v)", tc.path, route, ok, tc.route, tc.ok)
		}
	}

	// Cached second lookup must agree.
	route, ok := tracker.Match("/api/users/42")
	if !ok || route != "/api/users/:id" {
		t.Errorf("cached Match(/api/users/42) = (%q, %v)", route, ok)
	}
}

func TestSLOTrackerUnregister(t *testing.T) {
	tracker := newTestTracker()
	defer tracker.Stop()

	if _, ok := tracker.Match("/files/x"); !ok {
		t.Fatal("expected /files/* to match before unregister")
	}
	if !tracker.Unregister("/files/*") {
		t.Fatal("Unregister(/files/*) = false")
	}
	if _, ok := tracker.Match("/files/x"); ok {
		t.Fatal("expected no match after unregister (cache must be invalidated)")
	}
	if tracker.Unregister("/files/*") {
		t.Fatal("second Unregister should report false")
	}
}

func TestSLOTrackerRecordAndSnapshot(t *testing.T) {
	tracker := newTestTracker()
	defer tracker.Stop()

	for i := range 10 {
		tracker.RecordRequest("/api/users/:id", 50*time.Millisecond, i == 0)
	}

	snap, ok := tracker.GetState("/api/users/:id")
	if !ok {
		t.Fatal("GetState returned !ok for registered route")
	}
	if snap.TotalRequests != 10 || snap.FailedRequests != 1 || snap.SuccessRequests != 9 {
		t.Errorf("counts = %d/%d/%d, want 10/1/9",
			snap.TotalRequests, snap.FailedRequests, snap.SuccessRequests)
	}
	if snap.WindowRequests != 10 || snap.WindowFailed != 1 {
		t.Errorf("window counts = %d/%d, want 10/1", snap.WindowRequests, snap.WindowFailed)
	}
	if snap.P99 < 49 || snap.P99 > 51 {
		t.Errorf("P99 = %v ms, want ~50", snap.P99)
	}
	if snap.Route != "/api/users/:id" {
		t.Errorf("Route = %q", snap.Route)
	}
}

func TestSLOCheckDetectsViolationAndRecovery(t *testing.T) {
	alerts := make(chan string, 1)
	recoveries := make(chan string, 1)
	tracker := NewSLOTracker(SLOTrackerConfig{
		CheckInterval:  time.Hour,
		AlertThreshold: 2.0,
		OnAlert:        func(route string, _ SLOSnapshot) { alerts <- route },
		OnRecovery:     func(route string, _ SLOSnapshot) { recoveries <- route },
	})
	defer tracker.Stop()
	tracker.Register("/api/orders/:id", SLO{Availability: 0.999})

	// 50% failures: burn rate far above threshold.
	for i := range 100 {
		tracker.RecordRequest("/api/orders/:id", 10*time.Millisecond, i%2 == 0)
	}
	tracker.checkAll()

	select {
	case route := <-alerts:
		if route != "/api/orders/:id" {
			t.Errorf("alert route = %q, want /api/orders/:id", route)
		}
	default:
		t.Fatal("expected an alert for 50% error rate")
	}
	if tracker.IsCompliant("/api/orders/:id") {
		t.Error("route should be non-compliant")
	}
	if tracker.BurnRate("/api/orders/:id") < 2.0 {
		t.Errorf("burn rate = %v, want >= 2", tracker.BurnRate("/api/orders/:id"))
	}
}

func TestSLOLatencyTargets(t *testing.T) {
	tracker := NewSLOTracker(SLOTrackerConfig{CheckInterval: time.Hour})
	defer tracker.Stop()
	tracker.Register("/slow", SLO{Availability: 0.9, P95Latency: 10 * time.Millisecond})

	for range 100 {
		tracker.RecordRequest("/slow", 50*time.Millisecond, false)
	}
	tracker.checkAll()

	if tracker.IsCompliant("/slow") {
		t.Error("route exceeding P95 target should be non-compliant")
	}
}
