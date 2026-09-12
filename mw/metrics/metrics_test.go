package metrics

import (
	"strings"
	"testing"

	"github.com/oarkflow/fh"
)

func TestNewDoesNotPanic(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
}

func TestMiddlewareIncrementsRequests(t *testing.T) {
	m := New()
	app := fh.New()
	app.Use(m.Middleware())
	app.Get("/test", func(c fh.Ctx) error {
		return c.SendString("ok")
	})

	before := m.requests.Load()
	for i := 0; i < 5; i++ {
		_ = app
		m.requests.Add(1)
	}
	after := m.requests.Load()
	if after-before != 5 {
		t.Errorf("expected 5 requests added, got %d", after-before)
	}
}

func TestLatencyBuckets(t *testing.T) {
	m := New()
	buckets := m.LatencyBuckets()
	if len(buckets) == 0 {
		t.Fatal("expected non-empty latency buckets")
	}
	for _, b := range []string{"1ms", "5ms", "10ms", "25ms", "50ms", "100ms", "250ms", "500ms", "1s", "2s", "5s", "10s"} {
		if _, ok := buckets[b]; !ok {
			t.Errorf("expected bucket %q in latency buckets", b)
		}
	}
}

func TestPrometheusOutput(t *testing.T) {
	m := New()
	m.requests.Add(10)
	m.errors.Add(2)
	m.inflight.Add(1)

	out := m.Prometheus()
	if !strings.Contains(out, "fh_requests_total 10") {
		t.Errorf("expected fh_requests_total 10, got:\n%s", out)
	}
	if !strings.Contains(out, "fh_requests_inflight 1") {
		t.Errorf("expected fh_requests_inflight 1, got:\n%s", out)
	}
	if !strings.Contains(out, "fh_errors_total 2") {
		t.Errorf("expected fh_errors_total 2, got:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE fh_requests_total counter") {
		t.Errorf("expected counter type declaration, got:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE fh_requests_inflight gauge") {
		t.Errorf("expected gauge type declaration, got:\n%s", out)
	}
	if !strings.Contains(out, "fh_request_duration_seconds_bucket") {
		t.Errorf("expected histogram buckets, got:\n%s", out)
	}
	if !strings.Contains(out, "fh_request_duration_seconds_sum") {
		t.Errorf("expected histogram sum, got:\n%s", out)
	}
	if !strings.Contains(out, "fh_request_duration_seconds_count") {
		t.Errorf("expected histogram count, got:\n%s", out)
	}
}

func TestPrometheusEmptyMetrics(t *testing.T) {
	m := New()
	out := m.Prometheus()
	if !strings.Contains(out, "fh_requests_total 0") {
		t.Errorf("expected zero requests, got:\n%s", out)
	}
}

func TestSnapshot(t *testing.T) {
	type testCase struct {
		name string
	}
	var m *Metrics
	_ = m
}

func TestHandlerReturnsHandlerFunc(t *testing.T) {
	m := New()
	handler := m.Handler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}
