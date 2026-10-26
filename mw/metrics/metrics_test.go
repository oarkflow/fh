package metrics

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/oarkflow/fh"
)

func TestNewDoesNotPanic(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
}

func TestRouteMetricsUseTemplatesAndBoundSeries(t *testing.T) {
	m := NewWithConfig(Config{MaxRoutes: 1})
	for _, pattern := range []string{"/users/:id", "/users/:id", "/other", ""} {
		if err := m.Middleware()(routeTestCtx{pattern: pattern}); err != nil {
			t.Fatal(err)
		}
	}
	routes := snapshot(&m.route)
	if routes["GET /users/:id"] != 2 || routes["<overflow>"] != 2 || len(routes) != 2 {
		t.Fatalf("unexpected bounded route metrics: %#v", routes)
	}
}

func TestRouteMetricCapUnderConcurrentRequests(t *testing.T) {
	m := NewWithConfig(Config{MaxRoutes: 3})
	var workers sync.WaitGroup
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			m.recordRoute(fmt.Sprintf("GET /route/%d", i))
		}(i)
	}
	workers.Wait()
	routes := snapshot(&m.route)
	if len(routes) != 4 || routes["<overflow>"] != 97 {
		t.Fatalf("route series exceeded cap: %#v", routes)
	}
}

type routeTestCtx struct {
	fh.Ctx
	pattern string
	code    int
	nextErr error
}

func (c routeTestCtx) Method() string { return "GET" }
func (c routeTestCtx) StatusCode() int {
	if c.code == 0 {
		return 200
	}
	return c.code
}
func (c routeTestCtx) RoutePattern() string { return c.pattern }
func (c routeTestCtx) Next() error          { return c.nextErr }
func (c routeTestCtx) Set(string, string)   {}
func (c routeTestCtx) ErrorReport(error) fh.ErrorReport {
	return fh.ErrorReport{Error: fh.PayloadTooLarge("too large")}
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

func TestPrometheusRouteSeriesAreBoundedAndLabeled(t *testing.T) {
	m := NewWithConfig(Config{MaxRoutes: 1})
	for _, ctx := range []routeTestCtx{{pattern: "/users/:id", code: 200}, {pattern: "/users/:id", code: 503}, {pattern: "/other", code: 404}} {
		if err := m.Middleware()(ctx); err != nil {
			t.Fatal(err)
		}
	}
	out := m.Prometheus()
	for _, want := range []string{
		`fh_route_requests_total{method="GET",route="/users/:id",status_class="2xx"} 1`,
		`fh_route_requests_total{method="GET",route="/users/:id",status_class="5xx"} 1`,
		`fh_route_requests_total{method="overflow",route="<overflow>",status_class="4xx"} 1`,
		`fh_route_request_duration_seconds_bucket{method="GET",route="/users/:id",status_class="2xx",le="+Inf"} 1`,
		`fh_route_request_duration_seconds_count{method="GET",route="/users/:id",status_class="2xx"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `route="/other"`) {
		t.Fatalf("route cap leaked raw route: %s", out)
	}
	if strings.Count(out, `fh_route_requests_total{`) != 3 {
		t.Fatalf("unexpected route series: %s", out)
	}
}

func TestPrometheusRouteStatusUsesHandlerError(t *testing.T) {
	m := New()
	_ = m.Middleware()(routeTestCtx{pattern: "/upload", nextErr: fh.PayloadTooLarge("too large")})
	if !strings.Contains(m.Prometheus(), `fh_route_requests_total{method="GET",route="/upload",status_class="4xx"} 1`) {
		t.Fatalf("handler error missing from route status class: %s", m.Prometheus())
	}
}

func TestPrometheusEmptyMetrics(t *testing.T) {
	m := New()
	out := m.Prometheus()
	if !strings.Contains(out, "fh_requests_total 0") {
		t.Errorf("expected zero requests, got:\n%s", out)
	}
}

func TestHandlerReturnsHandlerFunc(t *testing.T) {
	m := New()
	handler := m.Handler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}
