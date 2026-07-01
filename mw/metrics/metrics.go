package metrics

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

// Metrics provides lightweight in-process RED metrics without external dependencies.
// It exposes JSON by default and Prometheus text format with ?format=prometheus.
type Metrics struct {
	started  time.Time
	requests atomic.Uint64
	inflight atomic.Int64
	status   sync.Map
	route    sync.Map
	latency  [12]atomic.Uint64
	totalNS  atomic.Uint64
	errors   atomic.Uint64
}

var buckets = []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}

func New() *Metrics { return &Metrics{started: time.Now()} }

func (m *Metrics) Middleware() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		m.requests.Add(1)
		m.inflight.Add(1)
		start := time.Now()
		defer func() {
			dur := time.Since(start)
			m.inflight.Add(-1)
			m.totalNS.Add(uint64(dur.Nanoseconds()))
			for i, b := range buckets {
				if dur <= b {
					m.latency[i].Add(1)
					break
				}
			}
			code := c.StatusCode()
			if code >= 500 {
				m.errors.Add(1)
			}
			v, _ := m.status.LoadOrStore(strconv.Itoa(code), &atomic.Uint64{})
			v.(*atomic.Uint64).Add(1)
			v, _ = m.route.LoadOrStore(c.Method()+" "+c.Path(), &atomic.Uint64{})
			v.(*atomic.Uint64).Add(1)
			c.Set("Server-Timing", fmt.Sprintf("app;dur=%d", dur.Milliseconds()))
		}()
		return c.Next()
	}
}

func (m *Metrics) Handler() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if c.Query("format") == "prometheus" {
			return c.Type("text/plain; version=0.0.4").SendString(m.Prometheus())
		}
		return c.JSON(fh.Map{"uptime_seconds": int64(time.Since(m.started).Seconds()), "requests_total": m.requests.Load(), "requests_inflight": m.inflight.Load(), "errors_total": m.errors.Load(), "latency_buckets": m.LatencyBuckets(), "status": snapshot(&m.status), "routes": snapshot(&m.route)})
	}
}

func (m *Metrics) Prometheus() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP fh_requests_total Total HTTP requests handled by fh.\n# TYPE fh_requests_total counter\nfh_requests_total %d\n", m.requests.Load())
	fmt.Fprintf(&b, "# HELP fh_requests_inflight Current in-flight HTTP requests.\n# TYPE fh_requests_inflight gauge\nfh_requests_inflight %d\n", m.inflight.Load())
	fmt.Fprintf(&b, "# HELP fh_errors_total Total HTTP 5xx responses.\n# TYPE fh_errors_total counter\nfh_errors_total %d\n", m.errors.Load())
	fmt.Fprintf(&b, "# HELP fh_request_duration_seconds Request duration histogram.\n# TYPE fh_request_duration_seconds histogram\n")
	var cumulative uint64
	for i, bucket := range buckets {
		cumulative += m.latency[i].Load()
		fmt.Fprintf(&b, "fh_request_duration_seconds_bucket{le=\"%.3f\"} %d\n", bucket.Seconds(), cumulative)
	}
	fmt.Fprintf(&b, "fh_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.requests.Load())
	fmt.Fprintf(&b, "fh_request_duration_seconds_sum %.9f\nfh_request_duration_seconds_count %d\n", float64(m.totalNS.Load())/1e9, m.requests.Load())
	m.status.Range(func(k, v any) bool {
		fmt.Fprintf(&b, "fh_responses_total{status=\"%s\"} %d\n", k, v.(*atomic.Uint64).Load())
		return true
	})
	return b.String()
}

func (m *Metrics) LatencyBuckets() map[string]uint64 {
	out := make(map[string]uint64, len(buckets)+1)
	for i, b := range buckets {
		out[b.String()] = m.latency[i].Load()
	}
	return out
}

func snapshot(m *sync.Map) map[string]uint64 {
	out := map[string]uint64{}
	m.Range(func(k, v any) bool { out[fmt.Sprint(k)] = v.(*atomic.Uint64).Load(); return true })
	return out
}
