package metrics

import (
	"fmt"
	"sort"
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
	started    time.Time
	maxRoutes  int
	requests   atomic.Uint64
	inflight   atomic.Int64
	status     sync.Map
	route      sync.Map
	routeStats sync.Map
	routeMu    sync.Mutex
	routeCount int
	latency    [12]atomic.Uint64
	totalNS    atomic.Uint64
	errors     atomic.Uint64
}

type routeClassStats struct {
	requests atomic.Uint64
	latency  [12]atomic.Uint64
	totalNS  atomic.Uint64
}

type routeSeries struct {
	classes [6]routeClassStats
}

var buckets = []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}

// Config bounds the number of distinct route series retained in memory.
type Config struct {
	MaxRoutes int
}

func New() *Metrics { return NewWithConfig(Config{}) }

func NewWithConfig(config Config) *Metrics {
	maxRoutes := 256
	if config.MaxRoutes > 0 {
		maxRoutes = config.MaxRoutes
	}
	return &Metrics{started: time.Now(), maxRoutes: maxRoutes}
}

func (m *Metrics) Middleware() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		m.requests.Add(1)
		m.inflight.Add(1)
		start := time.Now()
		var handlerErr error
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
			if handlerErr != nil && (code == 0 || code == fh.StatusOK) {
				code = c.ErrorReport(handlerErr).Error.Status
			} else if code == 0 {
				code = fh.StatusOK
			}
			if code >= 500 {
				m.errors.Add(1)
			}
			v, _ := m.status.LoadOrStore(strconv.Itoa(code), &atomic.Uint64{})
			v.(*atomic.Uint64).Add(1)
			key := m.recordRoute(routeMetricKey(c))
			m.recordRouteDuration(key, code, dur)
			c.Set("Server-Timing", fmt.Sprintf("app;dur=%d", dur.Milliseconds()))
		}()
		handlerErr = c.Next()
		return handlerErr
	}
}

func routeMetricKey(c fh.Ctx) string {
	pattern := "<unmatched>"
	if routed, ok := c.(interface{ RoutePattern() string }); ok {
		if p := routed.RoutePattern(); p != "" {
			pattern = p
		}
	}
	return c.Method() + " " + pattern
}

func (m *Metrics) recordRoute(key string) string {
	if value, ok := m.route.Load(key); ok {
		value.(*atomic.Uint64).Add(1)
		return key
	}
	m.routeMu.Lock()
	defer m.routeMu.Unlock()
	if value, ok := m.route.Load(key); ok {
		value.(*atomic.Uint64).Add(1)
		return key
	}
	if m.routeCount >= m.maxRoutes {
		key = "<overflow>"
	} else {
		m.routeCount++
	}
	value, _ := m.route.LoadOrStore(key, &atomic.Uint64{})
	value.(*atomic.Uint64).Add(1)
	return key
}

func (m *Metrics) recordRouteDuration(key string, code int, dur time.Duration) {
	value, _ := m.routeStats.LoadOrStore(key, &routeSeries{})
	class := code / 100
	if class < 1 || class > 5 {
		class = 0
	}
	stats := &value.(*routeSeries).classes[class]
	stats.requests.Add(1)
	stats.totalNS.Add(uint64(dur.Nanoseconds()))
	for i, bucket := range buckets {
		if dur <= bucket {
			stats.latency[i].Add(1)
			break
		}
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
	fmt.Fprint(&b, "# HELP fh_route_requests_total Requests by route and status class.\n# TYPE fh_route_requests_total counter\n")
	fmt.Fprint(&b, "# HELP fh_route_request_duration_seconds Request duration by route and status class.\n# TYPE fh_route_request_duration_seconds histogram\n")
	var keys []string
	m.routeStats.Range(func(k, _ any) bool { keys = append(keys, k.(string)); return true })
	sort.Strings(keys)
	for _, key := range keys {
		value, ok := m.routeStats.Load(key)
		if !ok {
			continue
		}
		method, route, found := strings.Cut(key, " ")
		if !found {
			method, route = "overflow", key
		}
		for class, stats := range &value.(*routeSeries).classes {
			count := stats.requests.Load()
			if count == 0 {
				continue
			}
			statusClass := "other"
			if class > 0 {
				statusClass = strconv.Itoa(class) + "xx"
			}
			labels := fmt.Sprintf("method=\"%s\",route=\"%s\",status_class=\"%s\"", escapeLabel(method), escapeLabel(route), statusClass)
			fmt.Fprintf(&b, "fh_route_requests_total{%s} %d\n", labels, count)
			var cumulative uint64
			for i, bucket := range buckets {
				cumulative += stats.latency[i].Load()
				fmt.Fprintf(&b, "fh_route_request_duration_seconds_bucket{%s,le=\"%.3f\"} %d\n", labels, bucket.Seconds(), cumulative)
			}
			fmt.Fprintf(&b, "fh_route_request_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", labels, count)
			fmt.Fprintf(&b, "fh_route_request_duration_seconds_sum{%s} %.9f\n", labels, float64(stats.totalNS.Load())/1e9)
			fmt.Fprintf(&b, "fh_route_request_duration_seconds_count{%s} %d\n", labels, count)
		}
	}
	return b.String()
}

func escapeLabel(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return strings.ReplaceAll(s, "\"", "\\\"")
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
