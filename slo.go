package fh

import (
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SLO defines a Service Level Objective for a route.
type SLO struct {
	// Availability is the target availability (0.0-1.0). Example: 0.9999 = 99.99%.
	Availability float64

	// P99Latency is the target 99th percentile latency.
	P99Latency time.Duration

	// P95Latency is the target 95th percentile latency.
	P95Latency time.Duration

	// P50Latency is the target 50th percentile latency (median).
	P50Latency time.Duration

	// ErrorBudget is the total error budget window.
	ErrorBudget time.Duration

	// BurnRateWindow is the sliding window for burn rate calculation.
	// Default: 5 minutes.
	BurnRateWindow time.Duration
}

// SLOSnapshot is a read-only snapshot of SLO state (safe to copy).
type SLOSnapshot struct {
	Route                string    `json:"route"`
	TotalRequests        int64     `json:"total_requests"`
	FailedRequests       int64     `json:"failed_requests"`
	SuccessRequests      int64     `json:"success_requests"`
	WindowRequests       int64     `json:"window_requests"`
	WindowFailed         int64     `json:"window_failed"`
	BurnRate             float64   `json:"burn_rate"`
	ErrorBudgetRemaining float64   `json:"error_budget_remaining"`
	Compliant            bool      `json:"compliant"`
	LastUpdate           time.Time `json:"last_update"`
	P50                  float64   `json:"p50_ms"`
	P95                  float64   `json:"p95_ms"`
	P99                  float64   `json:"p99_ms"`
}

// SLOTrackerConfig configures the SLO tracker.
type SLOTrackerConfig struct {
	// CheckInterval is how often to recalculate SLO state. Default: 10 seconds.
	CheckInterval time.Duration

	// AlertThreshold is the burn rate threshold that triggers an alert. Default: 2.0.
	AlertThreshold float64

	// OnAlert is called when an SLO violation is detected.
	OnAlert func(route string, state SLOSnapshot)

	// OnRecovery is called when an SLO recovers after a violation.
	OnRecovery func(route string, state SLOSnapshot)

	// MaxLatencySamples is the maximum number of latency samples per route. Default: 10000.
	MaxLatencySamples int
}

// sloBucketCount is the number of time buckets used for windowed burn rate.
const sloBucketCount = 60

// sloMatchCacheLimit bounds the path→route match cache; the cache is reset
// when it grows past this size to avoid unbounded growth from high-cardinality
// request paths.
const sloMatchCacheLimit = 8192

// sloBucket is one time slice of windowed request counters.
type sloBucket struct {
	stamp  int64 // bucket epoch (unix nanos / bucket duration); 0 = empty
	total  int64
	failed int64
}

type routeSLO struct {
	name    string
	slo     SLO
	pattern *RoutePattern  // non-nil for :param / * patterns
	regex   *regexp.Regexp // non-nil for ^regex patterns

	total   atomic.Int64
	failed  atomic.Int64
	success atomic.Int64

	mu          sync.Mutex
	latencies   []float64
	maxSamples  int
	bucketDur   time.Duration
	buckets     [sloBucketCount]sloBucket
	burnRate    float64
	budgetRem   float64
	compliant   bool
	lastUpdate  time.Time
	wasAlerting bool
}

// SLOTracker tracks SLO compliance across routes. Routes are registered with
// router-style patterns and matched by the Handler middleware.
type SLOTracker struct {
	mu       sync.RWMutex
	static   map[string]*routeSLO // exact-path routes for O(1) lookup
	dynamic  []*routeSLO          // :param / * / regex routes, most specific first
	byName   map[string]*routeSLO // all routes keyed by registered pattern
	matchMu  sync.RWMutex
	matchLRU map[string]*routeSLO // concrete path → resolved route (nil-able values not stored)
	config   SLOTrackerConfig
	stopCh   chan struct{}
	once     sync.Once
}

// NewSLOTracker creates a new SLO tracker.
func NewSLOTracker(cfg ...SLOTrackerConfig) *SLOTracker {
	c := SLOTrackerConfig{
		CheckInterval:     10 * time.Second,
		AlertThreshold:    2.0,
		MaxLatencySamples: 10000,
	}
	if len(cfg) > 0 {
		if cfg[0].CheckInterval > 0 {
			c.CheckInterval = cfg[0].CheckInterval
		}
		if cfg[0].AlertThreshold > 0 {
			c.AlertThreshold = cfg[0].AlertThreshold
		}
		if cfg[0].OnAlert != nil {
			c.OnAlert = cfg[0].OnAlert
		}
		if cfg[0].OnRecovery != nil {
			c.OnRecovery = cfg[0].OnRecovery
		}
		if cfg[0].MaxLatencySamples > 0 {
			c.MaxLatencySamples = cfg[0].MaxLatencySamples
		}
	}

	t := &SLOTracker{
		static:   make(map[string]*routeSLO),
		byName:   make(map[string]*routeSLO),
		matchLRU: make(map[string]*routeSLO),
		config:   c,
		stopCh:   make(chan struct{}),
	}

	go t.checkLoop()
	return t
}

// Register registers an SLO for a route pattern. Supported pattern forms:
//
//   - static paths:        /api/users
//   - named parameters:    /api/users/:id, /api/users/:id/posts/:postID
//   - terminal wildcards:  /files/*, /files/*filepath
//   - regular expressions: any pattern starting with "^", e.g. ^/api/v[0-9]+/users$
//
// Non-regex patterns use the router's own matching semantics, so an SLO
// pattern matches exactly the paths the equivalent route would serve.
// Registering the same pattern again replaces the previous SLO.
// Register panics if a regex pattern does not compile (configuration error).
func (t *SLOTracker) Register(pattern string, slo SLO) {
	if slo.BurnRateWindow <= 0 {
		slo.BurnRateWindow = 5 * time.Minute
	}
	if slo.ErrorBudget <= 0 {
		slo.ErrorBudget = slo.BurnRateWindow
	}

	rs := &routeSLO{
		name:       pattern,
		slo:        slo,
		latencies:  make([]float64, 0, 1024),
		maxSamples: t.config.MaxLatencySamples,
		bucketDur:  slo.BurnRateWindow / sloBucketCount,
		budgetRem:  1.0,
		compliant:  true,
		lastUpdate: time.Now(),
	}
	if rs.bucketDur <= 0 {
		rs.bucketDur = time.Second
	}

	switch {
	case strings.HasPrefix(pattern, "^"):
		rs.regex = regexp.MustCompile(pattern)
	case strings.ContainsAny(pattern, ":*"):
		rs.pattern = CompileRoutePattern(pattern)
	}

	t.mu.Lock()
	if old, ok := t.byName[pattern]; ok {
		t.removeLocked(old)
	}
	t.byName[pattern] = rs
	if rs.pattern == nil && rs.regex == nil {
		t.static[pattern] = rs
	} else {
		t.dynamic = append(t.dynamic, rs)
		sortDynamicSLOs(t.dynamic)
	}
	t.mu.Unlock()

	t.resetMatchCache()
}

// Unregister removes a route's SLO. It reports whether the pattern was registered.
func (t *SLOTracker) Unregister(pattern string) bool {
	t.mu.Lock()
	rs, ok := t.byName[pattern]
	if ok {
		t.removeLocked(rs)
	}
	t.mu.Unlock()
	if ok {
		t.resetMatchCache()
	}
	return ok
}

// removeLocked removes rs from all indexes. Caller must hold t.mu.
func (t *SLOTracker) removeLocked(rs *routeSLO) {
	delete(t.byName, rs.name)
	delete(t.static, rs.name)
	for i, d := range t.dynamic {
		if d == rs {
			t.dynamic = append(t.dynamic[:i], t.dynamic[i+1:]...)
			break
		}
	}
}

// sortDynamicSLOs orders dynamic routes most-specific first: route patterns
// before regexes; among route patterns, more static segments first, wildcards
// last; ties broken by longer (more segments) pattern first.
func sortDynamicSLOs(routes []*routeSLO) {
	sort.SliceStable(routes, func(i, j int) bool {
		a, b := routes[i], routes[j]
		aRegex, bRegex := a.regex != nil, b.regex != nil
		if aRegex != bRegex {
			return !aRegex // patterns before regexes
		}
		if aRegex {
			return false // regexes keep registration order
		}
		aWild, bWild := strings.Contains(a.name, "*"), strings.Contains(b.name, "*")
		if aWild != bWild {
			return !aWild // wildcards last
		}
		aStatic, bStatic := staticSegmentCount(a.name), staticSegmentCount(b.name)
		if aStatic != bStatic {
			return aStatic > bStatic
		}
		return strings.Count(a.name, "/") > strings.Count(b.name, "/")
	})
}

func staticSegmentCount(pattern string) int {
	n := 0
	for _, seg := range strings.Split(pattern, "/") {
		if seg != "" && seg[0] != ':' && seg[0] != '*' {
			n++
		}
	}
	return n
}

// Match resolves a concrete request path to its registered SLO route.
// Precedence: exact static match, then dynamic patterns most-specific first,
// then regex patterns in registration order.
func (t *SLOTracker) Match(path string) (route string, ok bool) {
	rs := t.match(path)
	if rs == nil {
		return "", false
	}
	return rs.name, true
}

func (t *SLOTracker) match(path string) *routeSLO {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}

	t.matchMu.RLock()
	rs, cached := t.matchLRU[path]
	t.matchMu.RUnlock()
	if cached {
		return rs
	}

	t.mu.RLock()
	rs = t.static[path]
	if rs == nil {
		for _, d := range t.dynamic {
			if d.regex != nil {
				if d.regex.MatchString(path) {
					rs = d
					break
				}
			} else if d.pattern.Match(path, nil) {
				rs = d
				break
			}
		}
	}
	t.mu.RUnlock()

	t.matchMu.Lock()
	if len(t.matchLRU) >= sloMatchCacheLimit {
		t.matchLRU = make(map[string]*routeSLO, sloMatchCacheLimit/4)
	}
	t.matchLRU[path] = rs
	t.matchMu.Unlock()
	return rs
}

func (t *SLOTracker) resetMatchCache() {
	t.matchMu.Lock()
	t.matchLRU = make(map[string]*routeSLO)
	t.matchMu.Unlock()
}

// Handler returns middleware that tracks SLO compliance for every request
// whose path matches a registered pattern. Unmatched requests pass through
// with no overhead beyond the (cached) match lookup.
//
//	tracker.Register("/api/users/:id", fh.SLO{...})
//	app.Use(tracker.Handler())
func (t *SLOTracker) Handler() HandlerFunc {
	return func(c Ctx) error {
		rs := t.match(c.Path())
		if rs == nil {
			return c.Next()
		}
		start := time.Now()
		err := c.Next()
		failed := err != nil || c.StatusCode() >= 500
		rs.record(time.Since(start), failed)
		return err
	}
}

// RecordRequest records a completed request for SLO tracking against an
// exact registered pattern. Prefer Handler for automatic path matching.
func (t *SLOTracker) RecordRequest(route string, latency time.Duration, failed bool) {
	t.mu.RLock()
	rs, ok := t.byName[route]
	t.mu.RUnlock()
	if !ok {
		return
	}
	rs.record(latency, failed)
}

func (rs *routeSLO) record(latency time.Duration, failed bool) {
	latMs := float64(latency.Microseconds()) / 1000.0
	epoch := time.Now().UnixNano() / int64(rs.bucketDur)
	idx := epoch % sloBucketCount

	rs.mu.Lock()
	if len(rs.latencies) >= rs.maxSamples {
		rs.latencies = rs.latencies[len(rs.latencies)/2:]
	}
	rs.latencies = append(rs.latencies, latMs)

	b := &rs.buckets[idx]
	if b.stamp != epoch {
		b.stamp = epoch
		b.total = 0
		b.failed = 0
	}
	b.total++
	if failed {
		b.failed++
	}
	rs.mu.Unlock()

	rs.total.Add(1)
	if failed {
		rs.failed.Add(1)
	} else {
		rs.success.Add(1)
	}
}

// windowCounts sums request counters over the burn rate window.
// Caller must hold rs.mu.
func (rs *routeSLO) windowCounts(now time.Time) (total, failed int64) {
	epoch := now.UnixNano() / int64(rs.bucketDur)
	oldest := epoch - sloBucketCount + 1
	for i := range rs.buckets {
		b := &rs.buckets[i]
		if b.stamp >= oldest && b.stamp <= epoch {
			total += b.total
			failed += b.failed
		}
	}
	return total, failed
}

// GetState returns the current SLO state for a route pattern.
func (t *SLOTracker) GetState(route string) (SLOSnapshot, bool) {
	t.mu.RLock()
	rs, ok := t.byName[route]
	t.mu.RUnlock()
	if !ok {
		return SLOSnapshot{}, false
	}
	return rs.snapshot(), true
}

// IsCompliant reports whether a route is currently meeting its SLO.
// Unregistered routes are considered compliant.
func (t *SLOTracker) IsCompliant(route string) bool {
	t.mu.RLock()
	rs, ok := t.byName[route]
	t.mu.RUnlock()
	if !ok {
		return true
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.compliant
}

// BurnRate returns the current error burn rate for a route.
func (t *SLOTracker) BurnRate(route string) float64 {
	t.mu.RLock()
	rs, ok := t.byName[route]
	t.mu.RUnlock()
	if !ok {
		return 0
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.burnRate
}

// Snapshot returns a copy of all SLO states keyed by registered pattern.
func (t *SLOTracker) Snapshot() map[string]SLOSnapshot {
	t.mu.RLock()
	routes := make([]*routeSLO, 0, len(t.byName))
	for _, rs := range t.byName {
		routes = append(routes, rs)
	}
	t.mu.RUnlock()

	result := make(map[string]SLOSnapshot, len(routes))
	for _, rs := range routes {
		result[rs.name] = rs.snapshot()
	}
	return result
}

// Stop halts the background SLO checker.
func (t *SLOTracker) Stop() {
	t.once.Do(func() { close(t.stopCh) })
}

func (rs *routeSLO) snapshot() SLOSnapshot {
	now := time.Now()
	rs.mu.Lock()
	winTotal, winFailed := rs.windowCounts(now)
	s := SLOSnapshot{
		Route:                rs.name,
		TotalRequests:        rs.total.Load(),
		FailedRequests:       rs.failed.Load(),
		SuccessRequests:      rs.success.Load(),
		WindowRequests:       winTotal,
		WindowFailed:         winFailed,
		BurnRate:             rs.burnRate,
		ErrorBudgetRemaining: rs.budgetRem,
		Compliant:            rs.compliant,
		LastUpdate:           rs.lastUpdate,
	}
	samples := make([]float64, len(rs.latencies))
	copy(samples, rs.latencies)
	rs.mu.Unlock()

	s.P50, s.P95, s.P99 = percentiles(samples)
	return s
}

// percentiles sorts samples in place and returns p50, p95, and p99 values.
func percentiles(samples []float64) (p50, p95, p99 float64) {
	n := len(samples)
	if n == 0 {
		return 0, 0, 0
	}
	sort.Float64s(samples)
	at := func(pct int) float64 {
		idx := n * pct / 100
		if idx >= n {
			idx = n - 1
		}
		return samples[idx]
	}
	return at(50), at(95), at(99)
}

func (t *SLOTracker) checkLoop() {
	ticker := time.NewTicker(t.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.checkAll()
		}
	}
}

func (t *SLOTracker) checkAll() {
	t.mu.RLock()
	routes := make([]*routeSLO, 0, len(t.byName))
	for _, rs := range t.byName {
		routes = append(routes, rs)
	}
	t.mu.RUnlock()

	now := time.Now()
	for _, rs := range routes {
		t.checkSLO(rs, now)
	}
}

func (t *SLOTracker) checkSLO(rs *routeSLO, now time.Time) {
	total := rs.total.Load()
	failed := rs.failed.Load()
	if total == 0 {
		return
	}

	rs.mu.Lock()
	winTotal, winFailed := rs.windowCounts(now)
	samples := make([]float64, len(rs.latencies))
	copy(samples, rs.latencies)
	rs.mu.Unlock()

	// Overall availability drives the error budget; the sliding window drives
	// the burn rate so old traffic does not mask (or prolong) a violation.
	actualAvailability := float64(total-failed) / float64(total)
	allowedErrorRate := 1.0 - rs.slo.Availability

	var windowErrorRate float64
	if winTotal > 0 {
		windowErrorRate = float64(winFailed) / float64(winTotal)
	}

	var burnRate float64
	if allowedErrorRate > 0 {
		burnRate = windowErrorRate / allowedErrorRate
	} else if windowErrorRate > 0 {
		burnRate = 1e18
	}

	errorBudgetTotal := float64(total) * allowedErrorRate
	budgetRemaining := 1.0
	if errorBudgetTotal > 0 {
		budgetRemaining = 1.0 - float64(failed)/errorBudgetTotal
		if budgetRemaining < 0 {
			budgetRemaining = 0
		}
	}

	p50, p95, p99 := percentiles(samples)
	latencyOK := meetsLatencyTarget(rs.slo.P50Latency, p50) &&
		meetsLatencyTarget(rs.slo.P95Latency, p95) &&
		meetsLatencyTarget(rs.slo.P99Latency, p99)

	rs.mu.Lock()
	rs.burnRate = burnRate
	rs.budgetRem = budgetRemaining
	rs.compliant = actualAvailability >= rs.slo.Availability &&
		latencyOK &&
		burnRate < t.config.AlertThreshold
	rs.lastUpdate = now

	isAlerting := !rs.compliant
	wasAlerting := rs.wasAlerting
	rs.wasAlerting = isAlerting
	rs.mu.Unlock()

	if isAlerting && !wasAlerting && t.config.OnAlert != nil {
		t.config.OnAlert(rs.name, rs.snapshot())
	} else if !isAlerting && wasAlerting && t.config.OnRecovery != nil {
		t.config.OnRecovery(rs.name, rs.snapshot())
	}
}

// meetsLatencyTarget reports whether an observed percentile (ms) meets its
// target; a zero target means the percentile is not part of the SLO.
func meetsLatencyTarget(target time.Duration, observedMs float64) bool {
	return target <= 0 || observedMs <= float64(target.Microseconds())/1000.0
}
