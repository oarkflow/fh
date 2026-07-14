package fh

import (
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

	// BurnRateWindow is the window for burn rate calculation.
	// Default: 5 minutes.
	BurnRateWindow time.Duration
}

// SLOSnapshot is a read-only snapshot of SLO state (safe to copy).
type SLOSnapshot struct {
	TotalRequests        int64
	FailedRequests       int64
	SuccessRequests      int64
	BurnRate             float64
	ErrorBudgetRemaining float64
	Compliant            bool
	LastUpdate           time.Time
	P50                  float64
	P95                  float64
	P99                  float64
}

// SLOTracker tracks SLO compliance across routes.
type SLOTracker struct {
	mu      sync.RWMutex
	routes  map[string]*routeSLO
	config  SLOTrackerConfig
	stopCh  chan struct{}
	once    sync.Once
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

type routeSLO struct {
	slo         SLO
	mu          sync.Mutex
	total       atomic.Int64
	failed      atomic.Int64
	success     atomic.Int64
	latencies   []float64
	maxSamples  int
	burnRate    float64
	budgetRem   float64
	compliant   bool
	lastUpdate  time.Time
	wasAlerting bool
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
		routes: make(map[string]*routeSLO),
		config: c,
		stopCh: make(chan struct{}),
	}

	go t.checkLoop()
	return t
}

// Register registers an SLO for a route.
func (t *SLOTracker) Register(route string, slo SLO) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if slo.BurnRateWindow <= 0 {
		slo.BurnRateWindow = 5 * time.Minute
	}
	if slo.ErrorBudget <= 0 {
		slo.ErrorBudget = slo.BurnRateWindow
	}

	t.routes[route] = &routeSLO{
		slo:        slo,
		latencies:  make([]float64, 0, 1024),
		maxSamples: t.config.MaxLatencySamples,
		budgetRem:  1.0,
		compliant:  true,
		lastUpdate: time.Now(),
	}
}

// RecordRequest records a completed request for SLO tracking.
func (t *SLOTracker) RecordRequest(route string, latency time.Duration, failed bool) {
	t.mu.RLock()
	rs, ok := t.routes[route]
	t.mu.RUnlock()
	if !ok {
		return
	}

	latMs := float64(latency.Microseconds()) / 1000.0

	rs.mu.Lock()
	if len(rs.latencies) >= rs.maxSamples {
		rs.latencies = rs.latencies[len(rs.latencies)/2:]
	}
	rs.latencies = append(rs.latencies, latMs)
	rs.mu.Unlock()

	rs.total.Add(1)
	if failed {
		rs.failed.Add(1)
	} else {
		rs.success.Add(1)
	}
}

// GetState returns the current SLO state for a route.
func (t *SLOTracker) GetState(route string) (SLOSnapshot, bool) {
	t.mu.RLock()
	rs, ok := t.routes[route]
	t.mu.RUnlock()
	if !ok {
		return SLOSnapshot{}, false
	}

	return rs.snapshot(), true
}

// IsCompliant reports whether a route is currently meeting its SLO.
func (t *SLOTracker) IsCompliant(route string) bool {
	t.mu.RLock()
	rs, ok := t.routes[route]
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
	rs, ok := t.routes[route]
	t.mu.RUnlock()
	if !ok {
		return 0
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.burnRate
}

// Snapshot returns a copy of all SLO states.
func (t *SLOTracker) Snapshot() map[string]SLOSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]SLOSnapshot, len(t.routes))
	for route, rs := range t.routes {
		result[route] = rs.snapshot()
	}
	return result
}

// Stop halts the background SLO checker.
func (t *SLOTracker) Stop() {
	t.once.Do(func() { close(t.stopCh) })
}

func (rs *routeSLO) snapshot() SLOSnapshot {
	rs.mu.Lock()
	s := SLOSnapshot{
		TotalRequests:        rs.total.Load(),
		FailedRequests:       rs.failed.Load(),
		SuccessRequests:      rs.success.Load(),
		BurnRate:             rs.burnRate,
		ErrorBudgetRemaining: rs.budgetRem,
		Compliant:            rs.compliant,
		LastUpdate:           rs.lastUpdate,
		P50:                  0,
		P95:                  0,
		P99:                  0,
	}

	// Calculate percentiles from a copy of latencies.
	samples := make([]float64, len(rs.latencies))
	copy(samples, rs.latencies)
	rs.mu.Unlock()

	if len(samples) > 0 {
		sortFloat64(samples)
		n := len(samples)
		s.P50 = samples[n*50/100]
		s.P95 = samples[n*95/100]
		idx := n * 99 / 100
		if idx >= n {
			idx = n - 1
		}
		s.P99 = samples[idx]
	}

	return s
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
	routes := make([]*routeSLO, 0, len(t.routes))
	for _, rs := range t.routes {
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

	// Calculate actual availability.
	actualAvailability := float64(total-failed) / float64(total)

	// Calculate burn rate.
	errorRate := float64(failed) / float64(total)
	allowedErrorRate := 1.0 - rs.slo.Availability

	var burnRate float64
	if allowedErrorRate > 0 {
		burnRate = errorRate / allowedErrorRate
	} else if errorRate > 0 {
		burnRate = 1e18
	}

	// Calculate error budget remaining.
	errorBudgetTotal := float64(total) * allowedErrorRate
	errorBudgetUsed := float64(failed)
	var budgetRemaining float64
	if errorBudgetTotal > 0 {
		budgetRemaining = 1.0 - (errorBudgetUsed / errorBudgetTotal)
		if budgetRemaining < 0 {
			budgetRemaining = 0
		}
	} else {
		budgetRemaining = 1.0
	}

	// Calculate percentiles.
	rs.mu.Lock()
	samples := make([]float64, len(rs.latencies))
	copy(samples, rs.latencies)
	rs.mu.Unlock()

	var p99 float64
	if len(samples) > 0 {
		sortFloat64(samples)
		n := len(samples)
		idx := n * 99 / 100
		if idx >= n {
			idx = n - 1
		}
		p99 = samples[idx]
	}

	// Update state.
	rs.mu.Lock()
	rs.burnRate = burnRate
	rs.budgetRem = budgetRemaining
	rs.compliant = actualAvailability >= rs.slo.Availability &&
		(rs.slo.P99Latency <= 0 || p99 <= float64(rs.slo.P99Latency.Microseconds())/1000.0) &&
		burnRate < t.config.AlertThreshold
	rs.lastUpdate = now

	// Fire alerts.
	isAlerting := burnRate >= t.config.AlertThreshold || !rs.compliant
	wasAlerting := rs.wasAlerting
	rs.wasAlerting = isAlerting
	rs.mu.Unlock()

	if isAlerting && !wasAlerting && t.config.OnAlert != nil {
		snap := rs.snapshot()
		t.config.OnAlert("", snap)
	} else if !isAlerting && wasAlerting && t.config.OnRecovery != nil {
		snap := rs.snapshot()
		t.config.OnRecovery("", snap)
	}
}

func sortFloat64(b []float64) {
	for i := 1; i < len(b); i++ {
		for j := i; j > 0 && b[j] < b[j-1]; j-- {
			b[j], b[j-1] = b[j-1], b[j]
		}
	}
}

// SLOMiddleware creates middleware that tracks SLO compliance for a route.
func SLOMiddleware(tracker *SLOTracker, route string) HandlerFunc {
	return func(c Ctx) error {
		start := time.Now()
		err := c.Next()
		latency := time.Since(start)
		failed := err != nil || c.StatusCode() >= 500
		tracker.RecordRequest(route, latency, failed)
		return err
	}
}
