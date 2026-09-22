package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/graph"
)

// Metrics is a REF Observer that also serves Prometheus text.
//
// It is attached as an Async observer, which means a slow scrape or a lock here
// cannot slow an execution down — the scheduler hands events to a bounded channel
// and moves on. Observation must never be able to break the thing it observes, and
// the tier is how that is expressed rather than a comment saying "be careful".
type Metrics struct {
	mu sync.Mutex

	nodeStarted  map[string]int64
	nodeFailed   map[string]int64
	decisions    map[string]int64
	effects      map[string]int64
	effectFailed map[string]int64
	executions   map[string]int64
	failures     map[string]int64
	business     map[string]float64

	latency map[string]*histogram
}

// histogram is a small fixed-bucket latency histogram. Prometheus-compatible
// buckets, cumulative as the exposition format requires.
type histogram struct {
	bounds []float64
	counts []int64
	sum    float64
	total  int64
}

var latencyBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000}

func newHistogram() *histogram {
	return &histogram{bounds: latencyBuckets, counts: make([]int64, len(latencyBuckets)+1)}
}

func (h *histogram) observe(ms float64) {
	h.sum += ms
	h.total++
	for index, bound := range h.bounds {
		if ms <= bound {
			h.counts[index]++
			return
		}
	}
	h.counts[len(h.bounds)]++
}

func NewMetrics() *Metrics {
	return &Metrics{
		nodeStarted:  map[string]int64{},
		nodeFailed:   map[string]int64{},
		decisions:    map[string]int64{},
		effects:      map[string]int64{},
		effectFailed: map[string]int64{},
		executions:   map[string]int64{},
		failures:     map[string]int64{},
		business:     map[string]float64{},
		latency:      map[string]*histogram{},
	}
}

// AddBusiness records an application counter. Effects use it, which is why
// telemetry is a fire-and-forget effect rather than a call inside the intent: it
// belongs to the plan, so it is visible in the plan.
func (m *Metrics) AddBusiness(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.business[name] += value
}

// --- observer.Observer ------------------------------------------------------

func (m *Metrics) NodeStarted(info graph.NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeStarted[info.Name]++
}

func (m *Metrics) NodeFinished(info graph.NodeInfo, err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeFailed[info.Name]++
}

// RecordDecision counts one policy verdict. Capabilities call it directly, because
// the kernel does not emit decision events — see AuditObserver's documentation.
func (m *Metrics) RecordDecision(policy string, allow bool) {
	outcome := "deny"
	if allow {
		outcome = "allow"
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decisions[policy+"|"+outcome]++
}

// DecisionMade implements the Observer interface, for the day the kernel emits it.
func (m *Metrics) DecisionMade(policy string, verdict uint8, _ string) {
	m.RecordDecision(policy, execution.Verdict(verdict) != execution.VerdictDeny)
}

func (m *Metrics) EffectCommitted(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.effectFailed[name]++
		return
	}
	m.effects[name]++
}

func (m *Metrics) ExecutionFinished(intentName string, durationMs float64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executions[intentName]++
	if err != nil {
		m.failures[intentName]++
	}
	hist, ok := m.latency[intentName]
	if !ok {
		hist = newHistogram()
		m.latency[intentName] = hist
	}
	hist.observe(durationMs)
}

// --- exposition -------------------------------------------------------------

// Prometheus renders the current values in the text exposition format.
func (m *Metrics) Prometheus() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out strings.Builder
	counter := func(name, help string, values map[string]int64, label string) {
		if len(values) == 0 {
			return
		}
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		for _, key := range sortedKeys(values) {
			fmt.Fprintf(&out, "%s{%s=%q} %d\n", name, label, key, values[key])
		}
	}

	counter("refapp_node_started_total", "Nodes started, by node.", m.nodeStarted, "node")
	counter("refapp_node_failed_total", "Nodes that returned an error, by node.", m.nodeFailed, "node")
	counter("refapp_effect_committed_total", "Effects committed, by effect.", m.effects, "effect")
	counter("refapp_effect_failed_total", "Effects that failed, by effect.", m.effectFailed, "effect")
	counter("refapp_executions_total", "Intent executions, by intent.", m.executions, "intent")
	counter("refapp_execution_failures_total", "Failed intent executions, by intent.", m.failures, "intent")

	if len(m.decisions) > 0 {
		fmt.Fprintf(&out, "# HELP refapp_decisions_total Policy decisions, by policy and outcome.\n# TYPE refapp_decisions_total counter\n")
		for _, key := range sortedKeys(m.decisions) {
			policy, outcome, _ := strings.Cut(key, "|")
			fmt.Fprintf(&out, "refapp_decisions_total{policy=%q,outcome=%q} %d\n", policy, outcome, m.decisions[key])
		}
	}

	if len(m.business) > 0 {
		fmt.Fprintf(&out, "# HELP refapp_business_total Application counters emitted by effects.\n# TYPE refapp_business_total counter\n")
		names := make([]string, 0, len(m.business))
		for name := range m.business {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(&out, "refapp_business_total{name=%q} %g\n", name, m.business[name])
		}
	}

	if len(m.latency) > 0 {
		fmt.Fprintf(&out, "# HELP refapp_execution_duration_ms Intent execution duration in milliseconds.\n# TYPE refapp_execution_duration_ms histogram\n")
		names := make([]string, 0, len(m.latency))
		for name := range m.latency {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			hist := m.latency[name]
			cumulative := int64(0)
			for index, bound := range hist.bounds {
				cumulative += hist.counts[index]
				fmt.Fprintf(&out, "refapp_execution_duration_ms_bucket{intent=%q,le=\"%g\"} %d\n", name, bound, cumulative)
			}
			cumulative += hist.counts[len(hist.bounds)]
			fmt.Fprintf(&out, "refapp_execution_duration_ms_bucket{intent=%q,le=\"+Inf\"} %d\n", name, cumulative)
			fmt.Fprintf(&out, "refapp_execution_duration_ms_sum{intent=%q} %g\n", name, hist.sum)
			fmt.Fprintf(&out, "refapp_execution_duration_ms_count{intent=%q} %d\n", name, hist.total)
		}
	}
	return out.String()
}

// Snapshot is the same data as JSON, for a human looking at it in a terminal.
func (m *Metrics) Snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()

	latency := map[string]any{}
	for name, hist := range m.latency {
		mean := 0.0
		if hist.total > 0 {
			mean = hist.sum / float64(hist.total)
		}
		latency[name] = map[string]any{"count": hist.total, "mean_ms": mean}
	}
	return map[string]any{
		"executions": copyCounters(m.executions),
		"failures":   copyCounters(m.failures),
		"decisions":  copyCounters(m.decisions),
		"effects":    copyCounters(m.effects),
		"business":   copyFloats(m.business),
		"latency":    latency,
		"as_of":      time.Now().UTC().Format(time.RFC3339),
	}
}

func sortedKeys(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func copyCounters(values map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func copyFloats(values map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
