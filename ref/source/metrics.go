package source

import "time"

// MetricsCollector accumulates source metrics for one invocation
// and provides aggregate statistics. The SourceMetrics type is defined
// in the observer package to avoid import cycles.
type MetricsCollector struct {
	metrics []Metrics
}

// Metrics provides timing breakdown for any external data fetch.
// This is the source-local version; the observer package defines its own
// SourceMetrics that mirrors this structure for the Observer interface.
type Metrics struct {
	// Source identification
	SourceName string     // "users-db", "payment-api", "s3-assets"
	SourceKind SourceKind // database, api, cache, etc.
	Operation  string     // "select", "get", "list", "mget", "query"
	QueryHash  uint64     // stable fingerprint (no sensitive values)

	// Timing breakdown — all durations measured independently
	QueueWait  time.Duration // time waiting for concurrency budget
	ConnWait   time.Duration // time acquiring connection/session
	ExecTime   time.Duration // time executing at the source
	DecodeTime time.Duration // time decoding response
	TotalTime  time.Duration // wall clock from enter to exit

	// Volume
	ResultCount int64 // rows, items, objects returned
	ResultBytes int64 // bytes transferred from source

	// Optimisation signals
	CacheHit   bool   // was this served from cache?
	CacheLevel string // "L0", "L1", "L2", "" (empty = no cache)
	Batched    bool   // was this part of a batched call?
	BatchSize  int    // how many keys were in the batch?
	Coalesced  bool   // was this coalesced with another caller?
	Error      bool   // did the source call fail?
}

// NewMetricsCollector creates a new collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// Record adds a Metrics entry.
func (mc *MetricsCollector) Record(m Metrics) {
	mc.metrics = append(mc.metrics, m)
}

// All returns all recorded metrics.
func (mc *MetricsCollector) All() []Metrics {
	return mc.metrics
}

// Summary returns aggregate statistics across all recorded source operations.
func (mc *MetricsCollector) Summary() MetricsSummary {
	s := MetricsSummary{
		BySource: make(map[string]SourceSummary),
	}
	for _, m := range mc.metrics {
		s.TotalOps++
		s.TotalTime += m.TotalTime
		s.TotalExecTime += m.ExecTime
		s.TotalRows += m.ResultCount
		s.TotalBytes += m.ResultBytes
		if m.CacheHit {
			s.CacheHits++
		}
		if m.Batched {
			s.BatchedOps++
		}
		if m.Coalesced {
			s.CoalescedOps++
		}
		if m.Error {
			s.Errors++
		}

		ss := s.BySource[m.SourceName]
		ss.Ops++
		ss.TotalTime += m.TotalTime
		ss.ExecTime += m.ExecTime
		ss.Rows += m.ResultCount
		ss.Bytes += m.ResultBytes
		if m.CacheHit {
			ss.CacheHits++
		}
		if m.Error {
			ss.Errors++
		}
		s.BySource[m.SourceName] = ss
	}
	return s
}

// MetricsSummary provides aggregate statistics.
type MetricsSummary struct {
	TotalOps      int
	TotalTime     time.Duration
	TotalExecTime time.Duration
	TotalRows     int64
	TotalBytes    int64
	CacheHits     int
	BatchedOps    int
	CoalescedOps  int
	Errors        int
	BySource      map[string]SourceSummary
}

// SourceSummary provides per-source aggregate statistics.
type SourceSummary struct {
	Ops       int
	TotalTime time.Duration
	ExecTime  time.Duration
	Rows      int64
	Bytes     int64
	CacheHits int
	Errors    int
}
