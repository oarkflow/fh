package fh

import "sync/atomic"

// ServerMetrics contains real-time server runtime metrics.
type ServerMetrics struct {
	ActiveConns   int64  `json:"active_conns"`
	TotalRequests uint64 `json:"total_requests"`
	TotalErrors   uint64 `json:"total_errors"`
	Status2xx     uint64 `json:"status_2xx"`
	Status3xx     uint64 `json:"status_3xx"`
	Status4xx     uint64 `json:"status_4xx"`
	Status5xx     uint64 `json:"status_5xx"`
}

type metricsTracker struct {
	activeConns   atomic.Int64
	totalRequests atomic.Uint64
	totalErrors   atomic.Uint64
	status2xx     atomic.Uint64
	status3xx     atomic.Uint64
	status4xx     atomic.Uint64
	status5xx     atomic.Uint64
}

func (m *metricsTracker) recordRequest(status int) {
	m.totalRequests.Add(1)
	switch {
	case status >= 200 && status < 300:
		m.status2xx.Add(1)
	case status >= 300 && status < 400:
		m.status3xx.Add(1)
	case status >= 400 && status < 500:
		m.status4xx.Add(1)
		m.totalErrors.Add(1)
	case status >= 500:
		m.status5xx.Add(1)
		m.totalErrors.Add(1)
	}
}

func (m *metricsTracker) snapshot() ServerMetrics {
	return ServerMetrics{
		ActiveConns:   m.activeConns.Load(),
		TotalRequests: m.totalRequests.Load(),
		TotalErrors:   m.totalErrors.Load(),
		Status2xx:     m.status2xx.Load(),
		Status3xx:     m.status3xx.Load(),
		Status4xx:     m.status4xx.Load(),
		Status5xx:     m.status5xx.Load(),
	}
}
