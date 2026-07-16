package auditlog

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type Event string

const (
	EventAuthFailure    Event = "auth_failure"
	EventCSRFBlock      Event = "csrf_block"
	EventRateLimit      Event = "rate_limit"
	EventIPBlocked      Event = "ip_blocked"
	EventSuspicious     Event = "suspicious_request"
	EventConfigChange   Event = "config_change"
	EventPrivilegeEsc   Event = "privilege_escalation"
	EventDataAccess     Event = "data_access"
	EventInjection      Event = "injection_attempt"
	EventMITM           Event = "mitm_detected"
	EventDDoS           Event = "ddos_detected"
	EventReplayDetected Event = "replay_detected"
)

type AuditEntry struct {
	Timestamp   time.Time         `json:"timestamp"`
	Severity    Severity          `json:"severity"`
	Event       Event             `json:"event"`
	ClientIP    string            `json:"client_ip"`
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	StatusCode  int               `json:"status_code"`
	UserAgent   string            `json:"user_agent"`
	PrincipalID string            `json:"principal_id,omitempty"`
	RequestID   string            `json:"request_id,omitempty"`
	Details     map[string]string `json:"details,omitempty"`
	Duration    string            `json:"duration,omitempty"`
}

type Sink interface {
	Write(entry AuditEntry)
}

type Config struct {
	Sinks          []Sink
	MinSeverity    Severity
	Skip           func(fh.Ctx) bool
	CaptureHeaders []string
	Logger         fh.Logger
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	return func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}

		start := time.Now()
		err := c.Next()
		dur := time.Since(start)

		status := c.StatusCode()
		severity := classifyStatus(status)

		if severityRank(severity) < severityRank(cfg.MinSeverity) {
			return err
		}

		entry := AuditEntry{
			Timestamp:  start.UTC(),
			Severity:   severity,
			Event:      classifyEvent(status, c.Path()),
			ClientIP:   c.IP(),
			Method:     c.Method(),
			Path:       c.Path(),
			StatusCode: status,
			UserAgent:  c.Get("User-Agent"),
			RequestID:  c.Get(fh.HeaderRequestID),
			Duration:   dur.String(),
		}

		if principal, ok := fh.PrincipalFrom(c); ok {
			entry.PrincipalID = principal.ID
		}

		if len(cfg.CaptureHeaders) > 0 {
			details := make(map[string]string)
			for _, h := range cfg.CaptureHeaders {
				if v := c.Get(h); v != "" {
					details[h] = maskSensitive(v)
				}
			}
			if len(details) > 0 {
				entry.Details = details
			}
		}

		for _, sink := range cfg.Sinks {
			sink.Write(entry)
		}

		if cfg.Logger != nil && severityRank(severity) >= severityRank(SeverityWarning) {
			cfg.Logger.Warn("audit_event",
				"event", string(entry.Event),
				"severity", string(entry.Severity),
				"client_ip", entry.ClientIP,
				"method", entry.Method,
				"path", entry.Path,
				"status", entry.StatusCode,
			)
		}

		return err
	}
}

func normalize(cfg Config) Config {
	if cfg.MinSeverity == "" {
		cfg.MinSeverity = SeverityInfo
	}
	if cfg.Sinks == nil {
		cfg.Sinks = []Sink{&LogSink{}}
	}
	return cfg
}

func classifyStatus(code int) Severity {
	switch {
	case code >= 500:
		return SeverityCritical
	case code >= 400:
		return SeverityWarning
	default:
		return SeverityInfo
	}
}

func classifyEvent(code int, path string) Event {
	switch {
	case code == 401:
		return EventAuthFailure
	case code == 403:
		return EventPrivilegeEsc
	case code == 429:
		return EventRateLimit
	case code == 400 && containsInjection(path):
		return EventInjection
	case code == 400:
		return EventSuspicious
	case code >= 500:
		return EventSuspicious
	default:
		return EventDataAccess
	}
}

func containsInjection(path string) bool {
	lower := strings.ToLower(path)
	indicators := []string{"<script", "union select", "drop table", "../", "..%2f", "%00", "javascript:"}
	for _, ind := range indicators {
		if strings.Contains(lower, ind) {
			return true
		}
	}
	return false
}

func maskSensitive(v string) string {
	if len(v) > 20 {
		return v[:4] + strings.Repeat("*", len(v)-8) + v[len(v)-4:]
	}
	if len(v) > 8 {
		return v[:2] + strings.Repeat("*", len(v)-4) + v[len(v)-2:]
	}
	return strings.Repeat("*", len(v))
}

func severityRank(s Severity) int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}

type LogSink struct {
	mu sync.Mutex
}

func (s *LogSink) Write(entry AuditEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("[AUDIT] %s severity=%s event=%s method=%s path=%s status=%d ip=%s\n",
		entry.Timestamp.Format(time.RFC3339),
		entry.Severity,
		entry.Event,
		entry.Method,
		entry.Path,
		entry.StatusCode,
		entry.ClientIP,
	)
}

type BufferSink struct {
	mu      sync.Mutex
	entries []AuditEntry
	maxSize int
}

func NewBufferSink(maxSize int) *BufferSink {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &BufferSink{maxSize: maxSize}
}

func (s *BufferSink) Write(entry AuditEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
	if len(s.entries) > s.maxSize {
		s.entries = s.entries[len(s.entries)-s.maxSize:]
	}
}

func (s *BufferSink) Entries() []AuditEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AuditEntry, len(s.entries))
	copy(out, s.entries)
	return out
}

func (s *BufferSink) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = s.entries[:0]
}
