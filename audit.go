package fh

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AuditConfig controls compliance-grade business/security audit logging.
type AuditConfig struct {
	Enabled   bool          `json:"enabled"`
	FilePath  string        `json:"file_path,omitempty"`
	Sink      AuditSink     `json:"-"`
	Redact    bool          `json:"redact"`
	Retention time.Duration `json:"retention,omitempty"`
}

type AuditEvent struct {
	ID            string         `json:"id"`
	Time          time.Time      `json:"time"`
	RequestID     string         `json:"request_id,omitempty"`
	CorrelationID string         `json:"correlation_id,omitempty"`
	TenantID      string         `json:"tenant_id,omitempty"`
	ActorID       string         `json:"actor_id,omitempty"`
	ActorType     string         `json:"actor_type,omitempty"`
	Action        string         `json:"action"`
	Resource      string         `json:"resource,omitempty"`
	ResourceID    string         `json:"resource_id,omitempty"`
	Result        string         `json:"result,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	Method        string         `json:"method,omitempty"`
	Path          string         `json:"path,omitempty"`
	IP            string         `json:"ip,omitempty"`
	DataClass     string         `json:"data_class,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type AuditSink interface {
	WriteAudit(context.Context, AuditEvent) error
}

type AuditSinkCloser interface {
	AuditSink
	Close() error
}

type FileAuditSink struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func OpenFileAuditSink(path string) (*FileAuditSink, error) {
	if path == "" {
		path = filepath.Join(".fh-reliability", "audit.jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileAuditSink{f: f, path: path}, nil
}
func (s *FileAuditSink) WriteAudit(ctx context.Context, e AuditEvent) error {
	if s == nil || s.f == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.ID == "" {
		e.ID = "aud_" + stringsTrimReq(newRequestID())
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err = s.f.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.f.Sync()
}
func (s *FileAuditSink) Close() error {
	if s == nil || s.f == nil {
		return nil
	}
	return s.f.Close()
}
func (s *FileAuditSink) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func stringsTrimReq(s string) string {
	if len(s) > 4 && s[:4] == "req_" {
		return s[4:]
	}
	return s
}

type AuditRecorder struct{ c *DefaultCtx }

func (c *DefaultCtx) Audit() AuditRecorder { return AuditRecorder{c: c} }

func (r AuditRecorder) Record(action, resource, resourceID string, meta ...map[string]any) error {
	if r.c == nil || r.c.server == nil {
		return nil
	}
	e := AuditEvent{Action: action, Resource: resource, ResourceID: resourceID, Result: "success"}
	if len(meta) > 0 {
		e.Metadata = meta[0]
	}
	return r.c.server.WriteAudit(r.c.Context(), enrichAuditFromCtx(r.c, e))
}

func (a *App) WriteAudit(ctx context.Context, e AuditEvent) error {
	if a == nil || a.audit == nil {
		return nil
	}
	if a.cfg.Audit.Redact || a.cfg.Redaction.Enabled {
		e = a.redactAudit(e)
	}
	return a.audit.WriteAudit(ctx, e)
}

func enrichAuditFromCtx(c *DefaultCtx, e AuditEvent) AuditEvent {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	if e.RequestID == "" {
		if v, _ := c.Locals("request_id").(string); v != "" {
			e.RequestID = v
		} else {
			e.RequestID = c.Get(HeaderRequestID)
		}
	}
	if e.CorrelationID == "" {
		e.CorrelationID = c.Get("X-Correlation-ID")
	}
	if e.Method == "" {
		e.Method = c.Method()
	}
	if e.Path == "" {
		e.Path = c.Path()
	}
	if e.IP == "" {
		e.IP = safeCtxIP(c)
	}
	if p, ok := PrincipalFrom(c); ok {
		if e.ActorID == "" {
			e.ActorID = p.ID
		}
		if e.ActorType == "" {
			e.ActorType = p.Type
		}
		if e.TenantID == "" {
			e.TenantID = p.TenantID
		}
	}
	if e.TenantID == "" {
		e.TenantID = TenantID(c)
	}
	if dp, ok := c.Locals("fh.data_policy").(DataPolicy); ok {
		e.DataClass = dp.Sensitivity
	}
	return e
}

func (a *App) redactAudit(e AuditEvent) AuditEvent {
	red := NewRedactor(a.cfg.Redaction)
	e.Reason = red.RedactString(e.Reason)
	if e.Metadata != nil {
		e.Metadata = red.RedactMap(e.Metadata)
	}
	return e
}

// Ledger records a business operation with before/after hashes for compliance evidence.
type LedgerEntry struct {
	ID, TenantID, ActorID, Action, Resource, ResourceID, Decision, BeforeHash, AfterHash, RequestID string
	CreatedAt                                                                                       time.Time
}

func (c *DefaultCtx) Ledger(action, resource, resourceID string, before, after []byte) error {
	return c.Audit().Record(action, resource, resourceID, map[string]any{"before_hash": hashBody(before), "after_hash": hashBody(after), "ledger": true})
}

func safeCtxIP(c *DefaultCtx) (ip string) {
	defer func() {
		if recover() != nil {
			ip = ""
		}
	}()
	if c == nil {
		return ""
	}
	return c.IP()
}
