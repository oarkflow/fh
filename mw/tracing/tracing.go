package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

const Traceparent = "Traceparent"

type Config struct {
	LocalKey       string
	ResponseHeader bool
	TrustIncoming  bool
}

type Span struct {
	TraceID   string        `json:"trace_id"`
	SpanID    string        `json:"span_id"`
	ParentID  string        `json:"parent_id,omitempty"`
	StartedAt time.Time     `json:"started_at"`
	EndedAt   time.Time     `json:"ended_at,omitempty"`
	Duration  time.Duration `json:"duration,omitempty"`
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.LocalKey == "" {
		cfg.LocalKey = "trace"
	}
	cfg.ResponseHeader = true
	return func(c fh.Ctx) error {
		sp := Span{StartedAt: time.Now().UTC()}
		if cfg.TrustIncoming {
			sp.TraceID, sp.ParentID = parse(c.Get(Traceparent))
		}
		if sp.TraceID == "" {
			sp.TraceID = randHex(16)
		}
		sp.SpanID = randHex(8)
		c.Locals(cfg.LocalKey, sp)
		if cfg.ResponseHeader {
			c.Set(Traceparent, "00-"+sp.TraceID+"-"+sp.SpanID+"-01")
			c.Set("X-Trace-ID", sp.TraceID)
		}
		defer func() {
			sp.EndedAt = time.Now().UTC()
			sp.Duration = sp.EndedAt.Sub(sp.StartedAt)
			c.Locals(cfg.LocalKey, sp)
		}()
		return c.Next()
	}
}
func parse(v string) (string, string) {
	parts := strings.Split(v, "-")
	if len(parts) >= 4 && len(parts[1]) == 32 && len(parts[2]) == 16 {
		return parts[1], parts[2]
	}
	return "", ""
}
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return strings.Repeat("0", n*2)
	}
	return hex.EncodeToString(b)
}
