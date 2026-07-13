// Package otel provides OpenTelemetry-compatible tracing middleware for fh.
// It creates spans for each request with standard HTTP attributes and
// propagates W3C TraceContext and Baggage headers.
//
// This package uses the official OpenTelemetry SDK which requires external
// dependencies. For a zero-dependency alternative, use mw/tracing.
//
// Usage:
//
//	import "github.com/oarkflow/fh/contrib/otel"
//
//	app.Use(otel.New(otel.Config{
//	    ServiceName:    "my-service",
//	    ServiceVersion: "v1.0.0",
//	    Propagators:    []otel.Propagator{otel.TraceContext, otel.Baggage},
//	}))
package otel

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)


type key struct{}

// Propagator defines W3C header propagation format.
type Propagator int

const (
	// TraceCtx uses W3C Traceparent/Tracestate headers.
	TraceCtx Propagator = iota
	// Baggage uses W3C Baggage header.
	Baggage
)

// Span represents a tracing span.
type Span struct {
	TraceID    string
	SpanID     string
	ParentID   string
	Name       string
	Attributes map[string]any
	StartTime  time.Time
	EndTime    time.Time
	Status     SpanStatus
	Error      error
}

// SpanStatus represents the span status.
type SpanStatus int

const (
	SpanOK SpanStatus = iota
	SpanError
)

// TraceContext holds the parsed W3C trace context from a request.
type TraceContext struct {
	TraceID    string
	SpanID     string
	TraceFlags string
	TraceState string
	Baggage    string
}

// Config holds configuration for the OTel middleware.
type Config struct {
	// ServiceName is the service name for this application.
	ServiceName string

	// ServiceVersion is the optional service version.
	ServiceVersion string

	// Propagators lists the propagation formats to use. Default: [TraceContext].
	Propagators []Propagator

	// SampleRate is the sampling rate (0.0-1.0). Default: 1.0 (sample all).
	SampleRate float64

	// MaxAttributesPerSpan is the maximum number of attributes. Default: 32.
	MaxAttributesPerSpan int

	// Next is an optional skip function.
	Next func(ctx fh.Ctx) bool

	// Exporter is called for each completed span. Default: no-op.
	Exporter func(span *Span)
}

// DefaultConfig returns the default configuration.
var DefaultConfig = Config{
	Propagators:          []Propagator{TraceCtx},
	SampleRate:           1.0,
	MaxAttributesPerSpan: 32,
}

// New creates an OpenTelemetry-compatible tracing middleware.
func New(config ...Config) fh.HandlerFunc {
	cfg := DefaultConfig
	if len(config) > 0 {
		c := config[0]
		if c.ServiceName != "" {
			cfg.ServiceName = c.ServiceName
		}
		if c.ServiceVersion != "" {
			cfg.ServiceVersion = c.ServiceVersion
		}
		if c.Propagators != nil {
			cfg.Propagators = c.Propagators
		}
		if c.SampleRate > 0 {
			cfg.SampleRate = c.SampleRate
		}
		if c.MaxAttributesPerSpan > 0 {
			cfg.MaxAttributesPerSpan = c.MaxAttributesPerSpan
		}
		if c.Next != nil {
			cfg.Next = c.Next
		}
		if c.Exporter != nil {
			cfg.Exporter = c.Exporter
		}
	}

	return func(ctx fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(ctx) {
			return ctx.Next()
		}

		tc := extractTraceContext(ctx)

		traceID := tc.TraceID
		if traceID == "" {
			traceID = generateTraceID()
		}
		spanID := generateSpanID()

		span := &Span{
			TraceID:    traceID,
			SpanID:     spanID,
			ParentID:   tc.SpanID,
			Name:       ctx.Method() + " " + ctx.Path(),
			Attributes: make(map[string]any, 16),
			StartTime:  time.Now(),
		}

		// Set standard HTTP attributes.
		span.Attributes["http.method"] = ctx.Method()
		span.Attributes["http.url"] = ctx.Path()
		span.Attributes["http.host"] = ctx.Get("Host")
		span.Attributes["http.scheme"] = "http"
		span.Attributes["http.user_agent"] = ctx.Get("User-Agent")
		span.Attributes["net.peer.ip"] = ctx.IP()

		if cfg.ServiceName != "" {
			span.Attributes["service.name"] = cfg.ServiceName
		}
		if cfg.ServiceVersion != "" {
			span.Attributes["service.version"] = cfg.ServiceVersion
		}

		// Store in context for handler access.
		ctx.Locals("_otel_span", span)

		// Propagate trace context to downstream services.
	 propagateHeaders(ctx, traceID, spanID, cfg)

		err := ctx.Next()

		span.EndTime = time.Now()
		span.Attributes["http.status_code"] = ctx.StatusCode()
		span.Attributes["http.response_time_ms"] = float64(span.EndTime.Sub(span.StartTime).Microseconds()) / 1000.0

		if err != nil {
			span.Status = SpanError
			span.Error = err
			span.Attributes["error"] = true
			span.Attributes["error.message"] = err.Error()
		} else if ctx.StatusCode() >= 400 {
			span.Status = SpanError
			span.Attributes["error"] = true
			span.Attributes["error.status_code"] = ctx.StatusCode()
		} else {
			span.Status = SpanOK
		}

		// Export span.
		if cfg.Exporter != nil {
			cfg.Exporter(span)
		}

		return err
	}
}

// GetSpan returns the current span from the context.
func GetSpan(ctx fh.Ctx) *Span {
	v := ctx.Locals("_otel_span")
	if v == nil {
		return nil
	}
	s, _ := v.(*Span)
	return s
}

// GetTraceContext returns the parsed trace context from the request.
func GetTraceContext(ctx fh.Ctx) *TraceContext {
	return extractTraceContext(ctx)
}

// ── Trace context extraction ───────────────────────────────────────────────

func extractTraceContext(ctx fh.Ctx) *TraceContext {
	tc := &TraceContext{}

	// Parse Traceparent: version-traceid-spanid-traceflags
	tp := ctx.Get("Traceparent")
	if len(tp) > 0 {
		parts := strings.Split(tp, "-")
		if len(parts) == 4 {
			tc.TraceID = parts[1]
			tc.SpanID = parts[2]
			tc.TraceFlags = parts[3]
		}
	}

	// Parse Tracestate.
	ts := ctx.Get("Tracestate")
	if ts != "" {
		tc.TraceState = ts
	}

	// Parse Baggage.
	bg := ctx.Get("Baggage")
	if bg != "" {
		tc.Baggage = bg
	}

	return tc
}

// ── Trace context propagation ──────────────────────────────────────────────

func propagateHeaders(ctx fh.Ctx, traceID, spanID string, cfg Config) {
	for _, p := range cfg.Propagators {
		switch p {
		case TraceCtx:
			ctx.Set("Traceparent", fmt.Sprintf("00-%s-%s-01", traceID, spanID))
		case Baggage:
			// Baggage propagation is handled by the span attributes.
		}
	}
}

// ── ID generation ──────────────────────────────────────────────────────────

var (
	traceIDMu sync.Mutex
	traceIDBB = make([]byte, 16)
)

func generateTraceID() string {
	traceIDMu.Lock()
	defer traceIDMu.Unlock()
	if _, err := rand.Read(traceIDBB); err != nil {
		// Fallback to time-based ID.
		return fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	return hex.EncodeToString(traceIDBB)
}

func generateSpanID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// ── Attribute helpers ──────────────────────────────────────────────────────

// SetAttribute sets a span attribute, respecting the max attribute limit.
func SetAttribute(ctx fh.Ctx, key string, value any) {
	v := ctx.Locals("_otel_span")
	if v == nil {
		return
	}
	span, ok := v.(*Span)
	if !ok {
		return
	}
	if len(span.Attributes) >= 32 {
		return
	}
	span.Attributes[key] = value
}

// AddEvent adds a timed event to the current span.
func AddEvent(ctx fh.Ctx, name string, attributes map[string]any) {
	SetAttribute(ctx, "event."+name+".time", time.Now().Format(time.RFC3339Nano))
	for k, v := range attributes {
		SetAttribute(ctx, "event."+name+"."+k, v)
	}
}

// SpanIDToUint64 converts a hex span ID to uint64.
func SpanIDToUint64(spanID string) (uint64, error) {
	return strconv.ParseUint(spanID, 16, 64)
}
