package capability

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
)

var (
	// TraceKey is the typed fact key for distributed trace context.
	TraceKey = fact.NewKey[TraceFact]("trace.context")
)

// TraceFact represents distributed tracing metadata.
type TraceFact struct {
	TraceID  string
	SpanID   string
	ParentID string
	Baggage  map[string]string
}

// TraceExtractorFunc extracts or generates trace context from the node context.
type TraceExtractorFunc func(nc *execution.NodeContext) TraceFact

// NewTraceCapability creates a trace context propagation capability (PureNode, PreAuthSafe).
func NewTraceCapability(name string, extractor TraceExtractorFunc, opts ...Option) Registration {
	if name == "" {
		name = "capability.trace"
	}
	reg := NewRegistration(name, graph.PureNode, opts...)
	if reg.Speculation == graph.NoSpeculation {
		reg.Speculation = graph.PreAuthSafe
	}
	reg.Provides = []fact.AnyKey{TraceKey.Any()}

	reg.Run = func(nc *execution.NodeContext) error {
		var tf TraceFact
		if extractor != nil {
			tf = extractor(nc)
		} else {
			tf = DefaultTraceExtractor(nc)
		}

		execution.Publish(nc, TraceKey, tf)
		return nil
	}

	return reg
}

// DefaultTraceExtractor generates randomized hex IDs if none exist.
func DefaultTraceExtractor(nc *execution.NodeContext) TraceFact {
	var b [16]byte
	_, _ = rand.Read(b[:])
	traceID := hex.EncodeToString(b[:])

	var s [8]byte
	_, _ = rand.Read(s[:])
	spanID := hex.EncodeToString(s[:])

	return TraceFact{
		TraceID: traceID,
		SpanID:  spanID,
		Baggage: make(map[string]string),
	}
}
