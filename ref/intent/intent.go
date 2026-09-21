package intent

import (
	"time"

	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
)

// Name uniquely identifies an intent.
type Name string

// Version supports contract evolution.
type Version int

// Outcome is the transport-agnostic result of an intent execution.
// No HTTP status, no HTTP headers — projection decides those.
type Outcome[T any] struct {
	Value   T
	Effects effect.EffectPlan
	Meta    OutcomeMeta
}

// OutcomeMeta contains transport-neutral execution metadata.
type OutcomeMeta struct {
	CacheControl string
	Tags         map[string]string
}

// Failure is a transport-agnostic domain error.
// Projection maps Category -> HTTP status / gRPC code.
type Failure struct {
	Code      string
	Category  Category
	Message   string
	Retryable bool
	Cause     error
	Meta      map[string]any
}

func (f Failure) Error() string {
	if f.Message != "" {
		return f.Message
	}
	return f.Code
}

func (f Failure) Unwrap() error {
	return f.Cause
}

// Category classifies failures for transport projection.
type Category uint8

const (
	CategoryInvalidInput Category = iota // -> HTTP 422, gRPC INVALID_ARGUMENT
	CategoryNotFound                      // -> HTTP 404, gRPC NOT_FOUND
	CategoryConflict                      // -> HTTP 409, gRPC ALREADY_EXISTS
	CategoryPermission                    // -> HTTP 403, gRPC PERMISSION_DENIED
	CategoryAuth                          // -> HTTP 401, gRPC UNAUTHENTICATED
	CategoryRateLimit                     // -> HTTP 429, gRPC RESOURCE_EXHAUSTED
	CategoryUnavailable                   // -> HTTP 503, gRPC UNAVAILABLE
	CategoryTimeout                       // -> HTTP 504, gRPC DEADLINE_EXCEEDED
	CategoryInternal                      // -> HTTP 500, gRPC INTERNAL
)

func (c Category) String() string {
	switch c {
	case CategoryInvalidInput:
		return "invalid_input"
	case CategoryNotFound:
		return "not_found"
	case CategoryConflict:
		return "conflict"
	case CategoryPermission:
		return "permission_denied"
	case CategoryAuth:
		return "unauthenticated"
	case CategoryRateLimit:
		return "rate_limit_exceeded"
	case CategoryUnavailable:
		return "unavailable"
	case CategoryTimeout:
		return "timeout"
	case CategoryInternal:
		return "internal_error"
	default:
		return "unknown"
	}
}

// Spec declares intent requirements and resource envelopes.
type Spec struct {
	Description   string
	Requires      []fact.AnyKey
	Timeout       time.Duration
	MaxDBQueries  int
	MaxExternalIO int
	MaxMemory     int64
	MaxEffects    int
}

// Intent is the strongly typed interface for business logic operations.
type Intent[I, O any] interface {
	Name() Name
	Spec() Spec
	Run(nc *execution.NodeContext, input I) (Outcome[O], error)
}

// Definition is the type-erased registration compiled by the engine.
type Definition struct {
	Name       Name
	Version    Version
	Spec       Spec
	InputKey   fact.AnyKey
	DecodeNode func(nc *execution.NodeContext) error
	Run        func(nc *execution.NodeContext) (any, effect.EffectPlan, OutcomeMeta, error)
}
