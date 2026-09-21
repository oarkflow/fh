package capability

import (
	"time"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
)

// Producer is a generic capability that produces a typed fact value.
type Producer[T any] interface {
	Produce(nc *execution.NodeContext) (T, error)
}

// ProducerFunc is an adapter to use a plain function as a Producer.
type ProducerFunc[T any] func(nc *execution.NodeContext) (T, error)

func (f ProducerFunc[T]) Produce(nc *execution.NodeContext) (T, error) {
	return f(nc)
}

// Registration describes a capability for the graph compiler.
type Registration struct {
	Name        string
	Requires    []fact.AnyKey
	Provides    []fact.AnyKey
	Kind        graph.NodeKind
	Speculation graph.SpeculationClass
	Resilience  Resilience
	Run         func(nc *execution.NodeContext) error
}

// Resilience describes per-capability operational characteristics.
type Resilience struct {
	Timeout     time.Duration
	MaxRetries  int
	Backoff     time.Duration
	Bulkhead    string
	Concurrency int
	Cacheable   bool
	Idempotent  bool
}

// Option configures a Registration.
type Option func(*Registration)

func WithTimeout(d time.Duration) Option {
	return func(r *Registration) { r.Resilience.Timeout = d }
}

func WithRetry(max int, backoff time.Duration) Option {
	return func(r *Registration) {
		r.Resilience.MaxRetries = max
		r.Resilience.Backoff = backoff
	}
}

func WithBulkhead(name string, concurrency int) Option {
	return func(r *Registration) {
		r.Resilience.Bulkhead = name
		r.Resilience.Concurrency = concurrency
	}
}

func WithSpeculation(class graph.SpeculationClass) Option {
	return func(r *Registration) { r.Speculation = class }
}

func WithKind(k graph.NodeKind) Option {
	return func(r *Registration) { r.Kind = k }
}

// NewRegistration creates a capability registration with options.
func NewRegistration(name string, kind graph.NodeKind, opts ...Option) Registration {
	r := Registration{
		Name: name,
		Kind: kind,
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

// Pure creates a PureNode capability registration (deterministic computation, no external I/O).
func Pure(name string, opts ...Option) Registration {
	return NewRegistration(name, graph.PureNode, opts...)
}

// Read creates a ReadNode capability registration (reads external state).
func Read(name string, opts ...Option) Registration {
	return NewRegistration(name, graph.ReadNode, opts...)
}

// Decision creates a DecisionNode capability registration (policy/authorization).
func Decision(name string, opts ...Option) Registration {
	return NewRegistration(name, graph.DecisionNode, opts...)
}

// WithRequires sets required fact keys on the registration.
func (r Registration) WithRequires(keys ...fact.AnyKey) Registration {
	r.Requires = keys
	return r
}

// WithProvides sets provided fact keys on the registration.
func (r Registration) WithProvides(keys ...fact.AnyKey) Registration {
	r.Provides = keys
	return r
}

// WithRun sets the execution callback on the registration.
func (r Registration) WithRun(run func(nc *execution.NodeContext) error) Registration {
	r.Run = run
	return r
}
