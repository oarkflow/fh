package ref

import (
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/observer"
	"github.com/oarkflow/fh/ref/runtime"
)

// Re-exported primary types
type (
	Engine         = runtime.Engine
	Option         = runtime.Option
	DispatchResult = runtime.DispatchResult

	Invocation = invocation.Invocation
	Input      = invocation.Input

	Intent[I, O any] = intent.Intent[I, O]
	Outcome[T any]   = intent.Outcome[T]
	OutcomeMeta      = intent.OutcomeMeta
	Failure          = intent.Failure
	Category         = intent.Category

	Effect     = effect.Effect
	EffectPlan = effect.EffectPlan
	EffectKind = effect.EffectKind

	Constraint = execution.Constraint
	Obligation = execution.Obligation

	NodeContext  = execution.NodeContext
	NodeExecutor = execution.NodeExecutor
	CompiledNode = execution.CompiledNode
	Program      = execution.Program
	Budget       = execution.Budget
	DecisionSet  = execution.DecisionSet
	Verdict      = execution.Verdict

	Observer = observer.Observer
)

const (
	LocalTransactional = effect.LocalTransactional
	DurableDelivery    = effect.DurableDelivery
	FireAndForget      = effect.FireAndForget

	NoSpeculation    = graph.NoSpeculation
	PreAuthSafe      = graph.PreAuthSafe
	PostIdentitySafe = graph.PostIdentitySafe
	PostPolicySafe   = graph.PostPolicySafe
)

// Re-exported constructors and helpers
var (
	NewEngine = runtime.NewEngine
	NewInput  = invocation.NewInput

	WithEffectStore = runtime.WithEffectStore
	WithObserver    = runtime.WithObserver
	WithCapability  = runtime.WithCapability

	Pure     = capability.Pure
	Read     = capability.Read
	Decision = capability.Decision
)

// NewKey creates a typed fact key with a stable definition ID.
func NewKey[T any](name string) fact.Key[T] {
	return fact.NewKey[T](name)
}

// Publish stores a typed fact by its Key in the node context.
func Publish[T any](nc *NodeContext, key fact.Key[T], value T) bool {
	return execution.Publish(nc, key, value)
}

// Require retrieves a typed fact by its Key from the node context.
func Require[T any](nc *NodeContext, key fact.Key[T]) (T, error) {
	return execution.Require(nc, key)
}

// Register registers a strongly typed intent into the engine.
func Register[I, O any](e *Engine, it intent.Intent[I, O], customDecoder ...intent.DecoderFunc[I]) error {
	return intent.Register(e.Intents(), it, customDecoder...)
}

// RegisterCapability registers a capability into the engine.
func RegisterCapability(e *Engine, reg capability.Registration) error {
	return e.Capabilities().Register(reg)
}
