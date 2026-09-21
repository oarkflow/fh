package execution

import (
	"context"
	"errors"
	"sync"

	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/invocation"
)

var (
	ErrFactMissing = errors.New("ref: required fact not available")
)

type nodeContextKey struct{}

// NodeContext is the immutable context passed to each node's RunFunc.
// Each node receives its own context — never a shared mutable one.
type NodeContext struct {
	context.Context // node-scoped; cancelled independently

	invocation   *invocation.Invocation
	facts        *fact.Store
	budget       *Budget
	decisions    *DecisionSet
	nodeID       graph.NodeID
	defToSlot    map[fact.DefinitionID]fact.PlanSlot

	mu      sync.Mutex
	effects []any
	hasSC   bool
	scOut   ExecutionOutcome
}

var nodeContextPool = sync.Pool{
	New: func() any {
		return &NodeContext{}
	},
}

// AcquireNodeContext gets an isolated NodeContext from the pool.
func AcquireNodeContext(
	ctx context.Context,
	inv *invocation.Invocation,
	facts *fact.Store,
	budget *Budget,
	decisions *DecisionSet,
	nodeID graph.NodeID,
	defToSlot map[fact.DefinitionID]fact.PlanSlot,
) *NodeContext {
	nc := nodeContextPool.Get().(*NodeContext)
	nc.Context = ctx
	nc.invocation = inv
	nc.facts = facts
	nc.budget = budget
	nc.decisions = decisions
	nc.nodeID = nodeID
	nc.defToSlot = defToSlot
	nc.effects = nc.effects[:0]
	nc.hasSC = false
	return nc
}

// ReleaseNodeContext returns a NodeContext to the pool.
func ReleaseNodeContext(nc *NodeContext) {
	if nc == nil {
		return
	}
	nc.Context = nil
	nc.invocation = nil
	nc.facts = nil
	nc.budget = nil
	nc.decisions = nil
	nc.defToSlot = nil
	nc.hasSC = false
	nc.scOut.Value = nil
	nc.scOut.Meta = nil
	nc.scOut.Effects = nil
	nodeContextPool.Put(nc)
}

// NewNodeContext creates an isolated NodeContext.
func NewNodeContext(
	ctx context.Context,
	inv *invocation.Invocation,
	facts *fact.Store,
	budget *Budget,
	decisions *DecisionSet,
	nodeID graph.NodeID,
	defToSlot map[fact.DefinitionID]fact.PlanSlot,
) *NodeContext {
	return &NodeContext{
		Context:    ctx,
		invocation: inv,
		facts:      facts,
		budget:     budget,
		decisions:  decisions,
		nodeID:     nodeID,
		defToSlot:  defToSlot,
	}
}

// Value overrides context.Value to intercept nodeContextKey without allocating context.WithValue.
func (nc *NodeContext) Value(key any) any {
	if _, ok := key.(nodeContextKey); ok {
		return nc
	}
	if nc.Context != nil {
		return nc.Context.Value(key)
	}
	return nil
}

// FromContext extracts a NodeContext from context.Context.
func FromContext(ctx context.Context) *NodeContext {
	if nc, ok := ctx.(*NodeContext); ok {
		return nc
	}
	if ctx == nil {
		return nil
	}
	if val := ctx.Value(nodeContextKey{}); val != nil {
		if nc, ok := val.(*NodeContext); ok {
			return nc
		}
	}
	return nil
}

// Invocation returns the transport-neutral input (immutable).
func (nc *NodeContext) Invocation() *invocation.Invocation { return nc.invocation }

// Facts returns the shared fact store (concurrent-safe via atomics).
func (nc *NodeContext) Facts() *fact.Store { return nc.facts }

// Budget returns the enforceable execution budget.
func (nc *NodeContext) Budget() *Budget { return nc.budget }

// Decisions returns the accumulated decision set.
func (nc *NodeContext) Decisions() *DecisionSet { return nc.decisions }

// NodeID returns this node's plan-local identifier.
func (nc *NodeContext) NodeID() graph.NodeID { return nc.nodeID }

// RecordEffect records an effect for deferred two-phase commit.
func (nc *NodeContext) RecordEffect(fx any) {
	if nc == nil {
		return
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.effects = append(nc.effects, fx)
}

// Effects returns all effects recorded in this node context.
func (nc *NodeContext) Effects() []any {
	if nc == nil {
		return nil
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return append([]any(nil), nc.effects...)
}

// SetShortCircuit short-circuits the rest of the execution with a cached or replayed value.
func (nc *NodeContext) SetShortCircuit(value any) {
	nc.SetShortCircuitWithMeta(value, nil)
}

// SetShortCircuitWithMeta short-circuits with an outcome value and metadata without heap allocation.
func (nc *NodeContext) SetShortCircuitWithMeta(value any, meta any) {
	if nc == nil {
		return
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.hasSC = true
	nc.scOut.State = StateShortCircuited
	nc.scOut.Value = value
	nc.scOut.Meta = meta
}

// GetShortCircuit returns the short-circuit outcome if any was set.
func (nc *NodeContext) GetShortCircuit() (*ExecutionOutcome, bool) {
	if nc == nil {
		return nil, false
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.hasSC {
		return &nc.scOut, true
	}
	return nil, false
}

// PublishFact stores a typed fact. Convenience wrapper.
func PublishFact[T any](nc *NodeContext, slot fact.PlanSlot, value T) {
	if nc == nil {
		return
	}
	fact.Put(nc.Facts(), slot, value)
}

// RequireFact retrieves a typed fact. Returns error if missing.
func RequireFact[T any](nc *NodeContext, slot fact.PlanSlot) (T, error) {
	var zero T
	if nc == nil {
		return zero, ErrFactMissing
	}
	v, ok := fact.Get[T](nc.Facts(), slot)
	if !ok {
		return zero, ErrFactMissing
	}
	return v, nil
}

// SlotOf finds the plan slot mapped to the given fact DefinitionID.
func (nc *NodeContext) SlotOf(id fact.DefinitionID) (fact.PlanSlot, bool) {
	if nc == nil || nc.defToSlot == nil {
		return 0, false
	}
	s, ok := nc.defToSlot[id]
	return s, ok
}

// Publish stores a typed fact by its Key. Returns false if key is not in the plan.
func Publish[T any](nc *NodeContext, key fact.Key[T], value T) bool {
	if nc == nil {
		return false
	}
	slot, ok := nc.SlotOf(key.DefinitionID())
	if !ok {
		return false
	}
	fact.Put(nc.Facts(), slot, value)
	return true
}

// Require retrieves a typed fact by its Key. Returns error if unmapped or missing.
func Require[T any](nc *NodeContext, key fact.Key[T]) (T, error) {
	var zero T
	if nc == nil {
		return zero, ErrFactMissing
	}
	slot, ok := nc.SlotOf(key.DefinitionID())
	if !ok {
		return zero, ErrFactMissing
	}
	return RequireFact[T](nc, slot)
}
