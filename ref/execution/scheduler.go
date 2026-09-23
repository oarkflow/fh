package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/observer"
)

var (
	errNilPlan = errors.New("ref: cannot execute nil plan")
)

// nodePanicError is a pre-allocated error type for panics (avoids fmt.Errorf).
type nodePanicError struct {
	node   string
	reason any
}

func (e *nodePanicError) Error() string {
	return "ref: panic in node \"" + e.node + "\""
}

// execCancelCtx is a lightweight cancellable context that avoids heap allocation.
// Uses an atomic flag instead of the full context.WithCancel machinery.
type execCancelCtx struct {
	context.Context
	canceled atomic.Bool
	done     chan struct{}
	doneMu   sync.Mutex
}

func (c *execCancelCtx) reset(parent context.Context) {
	c.Context = parent
	c.canceled.Store(false)
	c.doneMu.Lock()
	c.done = nil
	c.doneMu.Unlock()
}

func (c *execCancelCtx) Done() <-chan struct{} {
	c.doneMu.Lock()
	if c.done == nil {
		c.done = make(chan struct{})
		if c.canceled.Load() {
			close(c.done)
		}
	}
	done := c.done
	c.doneMu.Unlock()
	return done
}

func (c *execCancelCtx) cancel() {
	c.doneMu.Lock()
	if !c.canceled.Swap(true) && c.done != nil {
		close(c.done)
	}
	c.doneMu.Unlock()
}

func (c *execCancelCtx) Err() error {
	if c.canceled.Load() {
		return context.Canceled
	}
	return c.Context.Err()
}

// ExecutionState describes how execution completed.
type ExecutionState uint8

const (
	StateCompleted ExecutionState = iota
	StateShortCircuited
	StateDenied
	StateDeferred
	StateFailed
)

func (s ExecutionState) String() string {
	switch s {
	case StateCompleted:
		return "completed"
	case StateShortCircuited:
		return "short_circuited"
	case StateDenied:
		return "denied"
	case StateDeferred:
		return "deferred"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ExecutionOutcome carries the final result of execution.
type ExecutionOutcome struct {
	State   ExecutionState
	Value   any
	Err     error
	Effects []any
	Meta    any
}

// NodeExecutor is the executable callback for a graph node.
// It directly receives an isolated NodeContext, decoupling execution from the graph layer.
type NodeExecutor func(nc *NodeContext) error

// CompiledNode pairs a descriptive graph.Node with its runtime executor.
type CompiledNode struct {
	Spec *graph.Node
	Run  NodeExecutor
}

// Program pairs a compiled graph.Plan with its executable node callbacks.
type Program struct {
	Plan    *graph.Plan
	Runners []NodeExecutor
}

// nodeState tracks per-node runtime state.
type nodeState struct {
	remaining atomic.Int32 // dependency counter
	state     atomic.Uint32
}

const (
	nsBlocked  uint32 = 0
	nsReady    uint32 = 1
	nsRunning  uint32 = 2
	nsComplete uint32 = 3
	nsCanceled uint32 = 4
)

var (
	statesPool = sync.Pool{
		New: func() any {
			s := make([]nodeState, 32)
			return &s
		},
	}

	chanPool = sync.Pool{
		New: func() any {
			return make(chan graph.NodeID, 32)
		},
	}

	execStatePool = sync.Pool{
		New: func() any {
			return &execState{}
		},
	}
)

// execState holds shared mutable state for one execution.
// All helper functions are methods on this struct to avoid closure heap escapes.
type execState struct {
	// Immutable for this execution (set once in init, read everywhere)
	ctx       context.Context
	inv       *invocation.Invocation
	plan      *graph.Plan
	runners   []NodeExecutor
	budget    *Budget
	scheduler *Scheduler
	nodeCount int

	// Per-execution state
	states    []nodeState
	facts     *fact.Store
	decisions *DecisionSet
	specCtx   execCancelCtx // embedded — no pointer allocation

	// Completion tracking
	completed chan graph.NodeID
	wg        sync.WaitGroup
	inFlight  atomic.Int32
	start     time.Time

	// Outcome state (protected by outcomeMu)
	outcomeMu    sync.Mutex
	hasSC        bool
	scOut        ExecutionOutcome
	allEffects   []any
	errOnce      sync.Once
	firstErr     error

	// Gate tracking
	queueMu        sync.Mutex
	gatedMask       uint64
	gatedOverflow   map[int]struct{}
	readyStorage    [32]graph.NodeID

	// Pooled state ownership
	statesPtr *[]nodeState
}

var outcomePool = sync.Pool{
	New: func() any {
		return &ExecutionOutcome{}
	},
}

// AcquireOutcome gets a pooled ExecutionOutcome.
func AcquireOutcome() *ExecutionOutcome {
	return outcomePool.Get().(*ExecutionOutcome)
}

// ReleaseOutcome returns an ExecutionOutcome to the pool.
func ReleaseOutcome(out *ExecutionOutcome) {
	if out == nil {
		return
	}
	out.State = StateCompleted
	out.Value = nil
	out.Err = nil
	out.Effects = nil
	out.Meta = nil
	outcomePool.Put(out)
}

// isGateEligible checks whether a dependency-ready node can run now.
func (es *execState) isGateEligible(node *graph.Node) bool {
	if es.decisions.Verdict() == VerdictDeny {
		return false
	}
	if node.Kind == graph.DecisionNode {
		return true
	}
	if node.Kind == graph.OperationNode || node.Kind == graph.EffectNode || node.Kind == graph.AsyncEffect {
		if es.plan.HasDecisions && es.decisions.Verdict() != VerdictAllow {
			return false
		}
		return true
	}
	switch node.Speculation {
	case graph.NoSpeculation:
		if es.plan.HasDecisions && es.decisions.Verdict() != VerdictAllow {
			return false
		}
		return true
	case graph.PreAuthSafe:
		return true
	case graph.PostIdentitySafe:
		for _, slot := range node.Requires {
			if !es.facts.Has(slot) {
				return false
			}
		}
		return true
	case graph.PostPolicySafe:
		if es.plan.HasDecisions && es.decisions.Verdict() != VerdictAllow {
			return false
		}
		return true
	}
	return false
}

// executeNode executes a single node synchronously.
func (es *execState) executeNode(nodeID graph.NodeID) {
	node := es.plan.Nodes[nodeID]

	es.outcomeMu.Lock()
	hasSC := es.hasSC
	es.outcomeMu.Unlock()
	if hasSC || es.specCtx.Err() != nil {
		es.states[nodeID].state.Store(nsCanceled)
		return
	}

	nc := AcquireNodeContext(
		&es.specCtx,
		es.inv,
		es.facts,
		es.budget,
		es.decisions,
		nodeID,
		es.plan.DefToSlot,
		es.plan.DefSlots,
		es.plan.MaxDefID,
	)

	info := node.Info()
	for _, obs := range es.scheduler.observers {
		obs.NodeStarted(info)
	}

	var runErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				runErr = &nodePanicError{node: node.Name, reason: r}
			}
		}()
		if int(nodeID) < len(es.runners) && es.runners[nodeID] != nil {
			runErr = es.runners[nodeID](nc)
		}
	}()

	for _, obs := range es.scheduler.observers {
		obs.NodeFinished(info, runErr)
	}

	es.outcomeMu.Lock()
	// Direct iteration avoids the defensive copy in nc.Effects()
	nc.mu.Lock()
	for _, fx := range nc.effects {
		es.allEffects = append(es.allEffects, fx)
	}
	nc.mu.Unlock()
	if sc, ok := nc.GetShortCircuit(); ok && !es.hasSC {
		es.hasSC = true
		es.scOut = *sc
		es.specCtx.cancel()
	}
	es.outcomeMu.Unlock()

	ReleaseNodeContext(nc)

	if runErr != nil {
		es.errOnce.Do(func() { es.firstErr = runErr })
		es.states[nodeID].state.Store(nsCanceled)

		if node.Kind == graph.DecisionNode && es.decisions.Verdict() == VerdictDeny {
			es.specCtx.cancel()
		}
	} else {
		es.states[nodeID].state.Store(nsComplete)
	}
}

// launchAsync launches a node for async execution in a goroutine.
func (es *execState) launchAsync(nodeID graph.NodeID) {
	if !es.states[nodeID].state.CompareAndSwap(nsReady, nsRunning) {
		return
	}

	es.inFlight.Add(1)
	es.wg.Add(1)
	go func() {
		defer es.wg.Done()
		es.executeNode(nodeID)
		select {
		case es.completed <- nodeID:
		default:
		}
	}()
}

// checkGatedWait evaluates all waiting nodes and promotes eligible ones.
func (es *execState) checkGatedWait(dst []graph.NodeID) []graph.NodeID {
	es.queueMu.Lock()
	defer es.queueMu.Unlock()
	if es.gatedMask == 0 && (es.gatedOverflow == nil || len(es.gatedOverflow) == 0) {
		return dst
	}
	for nid := 0; nid < es.nodeCount && nid < 64; nid++ {
		bit := uint64(1) << nid
		if es.gatedMask&bit != 0 && es.isGateEligible(es.plan.Nodes[nid]) {
			es.gatedMask &^= bit
			es.states[nid].state.Store(nsReady)
			dst = append(dst, graph.NodeID(nid))
		}
	}
	for nid := range es.gatedOverflow {
		if nid >= 64 && es.isGateEligible(es.plan.Nodes[nid]) {
			delete(es.gatedOverflow, nid)
			es.states[nid].state.Store(nsReady)
			dst = append(dst, graph.NodeID(nid))
		}
	}
	return dst
}

// propagateDownstream decrements downstream dependency counters and collects newly ready nodes.
func (es *execState) propagateDownstream(nodeID graph.NodeID, dst []graph.NodeID) []graph.NodeID {
	es.queueMu.Lock()
	defer es.queueMu.Unlock()
	for _, downstream := range es.plan.Adjacency[nodeID] {
		remaining := es.states[downstream].remaining.Add(-1)
		if remaining == 0 {
			dn := es.plan.Nodes[downstream]
			if es.isGateEligible(dn) {
				es.states[downstream].state.Store(nsReady)
				dst = append(dst, downstream)
			} else {
				es.states[downstream].state.Store(nsBlocked)
				if downstream < 64 {
					es.gatedMask |= 1 << downstream
				} else {
					es.gatedOverflow[int(downstream)] = struct{}{}
				}
			}
		}
	}
	return dst
}

// reportFinish notifies observers of execution completion.
func (es *execState) reportFinish(err error) {
	durationMs := float64(time.Since(es.start).Nanoseconds()) / 1e6
	for _, obs := range es.scheduler.observers {
		obs.ExecutionFinished(string(es.inv.Intent), durationMs, err)
	}
}

// reset clears the execState for reuse via the pool.
func (es *execState) reset() {
	es.ctx = nil
	es.inv = nil
	es.plan = nil
	es.runners = nil
	es.budget = nil
	es.scheduler = nil
	es.states = nil
	es.facts = nil
	es.decisions = nil
	es.hasSC = false
	es.scOut = ExecutionOutcome{}
	es.allEffects = es.allEffects[:0]
	es.firstErr = nil
	es.errOnce = sync.Once{}
	es.inFlight.Store(0)
	es.completed = nil
	es.gatedMask = 0
	es.gatedOverflow = nil
	es.statesPtr = nil
	es.nodeCount = 0
}

// Scheduler executes a compiled Plan using dependency readiness and explicit gate queues.
// Dependencies determine readiness — not stage indices.
type Scheduler struct {
	observers []observer.Observer
}

// NewScheduler creates a Scheduler with the given observers.
func NewScheduler(observers ...observer.Observer) *Scheduler {
	return &Scheduler{observers: observers}
}

// ExecuteProgram executes a compiled Program.
func (s *Scheduler) ExecuteProgram(
	ctx context.Context,
	inv *invocation.Invocation,
	prog *Program,
	budget *Budget,
) (*ExecutionOutcome, error) {
	if prog == nil {
		return nil, fmt.Errorf("ref: cannot execute nil program")
	}
	return s.Execute(ctx, inv, prog.Plan, prog.Runners, budget)
}

// Execute runs a plan with node executors to completion.
func (s *Scheduler) Execute(
	ctx context.Context,
	inv *invocation.Invocation,
	plan *graph.Plan,
	runners []NodeExecutor,
	budget *Budget,
) (*ExecutionOutcome, error) {
	if plan == nil {
		return nil, errNilPlan
	}

	nodeCount := len(plan.Nodes)

	// Acquire pooled execState — all mutable state lives here,
	// eliminating closure heap escapes.
	es := execStatePool.Get().(*execState)
	es.ctx = ctx
	es.inv = inv
	es.plan = plan
	es.runners = runners
	es.budget = budget
	es.scheduler = s
	es.nodeCount = nodeCount
	es.start = time.Now()
	es.specCtx.reset(ctx)
	es.firstErr = nil
	es.errOnce = sync.Once{}
	es.hasSC = false
	es.scOut = ExecutionOutcome{}
	if cap(es.allEffects) > 0 {
		es.allEffects = es.allEffects[:0]
	} else {
		es.allEffects = nil
	}
	es.gatedMask = 0
	if nodeCount > 64 {
		es.gatedOverflow = make(map[int]struct{})
	} else {
		es.gatedOverflow = nil
	}
	es.inFlight.Store(0)

	defer func() {
		es.specCtx.cancel()
		// Return pooled resources
		if es.statesPtr != nil {
			st := es.states
			for i := range st {
				st[i].remaining.Store(0)
				st[i].state.Store(0)
			}
			statesPool.Put(es.statesPtr)
		}
		if es.completed != nil && nodeCount <= 32 {
			for len(es.completed) > 0 {
				<-es.completed
			}
			chanPool.Put(es.completed)
		}
		fact.ReleaseStore(es.facts)
		ReleaseDecisionSet(es.decisions)
		es.reset()
		execStatePool.Put(es)
	}()

	// Acquire pooled nodeState slice
	if nodeCount <= 32 {
		es.statesPtr = statesPool.Get().(*[]nodeState)
		es.states = (*es.statesPtr)[:nodeCount]
	} else {
		es.statesPtr = nil
		es.states = make([]nodeState, nodeCount)
	}

	for i, deps := range plan.InitialDeps {
		es.states[i].remaining.Store(deps)
		es.states[i].state.Store(nsBlocked)
	}

	es.facts = fact.AcquireStore(plan.SlotCount)
	es.decisions = AcquireDecisionSet(int32(plan.DecisionCount))

	if nodeCount <= 32 {
		es.completed = chanPool.Get().(chan graph.NodeID)
	} else {
		es.completed = make(chan graph.NodeID, nodeCount)
	}

	// Seed: collect initial nodes with zero dependencies
	es.queueMu.Lock()
	initialReady := es.readyStorage[:0]
	for i := range es.states {
		if es.states[i].remaining.Load() == 0 {
			node := plan.Nodes[i]
			if es.isGateEligible(node) {
				es.states[i].state.Store(nsReady)
				initialReady = append(initialReady, graph.NodeID(i))
			} else {
				es.states[i].state.Store(nsBlocked)
				if i < 64 {
					es.gatedMask |= 1 << i
				}
			}
		}
	}
	es.queueMu.Unlock()

	// Synchronous Fast-Path: run nodes inline when possible.
	// Handles multi-root DAGs by running all initial ready nodes sequentially
	// when they are independent (no goroutine overhead for linear/simple plans).
	for len(initialReady) >= 1 && es.inFlight.Load() == 0 {
		if len(initialReady) == 1 {
			// Single ready node — run inline
			curr := initialReady[0]
			initialReady = nil

			es.states[curr].state.Store(nsRunning)
			es.executeNode(curr)

			es.outcomeMu.Lock()
			hasSC := es.hasSC
			es.outcomeMu.Unlock()
			if hasSC || es.firstErr != nil || (plan.Nodes[curr].Kind == graph.DecisionNode && es.decisions.Verdict() == VerdictDeny) {
				goto finished
			}

			nextReady := es.propagateDownstream(curr, es.readyStorage[:0])
			nextReady = es.checkGatedWait(nextReady)

			if len(nextReady) >= 1 {
				initialReady = nextReady
				continue
			}
			// No successors — done
			goto finished
		}

		// Multiple ready nodes — run all sequentially inline
		// (avoids goroutine overhead for small plans with independent roots)
		for _, nid := range initialReady {
			es.states[nid].state.Store(nsRunning)
			es.executeNode(nid)

			es.outcomeMu.Lock()
			hasSC := es.hasSC
			es.outcomeMu.Unlock()
			if hasSC || es.firstErr != nil {
				goto finished
			}
		}

		// Propagate all completed roots and collect next wave
		nextReady := es.readyStorage[:0]
		for _, nid := range initialReady {
			nextReady = es.propagateDownstream(nid, nextReady)
		}
		nextReady = es.checkGatedWait(nextReady)
		initialReady = nil

		if len(nextReady) >= 1 {
			initialReady = nextReady
			continue
		}
		goto finished
	}

	if es.inFlight.Load() == 0 && len(initialReady) == 0 {
		goto finished
	}

	// Launch any remaining initial nodes concurrently
	for _, nid := range initialReady {
		es.launchAsync(nid)
	}

	for {
		select {
		case <-ctx.Done():
			es.specCtx.cancel()
			es.wg.Wait()
			es.reportFinish(ctx.Err())
			out := AcquireOutcome()
			out.State = StateFailed
			out.Err = ctx.Err()
			return out, ctx.Err()

		case nodeID := <-es.completed:
			es.inFlight.Add(-1)

			es.outcomeMu.Lock()
			hasSC := es.hasSC
			es.outcomeMu.Unlock()

			if hasSC {
				es.specCtx.cancel()
				es.wg.Wait()
				es.outcomeMu.Lock()
				out := AcquireOutcome()
				*out = es.scOut
				out.Effects = es.allEffects
				es.outcomeMu.Unlock()
				es.reportFinish(nil)
				return out, nil
			}

			if es.firstErr != nil {
				es.specCtx.cancel()
				es.wg.Wait()
				es.reportFinish(es.firstErr)
				out := AcquireOutcome()
				if es.decisions.Verdict() == VerdictDeny {
					out.State = StateDenied
				} else {
					out.State = StateFailed
				}
				out.Err = es.firstErr
				out.Effects = es.allEffects
				return out, es.firstErr
			}

			if plan.Nodes[nodeID].Kind == graph.DecisionNode && es.decisions.Verdict() == VerdictDeny {
				es.specCtx.cancel()
				es.wg.Wait()
				es.reportFinish(nil)
				out := AcquireOutcome()
				out.State = StateDenied
				out.Effects = es.allEffects
				return out, nil
			}

			// Propagate to downstream
			downstreamReady := es.propagateDownstream(nodeID, es.readyStorage[:0])
			downstreamReady = es.checkGatedWait(downstreamReady)

			for _, nid := range downstreamReady {
				es.launchAsync(nid)
			}

			if es.inFlight.Load() == 0 {
				goto finished
			}
		}
	}

finished:

	es.wg.Wait()

	if es.firstErr != nil {
		es.reportFinish(es.firstErr)
		out := AcquireOutcome()
		if es.decisions.Verdict() == VerdictDeny {
			out.State = StateDenied
		} else {
			out.State = StateFailed
		}
		out.Err = es.firstErr
		out.Effects = es.allEffects
		return out, es.firstErr
	}

	es.queueMu.Lock()
	stuckGated := es.gatedMask != 0
	es.queueMu.Unlock()

	if stuckGated || es.decisions.Verdict() == VerdictDeny {
		outcome := AcquireOutcome()
		outcome.State = StateDenied
		outcome.Effects = es.allEffects
		es.reportFinish(nil)
		return outcome, nil
	}

	es.outcomeMu.Lock()
	if es.hasSC {
		out := AcquireOutcome()
		*out = es.scOut
		out.Effects = es.allEffects
		es.outcomeMu.Unlock()
		es.reportFinish(nil)
		return out, nil
	}
	es.outcomeMu.Unlock()

	finalOutcome := AcquireOutcome()
	finalOutcome.State = StateCompleted
	finalOutcome.Effects = es.allEffects

	es.reportFinish(nil)

	return finalOutcome, nil
}
