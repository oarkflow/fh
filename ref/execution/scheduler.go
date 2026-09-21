package execution

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/observer"
)

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
)

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
		return nil, fmt.Errorf("ref: cannot execute nil plan")
	}

	start := time.Now()
	nodeCount := len(plan.Nodes)

	// Acquire pooled nodeState slice
	var states []nodeState
	var statesPtr *[]nodeState
	if nodeCount <= 32 {
		statesPtr = statesPool.Get().(*[]nodeState)
		states = (*statesPtr)[:nodeCount]
		defer func() {
			statesPool.Put(statesPtr)
		}()
	} else {
		states = make([]nodeState, nodeCount)
	}

	for i, deps := range plan.InitialDeps {
		states[i].remaining.Store(deps)
		states[i].state.Store(nsBlocked)
	}

	facts := fact.AcquireStore(plan.SlotCount)
	defer fact.ReleaseStore(facts)

	decisions := AcquireDecisionSet(int32(plan.DecisionCount))
	defer ReleaseDecisionSet(decisions)

	var completed chan graph.NodeID
	if nodeCount <= 32 {
		completed = chanPool.Get().(chan graph.NodeID)
		defer func() {
			for len(completed) > 0 {
				<-completed
			}
			chanPool.Put(completed)
		}()
	} else {
		completed = make(chan graph.NodeID, nodeCount)
	}

	var errOnce sync.Once
	var firstErr error
	var inFlight atomic.Int32

	var outcomeMu sync.Mutex
	var shortCircuit *ExecutionOutcome
	var allEffects []any

	specCtx, specCancel := context.WithCancel(ctx)
	defer specCancel()

	var wg sync.WaitGroup

	// Bitmask for gatedWait when nodeCount <= 64 (zero map allocations)
	var gatedMask uint64
	var queueMu sync.Mutex

	// isGateEligible checks whether a dependency-ready node can run now
	isGateEligible := func(node *graph.Node) bool {
		if decisions.Verdict() == VerdictDeny {
			return false
		}

		// DecisionNodes themselves make decisions; eligible when dependency-ready
		if node.Kind == graph.DecisionNode {
			return true
		}

		// Operations and Effects strictly require policy gate approval
		if node.Kind == graph.OperationNode || node.Kind == graph.EffectNode || node.Kind == graph.AsyncEffect {
			if plan.HasDecisions && decisions.Verdict() != VerdictAllow {
				return false
			}
			return true
		}

		switch node.Speculation {
		case graph.NoSpeculation:
			// NoSpeculation requires all decisions to pass before running
			if plan.HasDecisions && decisions.Verdict() != VerdictAllow {
				return false
			}
			return true

		case graph.PreAuthSafe:
			// PreAuthSafe can run before identity or decisions are available
			return true

		case graph.PostIdentitySafe:
			// Safe once all its required facts are published
			for _, slot := range node.Requires {
				if !facts.Has(slot) {
					return false
				}
			}
			return true

		case graph.PostPolicySafe:
			if plan.HasDecisions && decisions.Verdict() != VerdictAllow {
				return false
			}
			return true
		}

		return false
	}

	// executeNode executes a single node synchronously
	executeNode := func(nodeID graph.NodeID) {
		node := plan.Nodes[nodeID]

		outcomeMu.Lock()
		hasSC := shortCircuit != nil
		outcomeMu.Unlock()
		if hasSC || specCtx.Err() != nil {
			states[nodeID].state.Store(nsCanceled)
			return
		}

		nc := AcquireNodeContext(
			specCtx,
			inv,
			facts,
			budget,
			decisions,
			nodeID,
			plan.DefToSlot,
		)

		info := node.Info()
		for _, obs := range s.observers {
			obs.NodeStarted(info)
		}

		var runErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					runErr = fmt.Errorf("ref: panic in node %q: %v", node.Name, r)
				}
			}()
			if int(nodeID) < len(runners) && runners[nodeID] != nil {
				runErr = runners[nodeID](nc)
			}
		}()

		for _, obs := range s.observers {
			obs.NodeFinished(info, runErr)
		}

		outcomeMu.Lock()
		if fx := nc.Effects(); len(fx) > 0 {
			allEffects = append(allEffects, fx...)
		}
		if sc, ok := nc.GetShortCircuit(); ok && shortCircuit == nil {
			scCopy := *sc
			shortCircuit = &scCopy
			specCancel()
		}
		outcomeMu.Unlock()

		ReleaseNodeContext(nc)

		if runErr != nil {
			errOnce.Do(func() { firstErr = runErr })
			states[nodeID].state.Store(nsCanceled)

			if node.Kind == graph.DecisionNode && decisions.Verdict() == VerdictDeny {
				specCancel()
			}
		} else {
			states[nodeID].state.Store(nsComplete)
		}
	}

	var launchAsync func(nodeID graph.NodeID)

	launchAsync = func(nodeID graph.NodeID) {
		if !states[nodeID].state.CompareAndSwap(nsReady, nsRunning) {
			return
		}

		inFlight.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			executeNode(nodeID)
			select {
			case completed <- nodeID:
			default:
			}
		}()
	}

	// checkGatedWait evaluates all waiting nodes and promotes eligible ones
	checkGatedWait := func() []graph.NodeID {
		queueMu.Lock()
		defer queueMu.Unlock()

		if gatedMask == 0 {
			return nil
		}

		var newlyEligible []graph.NodeID
		for nid := 0; nid < nodeCount; nid++ {
			bit := uint64(1) << nid
			if (gatedMask & bit) != 0 {
				node := plan.Nodes[nid]
				if isGateEligible(node) {
					gatedMask &= ^bit
					states[nid].state.Store(nsReady)
					newlyEligible = append(newlyEligible, graph.NodeID(nid))
				}
			}
		}
		return newlyEligible
	}

	// Propagate completion of a node to its downstream neighbors
	propagateDownstream := func(nodeID graph.NodeID) []graph.NodeID {
		queueMu.Lock()
		defer queueMu.Unlock()

		var readyList []graph.NodeID
		for _, downstream := range plan.Adjacency[nodeID] {
			remaining := states[downstream].remaining.Add(-1)
			if remaining == 0 {
				dn := plan.Nodes[downstream]
				if isGateEligible(dn) {
					states[downstream].state.Store(nsReady)
					readyList = append(readyList, downstream)
				} else {
					states[downstream].state.Store(nsBlocked)
					if downstream < 64 {
						gatedMask |= 1 << downstream
					}
				}
			}
		}
		return readyList
	}

	// Seed: collect initial nodes with zero dependencies
	queueMu.Lock()
	var initialReady []graph.NodeID
	for i := range states {
		if states[i].remaining.Load() == 0 {
			node := plan.Nodes[i]
			if isGateEligible(node) {
				states[i].state.Store(nsReady)
				initialReady = append(initialReady, graph.NodeID(i))
			} else {
				states[i].state.Store(nsBlocked)
				if i < 64 {
					gatedMask |= 1 << i
				}
			}
		}
	}
	queueMu.Unlock()

	// Synchronous Fast-Path: if only 1 node is ready and inFlight == 0, run inline!
	for len(initialReady) == 1 && inFlight.Load() == 0 {
		curr := initialReady[0]
		initialReady = nil

		states[curr].state.Store(nsRunning)
		executeNode(curr)

		outcomeMu.Lock()
		hasSC := shortCircuit != nil
		outcomeMu.Unlock()
		if hasSC || firstErr != nil || (plan.Nodes[curr].Kind == graph.DecisionNode && decisions.Verdict() == VerdictDeny) {
			goto finished
		}

		nextReady := propagateDownstream(curr)
		newGated := checkGatedWait()
		if len(newGated) > 0 {
			nextReady = append(nextReady, newGated...)
		}

		if len(nextReady) == 1 {
			initialReady = nextReady
		} else if len(nextReady) > 1 {
			// Fork parallel branches
			for _, nid := range nextReady {
				launchAsync(nid)
			}
			break
		}
	}

	if inFlight.Load() == 0 && len(initialReady) == 0 {
		goto finished
	}

	// Launch any remaining initial nodes concurrently
	for _, nid := range initialReady {
		launchAsync(nid)
	}

	for {
		select {
		case <-ctx.Done():
			specCancel()
			wg.Wait()
			durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
			for _, obs := range s.observers {
				obs.ExecutionFinished(string(inv.Intent), durationMs, ctx.Err())
			}
			return &ExecutionOutcome{State: StateFailed, Err: ctx.Err()}, ctx.Err()

		case nodeID := <-completed:
			inFlight.Add(-1)

			outcomeMu.Lock()
			hasSC := shortCircuit != nil
			outcomeMu.Unlock()

			if hasSC {
				specCancel()
				wg.Wait()
				outcomeMu.Lock()
				scCopy := *shortCircuit
				scCopy.Effects = allEffects
				outcomeMu.Unlock()
				durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
				for _, obs := range s.observers {
					obs.ExecutionFinished(string(inv.Intent), durationMs, nil)
				}
				return &scCopy, nil
			}

			if firstErr != nil {
				specCancel()
				wg.Wait()
				durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
				for _, obs := range s.observers {
					obs.ExecutionFinished(string(inv.Intent), durationMs, firstErr)
				}
				if decisions.Verdict() == VerdictDeny {
					return &ExecutionOutcome{State: StateDenied, Err: firstErr, Effects: allEffects}, firstErr
				}
				return &ExecutionOutcome{State: StateFailed, Err: firstErr, Effects: allEffects}, firstErr
			}

			if plan.Nodes[nodeID].Kind == graph.DecisionNode && decisions.Verdict() == VerdictDeny {
				specCancel()
				wg.Wait()
				durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
				for _, obs := range s.observers {
					obs.ExecutionFinished(string(inv.Intent), durationMs, nil)
				}
				return &ExecutionOutcome{State: StateDenied, Effects: allEffects}, nil
			}

			// Propagate to downstream
			downstreamReady := propagateDownstream(nodeID)
			gatedReady := checkGatedWait()
			downstreamReady = append(downstreamReady, gatedReady...)

			for _, nid := range downstreamReady {
				launchAsync(nid)
			}

			if inFlight.Load() == 0 {
				goto finished
			}
		}
	}

finished:

	wg.Wait()

	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6

	if firstErr != nil {
		for _, obs := range s.observers {
			obs.ExecutionFinished(string(inv.Intent), durationMs, firstErr)
		}
		if decisions.Verdict() == VerdictDeny {
			return &ExecutionOutcome{State: StateDenied, Err: firstErr, Effects: allEffects}, firstErr
		}
		return &ExecutionOutcome{State: StateFailed, Err: firstErr, Effects: allEffects}, firstErr
	}

	queueMu.Lock()
	stuckGated := gatedMask != 0
	queueMu.Unlock()

	if stuckGated || decisions.Verdict() == VerdictDeny {
		outcome := &ExecutionOutcome{State: StateDenied, Effects: allEffects}
		for _, obs := range s.observers {
			obs.ExecutionFinished(string(inv.Intent), durationMs, nil)
		}
		return outcome, nil
	}

	outcomeMu.Lock()
	if shortCircuit != nil {
		scCopy := *shortCircuit
		scCopy.Effects = allEffects
		outcomeMu.Unlock()
		for _, obs := range s.observers {
			obs.ExecutionFinished(string(inv.Intent), durationMs, nil)
		}
		return &scCopy, nil
	}
	outcomeMu.Unlock()

	finalOutcome := &ExecutionOutcome{
		State:   StateCompleted,
		Effects: allEffects,
	}

	for _, obs := range s.observers {
		obs.ExecutionFinished(string(inv.Intent), durationMs, nil)
	}

	return finalOutcome, nil
}
