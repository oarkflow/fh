package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/observer"
)

var (
	errNilInvocation = errors.New("ref: invocation is nil")
)

// effectErrorBridge adapts the observer interface to effect.EffectErrorFunc.
type effectErrorBridge struct {
	obs []observer.Observer
}

func (b *effectErrorBridge) report(err effect.EffectError) {
	for _, obs := range b.obs {
		obs.EffectCommitted(err.Name, err)
	}
}

// DispatchResult is the transport-agnostic result of an executed intent.
type DispatchResult struct {
	Value   any
	Effects effect.EffectPlan
	Meta    intent.OutcomeMeta
}

var dispatchResultPool = sync.Pool{
	New: func() any {
		return &DispatchResult{}
	},
}

// AcquireDispatchResult retrieves a pooled DispatchResult.
func AcquireDispatchResult(val any, meta intent.OutcomeMeta) *DispatchResult {
	res := dispatchResultPool.Get().(*DispatchResult)
	res.Value = val
	res.Meta = meta
	res.Effects = effect.EffectPlan{}
	return res
}

// ReleaseDispatchResult returns a DispatchResult to the pool.
func ReleaseDispatchResult(res *DispatchResult) {
	if res == nil {
		return
	}
	res.Value = nil
	res.Meta = intent.OutcomeMeta{}
	res.Effects = effect.EffectPlan{}
	dispatchResultPool.Put(res)
}

// compiledGeneration is the immutable state snapshot published after Compile().
// Accessed via atomic.Pointer for lock-free reads.
type compiledGeneration struct {
	entries map[intent.Name]*intentEntry
	plans   map[intent.Name]*graph.Plan
}

// intentEntry combines a compiled program and its definition to eliminate dual map lookups.
type intentEntry struct {
	prog *execution.Program
	def  *intent.Definition
}

// Engine coordinates the compilation, scheduling, and effect lifecycle of intents.
type Engine struct {
	mu           sync.RWMutex
	capabilities *capability.Registry
	intents      *intent.Registry
	programs     map[intent.Name]*execution.Program
	plans        map[intent.Name]*graph.Plan
	intentDefs   map[intent.Name]*intent.Definition
	effectRunner *effect.Runner
	scheduler    *execution.Scheduler
	observers    []observer.Observer
	compiled     bool
	generation   atomic.Pointer[compiledGeneration]
}

// NewEngine creates a new REF engine.
func NewEngine(opts ...Option) *Engine {
	e := &Engine{
		capabilities: capability.NewRegistry(),
		intents:      intent.NewRegistry(),
		programs:     make(map[intent.Name]*execution.Program),
		plans:        make(map[intent.Name]*graph.Plan),
		intentDefs:   make(map[intent.Name]*intent.Definition),
		effectRunner: effect.NewRunner(nil),
	}
	for _, opt := range opts {
		opt(e)
	}
	e.scheduler = execution.NewScheduler(e.observers...)
	return e
}

// Capabilities returns the capability registry.
func (e *Engine) Capabilities() *capability.Registry {
	return e.capabilities
}

// Intents returns the intent registry.
func (e *Engine) Intents() *intent.Registry {
	return e.intents
}

// RegisterIntent registers a typed Intent[I, O] with the engine.
func (e *Engine) RegisterIntent(it intent.Intent[any, any]) error {
	return intent.Register(e.intents, it)
}

// RegisterDefinition registers a type-erased intent produced by a trusted
// declarative compiler. Definitions cannot be added after Compile because a
// compiled Engine is an immutable generation.
func (e *Engine) RegisterDefinition(def *intent.Definition) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.compiled {
		return fmt.Errorf("ref: cannot register intent after engine compilation")
	}
	return e.intents.RegisterDefinition(def)
}

// Compile builds execution plans for all registered intents.
func (e *Engine) Compile() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	defs := e.intents.All()
	for name, def := range defs {
		prog, err := e.compileIntent(def)
		if err != nil {
			return fmt.Errorf("ref: intent %q compilation error: %w", name, err)
		}
		e.programs[name] = prog
		e.plans[name] = prog.Plan
		e.intentDefs[name] = def
	}

	e.compiled = true

	// Publish immutable generation for lock-free dispatch
	gen := &compiledGeneration{
		entries: make(map[intent.Name]*intentEntry, len(e.programs)),
		plans:   e.plans,
	}
	for name, prog := range e.programs {
		gen.entries[name] = &intentEntry{prog: prog, def: e.intentDefs[name]}
	}
	e.generation.Store(gen)

	return nil
}

func (e *Engine) compileIntent(def *intent.Definition) (*execution.Program, error) {
	// 1. Resolve transitive capability dependencies
	capSet := make(map[string]*capability.Registration)
	factQueue := append([]fact.AnyKey(nil), def.Spec.Requires...)
	visitedFacts := make(map[fact.DefinitionID]bool)

	for len(factQueue) > 0 {
		reqFact := factQueue[0]
		factQueue = factQueue[1:]

		if visitedFacts[reqFact.DefID] {
			continue
		}
		visitedFacts[reqFact.DefID] = true
		// The intent decoder is the producer for its input fact. It is installed
		// below as a graph node rather than in the capability registry.
		if def.InputKey.DefID != 0 && reqFact.DefID == def.InputKey.DefID {
			continue
		}

		producer, ok := e.capabilities.ProducerOf(reqFact.DefID)
		if !ok {
			return nil, fmt.Errorf("ref: no capability registered to produce fact %q (def %d)",
				reqFact.Name, reqFact.DefID)
		}

		if _, exists := capSet[producer.Name]; !exists {
			capSet[producer.Name] = producer
			factQueue = append(factQueue, producer.Requires...)
		}
	}

	// 2. Assign dense PlanSlots for all involved facts
	defToSlot := make(map[fact.DefinitionID]fact.PlanSlot)
	slotCounter := 0

	assignSlot := func(id fact.DefinitionID) fact.PlanSlot {
		if slot, exists := defToSlot[id]; exists {
			return slot
		}
		slot := fact.PlanSlot(slotCounter)
		defToSlot[id] = slot
		slotCounter++
		return slot
	}

	for _, c := range capSet {
		for _, f := range c.Provides {
			assignSlot(f.DefID)
		}
		for _, f := range c.Requires {
			assignSlot(f.DefID)
		}
	}
	for _, f := range def.Spec.Requires {
		assignSlot(f.DefID)
	}
	if def.InputKey.DefID != 0 {
		assignSlot(def.InputKey.DefID)
	}

	// 3. Construct graph nodes and decoupled runtime executors
	var nodes []*graph.Node
	var runners []execution.NodeExecutor
	var nodeIdx uint32

	// Capabilities
	for _, c := range capSet {
		var reqSlots []fact.PlanSlot
		for _, r := range c.Requires {
			reqSlots = append(reqSlots, defToSlot[r.DefID])
		}

		var provSlots []fact.PlanSlot
		for _, p := range c.Provides {
			provSlots = append(provSlots, defToSlot[p.DefID])
		}

		node := &graph.Node{
			ID:          graph.NodeID(nodeIdx),
			Name:        c.Name,
			Kind:        c.Kind,
			Speculation: c.Speculation,
			Requires:    reqSlots,
			Provides:    provSlots,
		}
		nodes = append(nodes, node)
		runners = append(runners, c.Run)
		nodeIdx++
	}

	// 4. Automatic Input Decoding node (Item 6)
	// PureNode, PreAuthSafe: decodes raw invocation bytes into InputFact<I>
	if def.DecodeNode != nil && def.InputKey.DefID != 0 {
		inputSlot := defToSlot[def.InputKey.DefID]
		decodeNode := &graph.Node{
			ID:          graph.NodeID(nodeIdx),
			Name:        string(def.Name) + ".decode",
			Kind:        graph.PureNode,
			Speculation: graph.PreAuthSafe,
			Provides:    []fact.PlanSlot{inputSlot},
		}
		nodes = append(nodes, decodeNode)
		runners = append(runners, def.DecodeNode)
		nodeIdx++
	}

	// 5. OperationNode: receives already-decoded I and required facts
	var opReqSlots []fact.PlanSlot
	if def.DecodeNode != nil && def.InputKey.DefID != 0 {
		opReqSlots = append(opReqSlots, defToSlot[def.InputKey.DefID])
	}
	for _, r := range def.Spec.Requires {
		opReqSlots = append(opReqSlots, defToSlot[r.DefID])
	}

	opNode := &graph.Node{
		ID:          graph.NodeID(nodeIdx),
		Name:        string(def.Name) + ".operation",
		Kind:        graph.OperationNode,
		Speculation: graph.NoSpeculation,
		Requires:    opReqSlots,
	}
	nodes = append(nodes, opNode)

	runOp := def.Run
	runners = append(runners, func(nc *execution.NodeContext) error {
		val, fx, meta, err := runOp(nc)
		if err != nil {
			return err
		}
		if !fx.IsEmpty() {
			for _, e := range fx.All() {
				nc.RecordEffect(e)
			}
		}
		nc.SetShortCircuitWithMeta(val, meta)
		return nil
	})

	// 6. Build graph
	g, err := graph.Build(nodes, slotCounter)
	if err != nil {
		return nil, err
	}

	// 7. Compiler Speculation Enforcement (Item 8)
	// PostIdentitySafe must transitively depend on identity fact
	// PostPolicySafe must transitively depend on all policy decisions
	if err := e.enforceSpeculationInvariants(g, defToSlot); err != nil {
		return nil, err
	}

	// 8. Compile Plan
	plan, err := graph.Compile(g, string(def.Name), int(def.Version))
	if err != nil {
		return nil, err
	}
	plan.DefToSlot = defToSlot

	// Build flat O(1) lookup array from defToSlot map
	var maxDefID fact.DefinitionID
	for id := range defToSlot {
		if id > maxDefID {
			maxDefID = id
		}
	}
	if maxDefID > 0 {
		defSlots := make([]fact.PlanSlot, maxDefID+1)
		for id, slot := range defToSlot {
			defSlots[id] = slot
		}
		plan.DefSlots = defSlots
		plan.MaxDefID = uint32(maxDefID)
	}

	return &execution.Program{
		Plan:    plan,
		Runners: runners,
	}, nil
}

// enforceSpeculationInvariants proves speculation safety at compile time (Item 8).
func (e *Engine) enforceSpeculationInvariants(g *graph.Graph, defToSlot map[fact.DefinitionID]fact.PlanSlot) error {
	// Identify nodes that provide identity facts (e.g. PrincipalKey)
	var identityNodes []graph.NodeID
	principalSlot, hasPrincipal := defToSlot[capability.PrincipalKey.DefinitionID()]

	for _, n := range g.Nodes {
		if hasPrincipal {
			for _, p := range n.Provides {
				if p == principalSlot {
					identityNodes = append(identityNodes, n.ID)
					break
				}
			}
		}
	}

	// Identify all decision nodes
	var decisionNodes []graph.NodeID
	for _, n := range g.Nodes {
		if n.Kind == graph.DecisionNode {
			decisionNodes = append(decisionNodes, n.ID)
		}
	}

	for _, n := range g.Nodes {
		if n.Speculation == graph.PostIdentitySafe {
			if len(identityNodes) == 0 {
				return fmt.Errorf("ref: node %q is marked PostIdentitySafe but no identity capability is in the plan", n.Name)
			}
			// Must transitively depend on at least one identity node
			hasPath := false
			for _, idNode := range identityNodes {
				if isTransitiveAncestor(g, idNode, n.ID) {
					hasPath = true
					break
				}
			}
			if !hasPath {
				return fmt.Errorf("ref: node %q is marked PostIdentitySafe but has no transitive dependency on an identity node", n.Name)
			}
		}

		if n.Speculation == graph.PostPolicySafe && len(decisionNodes) > 0 {
			// Must transitively depend on all decision nodes
			for _, dn := range decisionNodes {
				if !isTransitiveAncestor(g, dn, n.ID) {
					return fmt.Errorf("ref: node %q is marked PostPolicySafe but does not transitively depend on policy decision node %q",
						n.Name, g.Nodes[dn].Name)
				}
			}
		}
	}

	return nil
}

func isTransitiveAncestor(g *graph.Graph, ancestor, target graph.NodeID) bool {
	visited := make([]bool, len(g.Nodes))
	var dfs func(curr graph.NodeID) bool
	dfs = func(curr graph.NodeID) bool {
		if curr == target {
			return true
		}
		visited[curr] = true
		for _, child := range g.Adjacency[curr] {
			if !visited[child] && dfs(child) {
				return true
			}
		}
		return false
	}

	for _, child := range g.Adjacency[ancestor] {
		if !visited[child] && dfs(child) {
			return true
		}
	}
	return false
}

// Dispatch executes an intent by invocation input.
// Dispatch executes an intent and commits its returned effects.
func (e *Engine) Dispatch(ctx context.Context, inv *invocation.Invocation) (*DispatchResult, error) {
	return e.dispatch(ctx, inv, true)
}

// DispatchPreview evaluates an intent without committing its returned effect plan.
// It refuses plans with explicit effect nodes. Use only for trusted read-only
// intents: Go capabilities are trusted code and cannot be made pure by inspection.
func (e *Engine) DispatchPreview(ctx context.Context, inv *invocation.Invocation) (*DispatchResult, error) {
	if inv == nil {
		return nil, errNilInvocation
	}
	plan, ok := e.Plan(intent.Name(inv.Intent))
	if !ok {
		return nil, intent.Failure{Code: "INTENT_NOT_FOUND", Category: intent.CategoryNotFound, Message: fmt.Sprintf("ref: unknown intent %q", inv.Intent)}
	}
	for _, node := range plan.Nodes {
		if node.Kind == graph.EffectNode || node.Kind == graph.AsyncEffect {
			return nil, fmt.Errorf("ref: preview refused effect-capable intent %q", inv.Intent)
		}
	}
	return e.dispatch(ctx, inv, false)
}

func (e *Engine) dispatch(ctx context.Context, inv *invocation.Invocation, commitEffects bool) (*DispatchResult, error) {
	if inv == nil {
		return nil, errNilInvocation
	}

	itName := intent.Name(inv.Intent)

	var prog *execution.Program
	var def *intent.Definition

	// Lock-free fast path: read from immutable generation
	if gen := e.generation.Load(); gen != nil {
		if entry, ok := gen.entries[itName]; ok {
			prog = entry.prog
			def = entry.def
		}
	}

	if prog == nil {
		// Fallback: try with read lock (pre-compilation or dynamic)
		e.mu.RLock()
		prog = e.programs[itName]
		def = e.intentDefs[itName]
		e.mu.RUnlock()
	}

	if prog == nil {
		// Attempt dynamic compilation if not compiled yet
		e.mu.Lock()
		if !e.compiled {
			if defLooked, found := e.intents.Lookup(itName); found {
				if p, err := e.compileIntent(defLooked); err == nil {
					e.programs[itName] = p
					e.plans[itName] = p.Plan
					e.intentDefs[itName] = defLooked
					prog = p
					def = defLooked
				}
			}
		}
		e.mu.Unlock()

		if prog == nil {
			return nil, intent.Failure{
				Code:     "INTENT_NOT_FOUND",
				Category: intent.CategoryNotFound,
				Message:  fmt.Sprintf("ref: unknown intent %q", inv.Intent),
			}
		}
	}

	var budget *execution.Budget
	if def != nil && (def.Spec.Timeout > 0 || def.Spec.MaxDBQueries > 0 || def.Spec.MaxExternalIO > 0) {
		budget = execution.NewBudget(
			def.Spec.Timeout,
			def.Spec.MaxDBQueries,
			def.Spec.MaxExternalIO,
			def.Spec.MaxMemory,
			def.Spec.MaxEffects,
		)
	}

	outcome, err := e.scheduler.ExecuteProgram(ctx, inv, prog, budget)
	if err != nil {
		return nil, err
	}
	defer execution.ReleaseOutcome(outcome)

	switch outcome.State {
	case execution.StateDenied:
		return nil, intent.Failure{
			Code:     "PERMISSION_DENIED",
			Category: intent.CategoryPermission,
			Message:  "access denied",
		}

	case execution.StateShortCircuited, execution.StateCompleted:
		// If effects were recorded during operation, commit them
		if commitEffects && len(outcome.Effects) > 0 {
			fxList := make([]effect.Effect, 0, len(outcome.Effects))
			for _, itm := range outcome.Effects {
				if ef, ok := itm.(effect.Effect); ok {
					fxList = append(fxList, ef)
				}
			}
			if len(fxList) > 0 {
				bridge := &effectErrorBridge{obs: e.observers}
				if err := effect.CommitPlan(ctx, e.effectRunner.Store(), string(inv.ID), fxList, bridge.report); err != nil {
					return nil, err
				}
			}
		}
		var meta intent.OutcomeMeta
		if outcome.Meta != nil {
			if m, ok := outcome.Meta.(intent.OutcomeMeta); ok {
				meta = m
			}
		}
		return AcquireDispatchResult(outcome.Value, meta), nil

	case execution.StateFailed:
		return nil, outcome.Err
	}

	return AcquireDispatchResult(outcome.Value, intent.OutcomeMeta{}), nil
}

// Plan returns the compiled execution plan for an intent.
func (e *Engine) Plan(name intent.Name) (*graph.Plan, bool) {
	if gen := e.generation.Load(); gen != nil {
		p, ok := gen.plans[name]
		if ok {
			return p, true
		}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	p, ok := e.plans[name]
	return p, ok
}
