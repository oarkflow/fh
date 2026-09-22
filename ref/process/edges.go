package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// Edge resolution: what happens after a step finishes.
//
// One contract holds across every edge type without exception: the guard is
// evaluated before the edge does anything. A fan-out, a manual gate and a
// compensation edge all honour a `when` condition exactly as a simple edge does.
// DAGFlow originally let several types bypass their own conditions, which meant a
// graph's behaviour depended on which edge type an author happened to pick; one
// contract, checked in one place, removes that whole class of surprise.
//
// The second rule: success-path and error-path edges never mix. A failed step
// resolves only error, fallback and compensate edges; a successful one resolves
// only the rest. There is no arrangement of conditions that can run a compensation
// on the happy path.

// resolveOutcome turns one executed step into cursor changes.
func (e *Engine) resolveOutcome(ctx context.Context, definition *Definition, run *Run, outcome stepOutcome) error {
	if outcome.step == nil {
		return e.failRun(ctx, run, outcome.frame.Step, outcome.err)
	}
	if outcome.frame.Compensate {
		return e.afterCompensation(ctx, run, outcome)
	}
	// A parked step contributed no result and no traversal. Its continuation is
	// held by the task, timer or subscription that parked it.
	if outcome.parked != nil {
		return e.recordPark(ctx, run, *outcome.parked)
	}

	if !outcome.skipped {
		run.Steps++
		if run.Visits == nil {
			run.Visits = map[string]int{}
		}
		run.Visits[outcome.frame.Step]++
	}

	if outcome.err != nil {
		return e.resolveFailure(ctx, definition, run, outcome)
	}

	scope, err := e.scope(ctx, run, outcome.frame, nil, outcome.result)
	if err != nil {
		return err
	}
	edges := definition.Outgoing(outcome.frame.Step)
	traversed, err := e.traverse(ctx, definition, run, outcome, edges, scope, false)
	if err != nil {
		return err
	}

	// A step marked terminal ends the run here even if nothing traversed. A step
	// that is not terminal and traversed nothing has reached a dead end the
	// compiler could not see — every edge's condition was false — and saying so
	// beats a run that silently stops.
	if outcome.step.Terminal {
		if encoded, err := json.Marshal(outcome.result); err == nil {
			run.Output = encoded
		}
		return e.completeRun(ctx, definition, run)
	}
	if traversed == 0 && !e.hasRemainingWork(run) {
		return e.failRun(ctx, run, outcome.frame.Step,
			fmt.Errorf("step %q finished but none of its %d outgoing edges' conditions matched, so the run cannot continue",
				outcome.frame.Step, len(edges)))
	}
	return nil
}

func (e *Engine) hasRemainingWork(run *Run) bool { return len(run.Frames) > 0 }

// resolveFailure handles a failed step: retry it, take an error path, or
// compensate and fail.
func (e *Engine) resolveFailure(ctx context.Context, definition *Definition, run *Run, outcome stepOutcome) error {
	step := outcome.step

	// Retry first. A retry is not a traversal: the same frame comes back with a
	// higher attempt number and a delay, so the step's own state row records the
	// attempt count rather than the graph recording a loop.
	policy := step.Retry
	if policy == nil {
		policy = definition.Retry
	}
	if policy != nil && outcome.frame.Attempt < max(policy.MaxAttempts, 1) && retriable(policy, outcome.err) {
		next := outcome.frame
		next.Attempt++
		delay := retryDelay(policy, outcome.frame.Attempt)
		when := e.now().Add(delay)
		next.NotBefore = &when
		run.Frames = append(run.Frames, next)
		if e.enqueuer != nil {
			_ = e.enqueuer.EnqueueAdvanceAt(ctx, run.ID, when)
		}
		return nil
	}

	scope, err := e.scope(ctx, run, outcome.frame, nil, map[string]any{"error": outcome.err.Error()})
	if err != nil {
		return err
	}
	edges := definition.Outgoing(outcome.frame.Step)
	traversed, err := e.traverse(ctx, definition, run, outcome, edges, scope, true)
	if err != nil {
		return err
	}
	if traversed > 0 {
		return nil
	}
	// Nothing caught the failure. Undo what already committed, then fail.
	return e.beginCompensation(ctx, definition, run, outcome.frame.Step, outcome.err)
}

// traverse resolves every eligible edge and returns how many actually traversed.
//
// Eligibility is computed for all edges first, so weighted and priority selection
// choose among edges that really could fire rather than among all of them.
func (e *Engine) traverse(ctx context.Context, definition *Definition, run *Run, outcome stepOutcome, edges []*Edge, scope Scope, errorMode bool) (int, error) {
	eligible := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		if edge.Type.ErrorPath() != errorMode {
			continue
		}
		// An edge whose source is not this step is a fan-in waiting on it; those are
		// handled by the fan-in branch below, which needs to run even though this
		// step is only one of its sources.
		if !slices.Contains(edge.allSources(), outcome.frame.Step) {
			continue
		}
		ok, err := evalGuard(edge.Guard, scope)
		if err != nil {
			return 0, fmt.Errorf("edge %q condition: %w", edge.Name, err)
		}
		if ok {
			eligible = append(eligible, edge)
		}
	}

	// Weighted and priority edges are mutually exclusive alternatives, so exactly
	// one of each group is chosen per resolution — and the draw happens once, so
	// every weighted case in this call agrees on the winner.
	chosenWeighted := pickWeighted(eligible)
	chosenPriority := pickPriority(eligible)

	traversed := 0
	for _, edge := range eligible {
		switch edge.Type {
		case EdgeWeighted:
			if edge != chosenWeighted {
				continue
			}
		case EdgePriority:
			if edge != chosenPriority {
				continue
			}
		}
		fired, err := e.resolveEdge(ctx, definition, run, outcome, edge, scope)
		if err != nil {
			return traversed, err
		}
		if fired {
			traversed++
		}
		if run.Status.Terminal() {
			return traversed, nil
		}
	}
	return traversed, nil
}

// resolveEdge applies one edge's semantics. It reports whether the edge did
// anything — traversed, parked or ended the run.
func (e *Engine) resolveEdge(ctx context.Context, definition *Definition, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	switch edge.Type {

	// --- plain traversal -------------------------------------------------
	case EdgeSimple, EdgeBranch, EdgeSwitch, EdgeFanOut, EdgeParallel, EdgeError, EdgeFallback,
		EdgeTransform, EdgeFilter, EdgeStreamPipe, EdgeRetry, EdgeTimeout:
		payload, err := e.edgePayload(edge, outcome, scope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		// A timeout edge's deadline is enforced by a timer on the target rather
		// than by holding a goroutine: that is what makes it survive a restart.
		if edge.Type == EdgeTimeout {
			if err := e.armTargetTimeout(ctx, run, edge); err != nil {
				return false, err
			}
		}
		e.pushFrames(run, edge, outcome.frame.Step, edge.allTargets(), payload, 1)
		return true, nil

	case EdgeConditionalFork:
		// Every target whose own guard passes fires. The edge-level guard has
		// already been checked; per-target guards are expressed as separate
		// conditional_fork edges sharing a source, which is why this behaves like a
		// fan-out once it is eligible.
		payload, err := e.edgePayload(edge, outcome, scope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		e.pushFrames(run, edge, outcome.frame.Step, edge.allTargets(), payload, 1)
		return true, nil

	case EdgeWeighted, EdgePriority:
		payload, err := e.edgePayload(edge, outcome, scope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		e.pushFrames(run, edge, outcome.frame.Step, edge.allTargets(), payload, 1)
		return true, nil

	case EdgeThreshold:
		return e.resolveThreshold(ctx, run, outcome, edge, scope)

	case EdgeDynamicFanOut:
		targets := e.dynamicTargets(definition, edge, outcome.result)
		if len(targets) == 0 {
			return false, nil
		}
		payload, err := e.edgePayload(edge, outcome, scope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		e.pushFrames(run, edge, outcome.frame.Step, targets, payload, 1)
		return true, nil

	// --- joins -----------------------------------------------------------
	case EdgeFanIn, EdgeJoin, EdgeQuorum:
		return e.resolveJoin(ctx, run, outcome, edge, scope)

	// --- race ------------------------------------------------------------
	case EdgeRace:
		return e.resolveRace(ctx, definition, run, outcome, edge, scope)

	// --- iteration -------------------------------------------------------
	case EdgeIterator, EdgeBatchIterator:
		return e.resolveIterator(ctx, run, outcome, edge, scope)

	case EdgeLoopUntil:
		return e.resolveLoopUntil(ctx, run, outcome, edge, scope)

	// --- suspension ------------------------------------------------------
	case EdgeDelayed:
		payload, err := e.edgePayload(edge, outcome, scope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		return true, e.parkOnTimer(ctx, run, edge, outcome.frame.Step, payload, e.now().Add(edge.Timeout), "delayed")

	case EdgeWaitEvent:
		return e.resolveWaitEvent(ctx, run, outcome, edge, scope)

	case EdgeManual:
		return true, e.parkManual(ctx, run, edge, outcome)

	case EdgeEscalation:
		return true, e.parkEscalation(ctx, run, edge, outcome)

	case EdgeRateLimited:
		return e.resolveRateLimited(ctx, run, outcome, edge, scope)

	// --- termination -----------------------------------------------------
	case EdgeCancel:
		now := e.now()
		run.Status = StatusCancelled
		run.CompletedAt = &now
		run.Frames = nil
		run.Waiting = nil
		if run.Error == "" {
			run.Error = fmt.Sprintf("cancelled after %s by edge %s", outcome.frame.Step, edge.Name)
		}
		if encoded, err := json.Marshal(outcome.result); err == nil {
			run.Output = encoded
		}
		_ = e.store.DeleteRunTimers(ctx, run.ID)
		_ = e.store.DeleteRunSubscriptions(ctx, run.ID)
		e.cancelRunTasks(ctx, run)
		for _, observer := range e.observers {
			observer.RunFinished(run)
		}
		return true, nil

	case EdgeCompensate:
		return true, e.beginCompensation(ctx, definition, run, outcome.frame.Step,
			fmt.Errorf("compensating after %s", outcome.frame.Step))

	default:
		return false, fmt.Errorf("edge %q has unhandled type %q", edge.Name, edge.Type)
	}
}

// edgePayload shapes what the targets receive. A filter inside the edge's data
// block returns ErrFiltered, which is how a filter edge declines to traverse.
func (e *Engine) edgePayload(edge *Edge, outcome stepOutcome, scope Scope) (any, error) {
	payload := outcome.result
	if edge.Shaper == nil {
		return payload, nil
	}
	return edge.Shaper.Shape(payload, scope)
}

// pushFrames appends work for each target.
func (e *Engine) pushFrames(run *Run, edge *Edge, from string, targets []string, payload any, attempt int) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		// A payload that cannot be serialised cannot cross a durability boundary.
		// Passing nothing is better than corrupting the cursor, and the target
		// step's own validation will report the missing input.
		encoded = nil
	}
	for _, target := range targets {
		run.Frames = append(run.Frames, Frame{
			Step: target, Input: encoded, From: from, Edge: edge.Name, Attempt: attempt,
		})
	}
}

// ---------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------

// pickWeighted performs one weighted-random draw across the eligible weighted
// edges. One draw per resolution, so all weighted cases agree.
func pickWeighted(edges []*Edge) *Edge {
	var (
		group []*Edge
		total float64
	)
	for _, edge := range edges {
		if edge.Type != EdgeWeighted {
			continue
		}
		weight := edge.Weight
		if weight <= 0 {
			weight = 1
		}
		group = append(group, edge)
		total += weight
	}
	if len(group) == 0 {
		return nil
	}
	if total <= 0 {
		return group[0]
	}
	draw := rand.Float64() * total
	running := 0.0
	for _, edge := range group {
		weight := edge.Weight
		if weight <= 0 {
			weight = 1
		}
		running += weight
		if draw <= running {
			return edge
		}
	}
	return group[len(group)-1]
}

// pickPriority selects the lowest priority value, ties going to the earlier
// declaration so the choice is deterministic.
func pickPriority(edges []*Edge) *Edge {
	var best *Edge
	for _, edge := range edges {
		if edge.Type != EdgePriority {
			continue
		}
		if best == nil || edge.Priority < best.Priority {
			best = edge
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

func (e *Engine) resolveThreshold(ctx context.Context, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	for _, band := range edge.Thresholds {
		if band.Value == nil {
			continue
		}
		raw, err := band.Value.Value(scope)
		if err != nil {
			return false, fmt.Errorf("edge %q threshold %q: %w", edge.Name, band.Name, err)
		}
		number, ok := toFloat(raw)
		if !ok {
			return false, fmt.Errorf("edge %q threshold %q: %v is not a number", edge.Name, band.Name, raw)
		}
		if !band.Contains(number) {
			continue
		}
		payload, err := e.edgePayload(edge, outcome, scope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		// The band that matched is carried forward, so an auditor can see which
		// rule applied rather than inferring it from the path taken.
		if object, ok := payload.(map[string]any); ok {
			enriched := make(map[string]any, len(object)+len(band.Data)+2)
			for key, value := range object {
				enriched[key] = value
			}
			for key, value := range band.Data {
				enriched[key] = value
			}
			enriched["threshold"] = band.Name
			if band.Reason != "" {
				enriched["threshold_reason"] = band.Reason
			}
			payload = enriched
		}
		e.pushFrames(run, edge, outcome.frame.Step, []string{band.Target}, payload, 1)
		return true, nil
	}
	// No band matched. That is a gap in the configuration rather than a decision,
	// and it is worth reporting: a threshold edge whose bands do not tile the
	// range will silently stall runs at whatever value falls through.
	return false, nil
}

// dynamicTargets reads the runtime target list, falling back to the static ones.
func (e *Engine) dynamicTargets(definition *Definition, edge *Edge, result any) []string {
	var candidates []string
	if edge.TargetsPath != "" {
		if object, ok := result.(map[string]any); ok {
			if value, found := lookup(object, edge.TargetsPath); found {
				candidates = toStrings(value)
			}
		}
	}
	if len(candidates) == 0 {
		return edge.allTargets()
	}
	// Only declared steps are accepted. Without this check a step's result could
	// name any step in the process — or a step that does not exist — and dispatch
	// to it, which is a control-flow injection.
	valid := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, ok := definition.Step(candidate); ok {
			valid = append(valid, candidate)
		}
	}
	return valid
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

// resolveJoin accumulates one source's completion and continues when the
// strategy is satisfied.
func (e *Engine) resolveJoin(ctx context.Context, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	join, err := e.store.GetJoin(ctx, run.ID, edge.Name)
	if err != nil {
		return false, err
	}
	if join == nil {
		join = &Join{
			RunID:   run.ID,
			Edge:    edge.Name,
			Sources: slices.Clone(edge.Sources),
			Results: map[string]json.RawMessage{},
			Errors:  map[string]string{},
		}
	}
	if join.Emitted {
		// A late source arriving after the join already continued must not fire the
		// downstream step a second time.
		return false, nil
	}
	if join.Results == nil {
		join.Results = map[string]json.RawMessage{}
	}
	if join.Errors == nil {
		join.Errors = map[string]string{}
	}

	if outcome.err != nil {
		join.Errors[outcome.frame.Step] = outcome.err.Error()
	} else if encoded, err := json.Marshal(outcome.result); err == nil {
		join.Results[outcome.frame.Step] = encoded
	}

	if !join.Complete(edge.Strategy, edge.Quorum) {
		return true, e.store.SaveJoin(ctx, join)
	}

	join.Emitted = true
	if err := e.store.SaveJoin(ctx, join); err != nil {
		return false, err
	}

	// The targets receive every source's result keyed by source step, plus the
	// errors, so a partial_success join can decide what to do about the gaps.
	results := make(map[string]any, len(join.Results))
	for source, encoded := range join.Results {
		var value any
		if json.Unmarshal(encoded, &value) == nil {
			results[source] = value
		}
	}
	payload := map[string]any{
		"results":  results,
		"sources":  join.Sources,
		"strategy": edge.Strategy,
		"errors":   join.Errors,
	}
	if edge.Shaper != nil {
		joinScope := make(Scope, len(scope)+1)
		for key, value := range scope {
			joinScope[key] = value
		}
		joinScope["result"] = payload
		shaped, err := edge.Shaper.Shape(payload, joinScope)
		if err != nil {
			if errors.Is(err, ErrFiltered) {
				return false, nil
			}
			return false, err
		}
		e.pushFrames(run, edge, outcome.frame.Step, edge.allTargets(), shaped, 1)
		return true, nil
	}
	e.pushFrames(run, edge, outcome.frame.Step, edge.allTargets(), payload, 1)
	return true, nil
}

// resolveRace pushes every target and records that the first to finish wins.
//
// The race is decided by a join row with an "any" strategy on the far side, which
// is how it survives a restart: a durable race cannot be a goroutine that returns
// first. The losers still execute; CancelLosers only suppresses their
// continuations, because a step that has already started cannot be unstarted —
// only its downstream work can be dropped.
func (e *Engine) resolveRace(ctx context.Context, definition *Definition, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	payload, err := e.edgePayload(edge, outcome, scope)
	if err != nil {
		if errors.Is(err, ErrFiltered) {
			return false, nil
		}
		return false, err
	}
	join := &Join{
		RunID:   run.ID,
		Edge:    edge.Name + ":race",
		Sources: edge.allTargets(),
		Results: map[string]json.RawMessage{},
		Errors:  map[string]string{},
	}
	if existing, err := e.store.GetJoin(ctx, run.ID, join.Edge); err == nil && existing != nil {
		join = existing
	}
	if err := e.store.SaveJoin(ctx, join); err != nil {
		return false, err
	}
	if edge.Timeout > 0 {
		if err := e.store.AddTimer(ctx, &Timer{
			ID: randomID(), RunID: run.ID, Fire: e.now().Add(edge.Timeout),
			Kind: "timeout", Step: outcome.frame.Step, Edge: edge.Name,
			OnFire: edge.OnTimeout, CreatedAt: e.now(),
		}); err != nil {
			return false, err
		}
	}
	e.pushFrames(run, edge, outcome.frame.Step, edge.allTargets(), payload, 1)
	return true, nil
}

// ---------------------------------------------------------------------------
// Iteration
// ---------------------------------------------------------------------------

// resolveIterator pushes one frame per element or per batch.
//
// Each iteration gets its own state key, so the twelfth item's failure is
// recorded against `notify[11]` rather than overwriting the eleventh's record —
// which is what makes an iterator's partial failure diagnosable.
func (e *Engine) resolveIterator(ctx context.Context, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	payload, err := e.edgePayload(edge, outcome, scope)
	if err != nil {
		if errors.Is(err, ErrFiltered) {
			return false, nil
		}
		return false, err
	}
	items := e.iteratorItems(edge, payload)
	if len(items) == 0 {
		// An empty collection is not a failure; it is a loop that runs zero times.
		// The run continues past the iterator through whatever the target's own
		// edges would have led to, which the author expresses with a separate edge
		// from the source.
		return false, nil
	}

	targets := edge.allTargets()
	target := targets[0]
	if edge.Type == EdgeBatchIterator {
		batches := make([]any, 0, len(items)/max(edge.BatchSize, 1)+1)
		for start := 0; start < len(items); start += edge.BatchSize {
			end := min(start+edge.BatchSize, len(items))
			batches = append(batches, items[start:end])
		}
		items = batches
	}

	for index, item := range items {
		encoded, err := json.Marshal(map[string]any{
			"item":  item,
			"index": index,
			"total": len(items),
		})
		if err != nil {
			return false, err
		}
		run.Frames = append(run.Frames, Frame{
			Step:    target,
			Input:   encoded,
			From:    outcome.frame.Step,
			Edge:    edge.Name,
			Key:     fmt.Sprintf("%s[%d]", target, index),
			Attempt: 1,
		})
	}
	return true, nil
}

func (e *Engine) iteratorItems(edge *Edge, payload any) []any {
	value := payload
	if edge.ItemsPath != "" {
		if object, ok := payload.(map[string]any); ok {
			found, ok := lookup(object, edge.ItemsPath)
			if !ok {
				return nil
			}
			value = found
		}
	}
	switch typed := value.(type) {
	case []any:
		return typed
	case []map[string]any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = item
		}
		return out
	default:
		return nil
	}
}

// resolveLoopUntil sends the run back to the target until the condition holds or
// the cycle cap is reached.
func (e *Engine) resolveLoopUntil(ctx context.Context, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	targets := edge.allTargets()
	if len(targets) == 0 {
		return false, nil
	}
	target := targets[0]

	// The guard on a loop_until edge is the *exit* condition: it has already been
	// evaluated as the edge's eligibility, so reaching here means the loop should
	// continue. The visit cap is the backstop for a condition that never holds.
	if run.Visits[target] >= edge.MaxConcurrency {
		return false, fmt.Errorf("edge %q looped back to %q %d times without its condition holding",
			edge.Name, target, run.Visits[target])
	}
	payload, err := e.edgePayload(edge, outcome, scope)
	if err != nil {
		if errors.Is(err, ErrFiltered) {
			return false, nil
		}
		return false, err
	}
	e.pushFrames(run, edge, outcome.frame.Step, []string{target}, payload, 1)
	return true, nil
}

// ---------------------------------------------------------------------------
// Suspension
// ---------------------------------------------------------------------------

// parkOnTimer records a timer that will resume the run at the target.
func (e *Engine) parkOnTimer(ctx context.Context, run *Run, edge *Edge, from string, payload any, when time.Time, kind string) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	targets := edge.allTargets()
	if len(targets) == 0 {
		return fmt.Errorf("edge %q has no target to resume at", edge.Name)
	}
	if err := e.store.AddTimer(ctx, &Timer{
		ID: randomID(), RunID: run.ID, Fire: when, Kind: kind,
		Step: targets[0], Edge: edge.Name, Payload: encoded, CreatedAt: e.now(),
	}); err != nil {
		return err
	}
	if e.enqueuer != nil {
		_ = e.enqueuer.EnqueueAdvanceAt(ctx, run.ID, when)
	}
	return e.recordPark(ctx, run, WaitState{
		Reason: "timer", Step: targets[0], Edge: edge.Name, Until: &when,
		Detail: fmt.Sprintf("waiting until %s", when.Format(time.RFC3339)),
	})
}

// armTargetTimeout records the deadline a timeout edge imposes on its target.
func (e *Engine) armTargetTimeout(ctx context.Context, run *Run, edge *Edge) error {
	when := e.now().Add(edge.Timeout)
	if err := e.store.AddTimer(ctx, &Timer{
		ID: randomID(), RunID: run.ID, Fire: when, Kind: "timeout",
		Step: edge.To, Edge: edge.Name, OnFire: edge.OnTimeout, CreatedAt: e.now(),
	}); err != nil {
		return err
	}
	if e.enqueuer != nil {
		_ = e.enqueuer.EnqueueAdvanceAt(ctx, run.ID, when)
	}
	return nil
}

// resolveWaitEvent records a subscription and, when the edge has a timeout, the
// deadline that gives up on it.
func (e *Engine) resolveWaitEvent(ctx context.Context, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	correlation, err := edge.Correlation.Value(scope)
	if err != nil {
		return false, fmt.Errorf("edge %q correlation: %w", edge.Name, err)
	}
	key := fmt.Sprint(correlation)
	if key == "" {
		// An empty correlation would make this subscription match every event of
		// its name, waking runs that have nothing to do with the signal.
		return false, fmt.Errorf("edge %q: the correlation expression %q evaluated to empty", edge.Name, edge.Correlation.Source())
	}
	targets := edge.allTargets()
	if len(targets) == 0 {
		return false, fmt.Errorf("edge %q has no target to resume at", edge.Name)
	}

	var expires *time.Time
	if edge.Timeout > 0 {
		deadline := e.now().Add(edge.Timeout)
		expires = &deadline
		if err := e.store.AddTimer(ctx, &Timer{
			ID: randomID(), RunID: run.ID, Fire: deadline, Kind: "wait_event",
			Step: targets[0], Edge: edge.Name, OnFire: edge.OnTimeout, CreatedAt: e.now(),
		}); err != nil {
			return false, err
		}
		if e.enqueuer != nil {
			_ = e.enqueuer.EnqueueAdvanceAt(ctx, run.ID, deadline)
		}
	}
	if err := e.store.Subscribe(ctx, &Subscription{
		ID: randomID(), RunID: run.ID, Event: edge.Event, Correlation: key,
		Step: targets[0], Edge: edge.Name, ExpiresAt: expires, CreatedAt: e.now(),
	}); err != nil {
		return false, err
	}
	return true, e.recordPark(ctx, run, WaitState{
		Reason: "event", Step: targets[0], Edge: edge.Name, Event: edge.Event, Until: expires,
		Detail: fmt.Sprintf("waiting for %s (%s)", edge.Event, key),
	})
}

// parkManual holds the run until an operator advances it. Nothing automatic ever
// releases a manual gate — that is its entire purpose.
func (e *Engine) parkManual(ctx context.Context, run *Run, edge *Edge, outcome stepOutcome) error {
	targets := edge.allTargets()
	if len(targets) == 0 {
		return fmt.Errorf("edge %q has no target to resume at", edge.Name)
	}
	payload, _ := json.Marshal(outcome.result)
	if err := e.store.Subscribe(ctx, &Subscription{
		ID: randomID(), RunID: run.ID, Event: manualEventName, Correlation: edge.Name,
		Step: targets[0], Edge: edge.Name, CreatedAt: e.now(),
	}); err != nil {
		return err
	}
	// The payload rides on a timer-shaped record with no fire time so the resumed
	// frame carries the source step's result; the subscription alone would lose it.
	if err := e.store.AddTimer(ctx, &Timer{
		ID: randomID(), RunID: run.ID, Fire: farFuture, Kind: "manual_payload",
		Step: targets[0], Edge: edge.Name, Payload: payload, CreatedAt: e.now(),
	}); err != nil {
		return err
	}
	return e.recordPark(ctx, run, WaitState{
		Reason: "manual", Step: targets[0], Edge: edge.Name,
		Detail: fmt.Sprintf("waiting for an operator to release the gate after %s", outcome.frame.Step),
	})
}

// parkEscalation waits, then raises the work to somebody else.
func (e *Engine) parkEscalation(ctx context.Context, run *Run, edge *Edge, outcome stepOutcome) error {
	when := e.now().Add(edge.Timeout)
	payload, _ := json.Marshal(outcome.result)
	targets := edge.allTargets()
	if len(targets) == 0 {
		return fmt.Errorf("edge %q has no target to escalate to", edge.Name)
	}
	if err := e.store.AddTimer(ctx, &Timer{
		ID: randomID(), RunID: run.ID, Fire: when, Kind: "escalation",
		Step: targets[0], Edge: edge.Name, Payload: payload, CreatedAt: e.now(),
	}); err != nil {
		return err
	}
	if e.enqueuer != nil {
		_ = e.enqueuer.EnqueueAdvanceAt(ctx, run.ID, when)
	}
	return e.recordPark(ctx, run, WaitState{
		Reason: "timer", Step: targets[0], Edge: edge.Name, Until: &when,
		Detail: "waiting before escalating",
	})
}

// resolveRateLimited parks until the limiter admits the run, rather than sleeping
// a worker. DAGFlow slept a fraction of the window in the resolver; a durable
// engine can simply come back later, which costs nothing and survives a restart.
func (e *Engine) resolveRateLimited(ctx context.Context, run *Run, outcome stepOutcome, edge *Edge, scope Scope) (bool, error) {
	payload, err := e.edgePayload(edge, outcome, scope)
	if err != nil {
		if errors.Is(err, ErrFiltered) {
			return false, nil
		}
		return false, err
	}
	targets := edge.allTargets()
	if len(targets) == 0 {
		return false, fmt.Errorf("edge %q has no target", edge.Name)
	}
	if e.limiter == nil {
		// Without a limiter the edge cannot honour its contract. Traversing anyway
		// would silently remove the limit the author asked for.
		return false, fmt.Errorf("edge %q is rate_limited but this engine has no rate limiter configured", edge.Name)
	}
	allowed, _, resetAt, err := e.limiter.Allow(ctx, "edge:"+edge.Name, edge.Limit, edge.Window)
	if err != nil {
		return false, fmt.Errorf("edge %q: the rate limiter is unavailable: %w", edge.Name, err)
	}
	if allowed {
		e.pushFrames(run, edge, outcome.frame.Step, targets, payload, 1)
		return true, nil
	}
	if resetAt.IsZero() {
		resetAt = e.now().Add(edge.Window)
	}
	return true, e.parkOnTimer(ctx, run, edge, outcome.frame.Step, payload, resetAt, "delayed")
}

// recordPark notes why the run is waiting. It does not itself set the status:
// a run with other frames still in flight keeps running, and only settle decides
// that the run as a whole is parked.
func (e *Engine) recordPark(ctx context.Context, run *Run, wait WaitState) error {
	run.Waiting = &wait
	return nil
}

// manualEventName is the reserved event a manual gate waits for. It is namespaced
// so an application cannot collide with it.
const manualEventName = "__manual__"

// farFuture is the fire time of a record that is not really a timer — a manual
// gate's payload carrier, which must never fire on its own.
var farFuture = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// Retry helpers
// ---------------------------------------------------------------------------

func retriable(policy *RetryPolicy, err error) bool {
	if policy.Retriable != nil {
		return policy.Retriable(err)
	}
	// Without a classifier from the host, retry everything except a cancelled
	// context — which cannot succeed on another attempt.
	return !errors.Is(err, context.Canceled)
}

// retryDelay computes the wait before an attempt, capped and optionally jittered.
func retryDelay(policy *RetryPolicy, attempt int) time.Duration {
	initial := policy.InitialDelay
	if initial <= 0 {
		initial = 250 * time.Millisecond
	}
	maxDelay := policy.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	var wait time.Duration
	switch policy.Strategy {
	case "fixed":
		wait = initial
	case "linear":
		wait = time.Duration(attempt) * initial
	default:
		wait = initial << min(attempt-1, 16)
	}
	if wait > maxDelay || wait <= 0 {
		wait = maxDelay
	}
	if policy.Jitter || policy.Strategy == "exponential_jitter" || policy.Strategy == "decorrelated_jitter" {
		half := wait / 2
		wait = half + time.Duration(rand.Int64N(int64(half)+1))
	}
	return wait
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

func evalGuard(guard Guard, scope Scope) (bool, error) {
	if guard == nil {
		return true, nil
	}
	return guard.Eval(scope)
}

func lookup(root map[string]any, path string) (any, bool) {
	var current any = root
	for _, segment := range splitPath(path) {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, found := object[segment]
		if !found {
			return nil, false
		}
		current = next
	}
	return current, true
}

func splitPath(path string) []string {
	if path == "" {
		return nil
	}
	segments := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			segments = append(segments, path[start:i])
			start = i + 1
		}
	}
	return append(segments, path[start:])
}

func toStrings(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok && text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		if typed == "" {
			return nil
		}
		return []string{typed}
	default:
		return nil
	}
}

func toFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		number, err := typed.Float64()
		return number, err == nil
	case string:
		var number float64
		if _, err := fmt.Sscanf(typed, "%g", &number); err == nil {
			return number, true
		}
		return 0, false
	default:
		return 0, false
	}
}
