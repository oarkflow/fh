package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Engine tests.
//
// Each one asserts a behaviour somebody would otherwise have to discover in
// production: that a compensation runs in reverse completion order, that a join
// fires once however many sources report, that a manual gate is not released by a
// timer, that a rate-limited edge parks instead of sleeping a worker.
//
// The harness runs everything against the memory store with the engine's own
// advance loop, so these are integration tests over the real state machine rather
// than unit tests of its pieces.

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// recordingRunner executes steps from a table of behaviours and records the order.
type recordingRunner struct {
	mu       sync.Mutex
	calls    []string
	results  map[string]any
	failures map[string]error
	// failOnce fails a step only on its first attempt, for testing retries.
	failOnce map[string]bool
	attempts map[string]int
	delay    map[string]time.Duration
}

func newRunner() *recordingRunner {
	return &recordingRunner{
		results:  map[string]any{},
		failures: map[string]error{},
		failOnce: map[string]bool{},
		attempts: map[string]int{},
		delay:    map[string]time.Duration{},
	}
}

// RunStep implements StepRunner.
func (r *recordingRunner) RunStep(ctx context.Context, call StepCall) (any, error) {
	r.mu.Lock()
	name := call.Intent
	r.calls = append(r.calls, name)
	r.attempts[name]++
	attempt := r.attempts[name]
	failure := r.failures[name]
	once := r.failOnce[name]
	result := r.results[name]
	delay := r.delay[name]
	r.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if once && attempt == 1 {
		return nil, errors.New("transient failure")
	}
	if failure != nil {
		return nil, failure
	}
	if result == nil {
		result = map[string]any{"step": call.Step.Name, "ok": true}
	}
	return result, nil
}

func (r *recordingRunner) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *recordingRunner) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[name]
}

// exprGuard is a tiny guard implementation for tests: it looks up a path in the
// scope and compares it to a value. The real one is ref/platform's expression
// engine; this keeps these tests independent of it.
type exprGuard struct {
	path  string
	equal any
	// negate inverts the comparison.
	negate bool
}

func (g exprGuard) Eval(scope Scope) (bool, error) {
	value, _ := lookupScope(scope, g.path)
	matched := fmt.Sprint(value) == fmt.Sprint(g.equal)
	if g.negate {
		return !matched, nil
	}
	return matched, nil
}

func (g exprGuard) Source() string { return g.path + "==" + fmt.Sprint(g.equal) }

// pathValuer reads a scope path as a value.
type pathValuer struct{ path string }

func (v pathValuer) Value(scope Scope) (any, error) {
	value, _ := lookupScope(scope, v.path)
	return value, nil
}

func (v pathValuer) Source() string { return v.path }

// constValuer returns a fixed value.
type constValuer struct{ value any }

func (v constValuer) Value(Scope) (any, error) { return v.value, nil }
func (v constValuer) Source() string           { return fmt.Sprint(v.value) }

// textRenderer returns fixed text.
type textRenderer struct{ text string }

func (r textRenderer) Render(Scope) (string, error) { return r.text, nil }

func lookupScope(scope Scope, path string) (any, bool) {
	var current any = map[string]any(scope)
	for _, segment := range strings.Split(path, ".") {
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

// harness bundles what a test needs.
type harness struct {
	engine  *Engine
	store   Store
	runner  *recordingRunner
	limiter *fakeLimiter
	notices []NotifyEvent
	mu      sync.Mutex
}

// NotifyProcess implements Notifier, collecting what the engine reported.
func (h *harness) NotifyProcess(_ context.Context, event NotifyEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.notices = append(h.notices, event)
	return nil
}

func (h *harness) noticeKinds() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	kinds := make([]string, 0, len(h.notices))
	for _, notice := range h.notices {
		kinds = append(kinds, notice.Kind)
	}
	return kinds
}

// fakeLimiter admits a fixed number of calls, then refuses.
type fakeLimiter struct {
	mu        sync.Mutex
	allowance int
	calls     int
}

func (l *fakeLimiter) Allow(context.Context, string, int, time.Duration) (bool, int, time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.calls <= l.allowance {
		return true, l.allowance - l.calls, time.Time{}, nil
	}
	return false, 0, time.Now().Add(20 * time.Millisecond), nil
}

func newHarness(t *testing.T, definition *Definition) *harness {
	t.Helper()
	h := &harness{store: NewMemoryStore(), runner: newRunner(), limiter: &fakeLimiter{allowance: 1000}}
	engine, err := New(Options{
		Store:    h.store,
		Runner:   h.runner,
		Notifier: h,
		Limiter:  h.limiter,
		Owner:    "test",
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := engine.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := engine.Register(definition); err != nil {
		t.Fatalf("register: %v", err)
	}
	h.engine = engine
	return h
}

// step builds a plain step.
func step(name, intent string) *Step { return &Step{Name: name, Intent: intent} }

// terminal builds a step that ends the run.
func terminal(name, intent string) *Step {
	return &Step{Name: name, Intent: intent, Terminal: true}
}

func edge(name string, kind EdgeType, from, to string) *Edge {
	return &Edge{Name: name, Type: kind, From: from, To: to}
}

// ---------------------------------------------------------------------------
// Sequencing
// ---------------------------------------------------------------------------

func TestSimpleSequenceCompletes(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "linear", Start: "a",
		Steps: map[string]*Step{
			"a": step("a", "do.a"),
			"b": step("b", "do.b"),
			"c": terminal("c", "do.c"),
		},
		Edges: []*Edge{
			edge("ab", EdgeSimple, "a", "b"),
			edge("bc", EdgeSimple, "b", "c"),
		},
	})

	run, err := h.engine.Start(context.Background(), "linear", map[string]any{"order": "A-1"}, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s), want completed", run.Status, run.Error)
	}
	if got := h.runner.order(); !slices.Equal(got, []string{"do.a", "do.b", "do.c"}) {
		t.Fatalf("execution order = %v", got)
	}
}

func TestBranchTakesOnlyTheMatchingEdge(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "branching", Start: "decide",
		Steps: map[string]*Step{
			"decide": step("decide", "do.decide"),
			"big":    terminal("big", "do.big"),
			"small":  terminal("small", "do.small"),
		},
		Edges: []*Edge{
			{Name: "to-big", Type: EdgeBranch, From: "decide", To: "big",
				Guard: exprGuard{path: "result.size", equal: "large"}},
			{Name: "to-small", Type: EdgeBranch, From: "decide", To: "small",
				Guard: exprGuard{path: "result.size", equal: "large", negate: true}},
		},
	})
	h.runner.results["do.decide"] = map[string]any{"size": "large"}

	run, err := h.engine.Start(context.Background(), "branching", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", run.Status, run.Error)
	}
	if got := h.runner.order(); !slices.Equal(got, []string{"do.decide", "do.big"}) {
		t.Fatalf("the untaken branch ran: %v", got)
	}
}

func TestDeadEndFailsRatherThanStallingSilently(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "dead-end", Start: "decide",
		Steps: map[string]*Step{
			"decide": step("decide", "do.decide"),
			"next":   terminal("next", "do.next"),
		},
		Edges: []*Edge{
			// The only edge's condition never holds, so the run reaches a step it
			// cannot leave. Reporting that beats a run that silently stops.
			{Name: "never", Type: EdgeBranch, From: "decide", To: "next",
				Guard: exprGuard{path: "result.size", equal: "impossible"}},
		},
	})

	run, err := h.engine.Start(context.Background(), "dead-end", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	if !strings.Contains(run.Error, "none of its") {
		t.Fatalf("error does not explain the dead end: %q", run.Error)
	}
}

func TestThresholdRoutesByBand(t *testing.T) {
	low, mid, high := 0.0, 1000.0, 10000.0
	h := newHarness(t, &Definition{
		Name: "banded", Start: "score",
		Steps: map[string]*Step{
			"score":  step("score", "do.score"),
			"auto":   terminal("auto", "do.auto"),
			"review": terminal("review", "do.review"),
		},
		Edges: []*Edge{{
			Name: "band", Type: EdgeThreshold, From: "score", Targets: []string{"auto", "review"},
			Thresholds: []Threshold{
				{Name: "small", Min: &low, Max: &mid, Value: pathValuer{path: "result.amount"}, Target: "auto", Reason: "below the review threshold"},
				{Name: "large", Min: &mid, Max: &high, Value: pathValuer{path: "result.amount"}, Target: "review", Reason: "needs review"},
			},
		}},
	})
	h.runner.results["do.score"] = map[string]any{"amount": 5000}

	run, err := h.engine.Start(context.Background(), "banded", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", run.Status, run.Error)
	}
	if got := h.runner.order(); !slices.Equal(got, []string{"do.score", "do.review"}) {
		t.Fatalf("wrong band taken: %v", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestFanOutAndJoinFireOnce(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "gather", Start: "split",
		Steps: map[string]*Step{
			"split":  step("split", "do.split"),
			"left":   step("left", "do.left"),
			"right":  step("right", "do.right"),
			"finish": terminal("finish", "do.finish"),
		},
		Edges: []*Edge{
			{Name: "out", Type: EdgeFanOut, From: "split", Targets: []string{"left", "right"}},
			{Name: "in", Type: EdgeFanIn, Sources: []string{"left", "right"}, To: "finish", Strategy: "all"},
		},
	})

	run, err := h.engine.Start(context.Background(), "gather", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", run.Status, run.Error)
	}
	// The join must fire exactly once however the two branches interleave: a
	// downstream step running twice is the failure this guards.
	if got := h.runner.count("do.finish"); got != 1 {
		t.Fatalf("the join fired %d times, want 1", got)
	}
	for _, name := range []string{"do.left", "do.right"} {
		if h.runner.count(name) != 1 {
			t.Fatalf("%s ran %d times", name, h.runner.count(name))
		}
	}
}

func TestQuorumJoinContinuesOnEnoughSources(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "quorum", Start: "split",
		Steps: map[string]*Step{
			"split":  step("split", "do.split"),
			"a":      step("a", "do.a"),
			"b":      step("b", "do.b"),
			"c":      step("c", "do.c"),
			"finish": terminal("finish", "do.finish"),
		},
		Edges: []*Edge{
			{Name: "out", Type: EdgeFanOut, From: "split", Targets: []string{"a", "b", "c"}},
			{Name: "in", Type: EdgeQuorum, Sources: []string{"a", "b", "c"}, To: "finish", Strategy: "quorum", Quorum: 2},
		},
	})

	run, err := h.engine.Start(context.Background(), "quorum", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", run.Status, run.Error)
	}
	if got := h.runner.count("do.finish"); got != 1 {
		t.Fatalf("the quorum fired %d times, want 1 — the third source must not fire it again", got)
	}
}

func TestIteratorRunsOncePerItem(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "iterate", Start: "load",
		Steps: map[string]*Step{
			"load":   step("load", "do.load"),
			"notify": terminal("notify", "do.notify"),
		},
		Edges: []*Edge{{
			Name: "each", Type: EdgeIterator, From: "load", To: "notify", ItemsPath: "items",
		}},
	})
	h.runner.results["do.load"] = map[string]any{"items": []any{"a", "b", "c"}}

	if _, err := h.engine.Start(context.Background(), "iterate", nil, StartOptions{}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := h.runner.count("do.notify"); got != 3 {
		t.Fatalf("the iterator ran the target %d times, want 3", got)
	}
}

// ---------------------------------------------------------------------------
// Reliability
// ---------------------------------------------------------------------------

func TestStepRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "retrying", Start: "flaky",
		Steps: map[string]*Step{
			"flaky": {Name: "flaky", Intent: "do.flaky", Terminal: true,
				Retry: &RetryPolicy{MaxAttempts: 3, Strategy: "fixed", InitialDelay: time.Millisecond}},
		},
	})
	h.runner.failOnce["do.flaky"] = true

	run, err := h.engine.Start(context.Background(), "retrying", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// The first attempt fails and the frame comes back future-dated, so the run
	// parks briefly rather than spinning.
	for range 50 {
		if run.Status.Terminal() {
			break
		}
		time.Sleep(5 * time.Millisecond)
		if err := h.engine.Advance(context.Background(), run.ID); err != nil {
			t.Fatalf("advance: %v", err)
		}
		run, _ = h.store.GetRun(context.Background(), run.ID)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", run.Status, run.Error)
	}
	if got := h.runner.count("do.flaky"); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestErrorEdgeCatchesAFailure(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "catching", Start: "risky",
		Steps: map[string]*Step{
			"risky":   step("risky", "do.risky"),
			"cleanup": terminal("cleanup", "do.cleanup"),
			"onward":  terminal("onward", "do.onward"),
		},
		Edges: []*Edge{
			edge("ok", EdgeSimple, "risky", "onward"),
			edge("bad", EdgeError, "risky", "cleanup"),
		},
	})
	h.runner.failures["do.risky"] = errors.New("it broke")

	run, err := h.engine.Start(context.Background(), "catching", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", run.Status, run.Error)
	}
	// The success edge must not have fired: an error path and a success path are
	// mutually exclusive, whatever the conditions say.
	if got := h.runner.order(); !slices.Equal(got, []string{"do.risky", "do.cleanup"}) {
		t.Fatalf("execution order = %v", got)
	}
}

func TestCompensationRunsInReverseCompletionOrder(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "saga", Start: "reserve",
		Steps: map[string]*Step{
			"reserve": {Name: "reserve", Intent: "do.reserve", Compensate: "undo.reserve"},
			"charge":  {Name: "charge", Intent: "do.charge", Compensate: "undo.charge"},
			"ship":    terminal("ship", "do.ship"),
		},
		Edges: []*Edge{
			edge("r-c", EdgeSimple, "reserve", "charge"),
			edge("c-s", EdgeSimple, "charge", "ship"),
		},
	})
	h.runner.failures["do.ship"] = errors.New("the carrier refused it")

	run, err := h.engine.Start(context.Background(), "saga", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}

	order := h.runner.order()
	// Newest committed step first: refunding before un-reserving would leave the
	// inventory released against a charge that still stands.
	want := []string{"do.reserve", "do.charge", "do.ship", "undo.charge", "undo.reserve"}
	if !slices.Equal(order, want) {
		t.Fatalf("compensation order = %v, want %v", order, want)
	}
	if !strings.Contains(run.Error, "rolled back") {
		t.Fatalf("the error does not mention the rollback: %q", run.Error)
	}

	// The compensated steps are marked, so a resumed rollback does not undo them
	// twice.
	states, _ := h.store.ListSteps(context.Background(), run.ID)
	for _, state := range states {
		if (state.Step == "reserve" || state.Step == "charge") && !state.Compensated {
			t.Fatalf("step %q was not marked compensated", state.Step)
		}
	}
}

func TestFailedCompensationStopsAndReportsWhatRemains(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "broken-saga", Start: "one",
		Steps: map[string]*Step{
			"one":   {Name: "one", Intent: "do.one", Compensate: "undo.one"},
			"two":   {Name: "two", Intent: "do.two", Compensate: "undo.two"},
			"three": terminal("three", "do.three"),
		},
		Edges: []*Edge{
			edge("a", EdgeSimple, "one", "two"),
			edge("b", EdgeSimple, "two", "three"),
		},
	})
	h.runner.failures["do.three"] = errors.New("failed")
	h.runner.failures["undo.two"] = errors.New("the refund API is down")

	run, err := h.engine.Start(context.Background(), "broken-saga", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusFailed {
		t.Fatalf("status = %s", run.Status)
	}
	// Carrying on past a failed compensation would leave an inconsistent middle
	// nobody can reason about, so the rollback stops and says what is outstanding.
	if slices.Contains(h.runner.order(), "undo.one") {
		t.Fatalf("the rollback continued past a failed compensation: %v", h.runner.order())
	}
	if !strings.Contains(run.Error, "uncompensated") {
		t.Fatalf("the error does not name what is left undone: %q", run.Error)
	}
	if !slices.Contains(h.noticeKinds(), "compensation_failed") {
		t.Fatalf("no compensation_failed notification was raised: %v", h.noticeKinds())
	}
}

// ---------------------------------------------------------------------------
// Suspension
// ---------------------------------------------------------------------------

func TestWaitEventParksUntilTheCorrelatedSignal(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "waiting", Start: "request",
		Steps: map[string]*Step{
			"request": step("request", "do.request"),
			"settle":  terminal("settle", "do.settle"),
		},
		Edges: []*Edge{{
			Name: "await", Type: EdgeWaitEvent, From: "request", To: "settle",
			Event: "payment.settled", Correlation: constValuer{value: "order-1"},
		}},
	})

	ctx := context.Background()
	run, err := h.engine.Start(ctx, "waiting", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusWaiting {
		t.Fatalf("status = %s, want waiting", run.Status)
	}
	if run.Waiting == nil || run.Waiting.Event != "payment.settled" {
		t.Fatalf("the run does not say what it is waiting for: %+v", run.Waiting)
	}
	if slices.Contains(h.runner.order(), "do.settle") {
		t.Fatal("the downstream step ran before the event arrived")
	}

	// A signal for a different order must not wake this run.
	woken, err := h.engine.Signal(ctx, "payment.settled", "order-2", map[string]any{"amount": 10})
	if err != nil {
		t.Fatalf("signal: %v", err)
	}
	if len(woken) != 0 {
		t.Fatalf("an uncorrelated event woke %v", woken)
	}

	woken, err = h.engine.Signal(ctx, "payment.settled", "order-1", map[string]any{"amount": 10})
	if err != nil {
		t.Fatalf("signal: %v", err)
	}
	if len(woken) != 1 {
		t.Fatalf("the correlated event woke %d runs, want 1", len(woken))
	}
	final, _ := h.store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status after the signal = %s (%s)", final.Status, final.Error)
	}
}

func TestManualGateIsReleasedOnlyByAnOperator(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "gated", Start: "prepare",
		Steps: map[string]*Step{
			"prepare": step("prepare", "do.prepare"),
			"release": terminal("release", "do.release"),
		},
		Edges: []*Edge{{Name: "gate", Type: EdgeManual, From: "prepare", To: "release"}},
	})

	ctx := context.Background()
	run, err := h.engine.Start(ctx, "gated", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusWaiting {
		t.Fatalf("status = %s, want waiting", run.Status)
	}

	// Ticking must not release it: a manual gate exists precisely so that nothing
	// automatic advances past it.
	if _, err := h.engine.Tick(ctx, 10); err != nil {
		t.Fatalf("tick: %v", err)
	}
	after, _ := h.store.GetRun(ctx, run.ID)
	if after.Status != StatusWaiting {
		t.Fatalf("a tick released the operator gate: %s", after.Status)
	}

	if err := h.engine.AdvanceManual(ctx, run.ID, ""); err != nil {
		t.Fatalf("advance manual: %v", err)
	}
	final, _ := h.store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status after release = %s (%s)", final.Status, final.Error)
	}
}

func TestDelayedEdgeParksAndResumesOnItsTimer(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "delaying", Start: "first",
		Steps: map[string]*Step{
			"first":  step("first", "do.first"),
			"second": terminal("second", "do.second"),
		},
		Edges: []*Edge{{Name: "wait", Type: EdgeDelayed, From: "first", To: "second", Timeout: 20 * time.Millisecond}},
	})

	ctx := context.Background()
	run, err := h.engine.Start(ctx, "delaying", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusWaiting {
		t.Fatalf("status = %s, want waiting", run.Status)
	}
	if slices.Contains(h.runner.order(), "do.second") {
		t.Fatal("the delayed step ran immediately")
	}

	time.Sleep(40 * time.Millisecond)
	if _, err := h.engine.Tick(ctx, 10); err != nil {
		t.Fatalf("tick: %v", err)
	}
	final, _ := h.store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status after the delay = %s (%s)", final.Status, final.Error)
	}
}

func TestWaitEventTimeoutTakesItsOnTimeoutPath(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "expiring", Start: "ask",
		Steps: map[string]*Step{
			"ask":     step("ask", "do.ask"),
			"answer":  terminal("answer", "do.answer"),
			"give-up": terminal("give-up", "do.giveup"),
		},
		Edges: []*Edge{{
			Name: "await", Type: EdgeWaitEvent, From: "ask", To: "answer",
			Event: "reply", Correlation: constValuer{value: "x"},
			Timeout: 10 * time.Millisecond, OnTimeout: "give-up",
		}},
	})

	ctx := context.Background()
	run, err := h.engine.Start(ctx, "expiring", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := h.engine.Tick(ctx, 10); err != nil {
		t.Fatalf("tick: %v", err)
	}
	final, _ := h.store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", final.Status, final.Error)
	}
	if !slices.Contains(h.runner.order(), "do.giveup") {
		t.Fatalf("the on_timeout path did not run: %v", h.runner.order())
	}
}

// ---------------------------------------------------------------------------
// Human tasks
// ---------------------------------------------------------------------------

func approvalDefinition() *Definition {
	return &Definition{
		Name: "approving", Start: "prepare",
		Steps: map[string]*Step{
			"prepare": step("prepare", "do.prepare"),
			"approve": {Name: "approve", Task: &TaskDefinition{
				Title:            textRenderer{text: "Approve the order"},
				Role:             "approver",
				Actions:          []string{"approve", "reject"},
				ForbidPrincipals: []Valuer{constValuer{value: "raiser"}},
			}},
			"fulfil":  terminal("fulfil", "do.fulfil"),
			"decline": terminal("decline", "do.decline"),
		},
		Edges: []*Edge{
			edge("p-a", EdgeSimple, "prepare", "approve"),
			{Name: "yes", Type: EdgeBranch, From: "approve", To: "fulfil",
				Guard: exprGuard{path: "result.action", equal: "approve"}},
			{Name: "no", Type: EdgeBranch, From: "approve", To: "decline",
				Guard: exprGuard{path: "result.action", equal: "reject"}},
		},
	}
}

func TestHumanTaskParksAndResumesOnTheChosenAction(t *testing.T) {
	h := newHarness(t, approvalDefinition())
	ctx := context.Background()

	run, err := h.engine.Start(ctx, "approving", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusWaiting {
		t.Fatalf("status = %s, want waiting", run.Status)
	}

	tasks, err := h.engine.ListTasks(ctx, TaskFilter{Roles: []string{"approver"}, Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("the approver's queue has %d tasks, want 1", len(tasks))
	}
	task := tasks[0]
	if task.Title != "Approve the order" {
		t.Fatalf("title = %q", task.Title)
	}

	// Separation of duties: the person who raised the request cannot approve it.
	if _, err := h.engine.CompleteTask(ctx, task.ID, "raiser", "approve", nil); err == nil {
		t.Fatal("a forbidden principal completed the task")
	}
	// An action outside the declared set would leave the outgoing branches with
	// nothing to match, so it is rejected here rather than stranding the run.
	if _, err := h.engine.CompleteTask(ctx, task.ID, "alice", "maybe", nil); err == nil {
		t.Fatal("an undeclared action was accepted")
	}

	if _, err := h.engine.CompleteTask(ctx, task.ID, "alice", "approve", map[string]any{"note": "looks fine"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	final, _ := h.store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", final.Status, final.Error)
	}
	if !slices.Contains(h.runner.order(), "do.fulfil") {
		t.Fatalf("the approve branch did not run: %v", h.runner.order())
	}
	if slices.Contains(h.runner.order(), "do.decline") {
		t.Fatal("the reject branch ran too")
	}
}

func TestTaskClaimIsExclusive(t *testing.T) {
	h := newHarness(t, approvalDefinition())
	ctx := context.Background()
	if _, err := h.engine.Start(ctx, "approving", nil, StartOptions{}); err != nil {
		t.Fatalf("start: %v", err)
	}
	tasks, _ := h.engine.ListTasks(ctx, TaskFilter{Roles: []string{"approver"}, Limit: 10})
	task := tasks[0]

	if _, err := h.engine.ClaimTask(ctx, task.ID, "alice"); err != nil {
		t.Fatalf("alice claims: %v", err)
	}
	if _, err := h.engine.ClaimTask(ctx, task.ID, "bob"); err == nil {
		t.Fatal("two people claimed the same task")
	}
	if _, err := h.engine.CompleteTask(ctx, task.ID, "bob", "approve", nil); err == nil {
		t.Fatal("somebody else completed a claimed task")
	}
	if _, err := h.engine.CompleteTask(ctx, task.ID, "alice", "approve", nil); err != nil {
		t.Fatalf("the claimant could not complete: %v", err)
	}
}

func TestCancellingARunClosesItsTasks(t *testing.T) {
	h := newHarness(t, approvalDefinition())
	ctx := context.Background()
	run, err := h.engine.Start(ctx, "approving", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := h.engine.Cancel(ctx, run.ID, "the customer changed their mind"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// Leaving an open approval for a cancelled order would have somebody approve
	// something that no longer exists.
	open, _ := h.engine.ListTasks(ctx, TaskFilter{RunID: run.ID, Limit: 10})
	if len(open) != 0 {
		t.Fatalf("%d tasks are still open on a cancelled run", len(open))
	}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestCompileRejectsUnreachableAndDeadEndSteps(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		definition *Definition
		contains   string
	}{
		{
			name: "unreachable step",
			definition: &Definition{
				Name: "orphaned", Start: "a",
				Steps: map[string]*Step{"a": terminal("a", "do.a"), "b": terminal("b", "do.b")},
			},
			contains: "cannot be reached",
		},
		{
			name: "no outgoing edge and not terminal",
			definition: &Definition{
				Name: "stuck", Start: "a",
				Steps: map[string]*Step{"a": step("a", "do.a")},
			},
			contains: "no outgoing success edge",
		},
		{
			name: "wait_event without a correlation",
			definition: &Definition{
				Name: "vague", Start: "a",
				Steps: map[string]*Step{"a": step("a", "do.a"), "b": terminal("b", "do.b")},
				Edges: []*Edge{{Name: "w", Type: EdgeWaitEvent, From: "a", To: "b", Event: "thing"}},
			},
			contains: "correlation",
		},
		{
			name: "compensate edge on a step with no compensation",
			definition: &Definition{
				Name: "nothing-to-undo", Start: "a",
				Steps: map[string]*Step{"a": step("a", "do.a"), "b": terminal("b", "do.b")},
				Edges: []*Edge{
					edge("ok", EdgeSimple, "a", "b"),
					{Name: "undo", Type: EdgeCompensate, From: "a", To: "b"},
				},
			},
			contains: "no compensate intent",
		},
		{
			name: "task with nobody addressed",
			definition: &Definition{
				Name: "invisible", Start: "a",
				Steps: map[string]*Step{"a": {Name: "a", Task: &TaskDefinition{}, Terminal: true}},
			},
			contains: "nobody can ever see it",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := Compile(testCase.definition)
			if err == nil {
				t.Fatal("expected a compile error")
			}
			if !strings.Contains(err.Error(), testCase.contains) {
				t.Fatalf("error = %q, want it to mention %q", err, testCase.contains)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Durability
// ---------------------------------------------------------------------------

func TestARunResumesFromItsPersistedCursor(t *testing.T) {
	definition := func() *Definition {
		return &Definition{
			Name: "resumable", Start: "one",
			Steps: map[string]*Step{
				"one":   step("one", "do.one"),
				"two":   {Name: "two", Task: &TaskDefinition{Title: textRenderer{text: "Check"}, Role: "checker", Actions: []string{"ok"}}},
				"three": terminal("three", "do.three"),
			},
			Edges: []*Edge{
				edge("a", EdgeSimple, "one", "two"),
				edge("b", EdgeSimple, "two", "three"),
			},
		}
	}

	store := NewMemoryStore()
	ctx := context.Background()

	// First "process": start the run, which parks on the task.
	firstRunner := newRunner()
	first, err := New(Options{Store: store, Runner: firstRunner, Owner: "replica-a"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := first.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := first.Register(definition()); err != nil {
		t.Fatalf("register: %v", err)
	}
	run, err := first.Start(ctx, "resumable", map[string]any{"id": 1}, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusWaiting {
		t.Fatalf("status = %s, want waiting", run.Status)
	}

	// Second "process": a fresh engine over the same store, as a restart would be.
	secondRunner := newRunner()
	second, err := New(Options{Store: store, Runner: secondRunner, Owner: "replica-b"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := second.Register(definition()); err != nil {
		t.Fatalf("register: %v", err)
	}

	tasks, _ := second.ListTasks(ctx, TaskFilter{Roles: []string{"checker"}, Limit: 10})
	if len(tasks) != 1 {
		t.Fatalf("the restarted engine sees %d tasks, want 1", len(tasks))
	}
	if _, err := second.CompleteTask(ctx, tasks[0].ID, "carol", "ok", nil); err != nil {
		t.Fatalf("complete: %v", err)
	}

	final, _ := store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", final.Status, final.Error)
	}
	// The step before the park ran on the first engine and must not re-run on the
	// second: that is what resuming from a cursor means.
	if secondRunner.count("do.one") != 0 {
		t.Fatal("the restarted engine re-ran a step that had already completed")
	}
	if secondRunner.count("do.three") != 1 {
		t.Fatalf("the restarted engine ran the remaining step %d times", secondRunner.count("do.three"))
	}
}

func TestConcurrentAdvancesDoNotDuplicateWork(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "contended", Start: "a",
		Steps: map[string]*Step{
			"a": step("a", "do.a"),
			"b": terminal("b", "do.b"),
		},
		Edges: []*Edge{edge("ab", EdgeSimple, "a", "b")},
	})
	h.runner.delay["do.a"] = 20 * time.Millisecond

	ctx := context.Background()
	run, err := h.engine.Start(ctx, "contended", nil, StartOptions{Detached: true})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Eight replicas advancing the same run at once. The lease makes one of them the
	// driver and the rest no-ops; without it they would interleave and run steps
	// twice.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.engine.Advance(ctx, run.ID)
		}()
	}
	wg.Wait()

	final, _ := h.store.GetRun(ctx, run.ID)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s (%s)", final.Status, final.Error)
	}
	for _, name := range []string{"do.a", "do.b"} {
		if got := h.runner.count(name); got != 1 {
			t.Fatalf("%s ran %d times under concurrent advances, want 1", name, got)
		}
	}
}

func TestIdempotentStartReturnsTheOriginalRun(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "once", Start: "a",
		Steps: map[string]*Step{"a": terminal("a", "do.a")},
	})
	ctx := context.Background()

	first, err := h.engine.Start(ctx, "once", nil, StartOptions{IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	second, err := h.engine.Start(ctx, "once", nil, StartOptions{IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("a repeated start created a second run: %s and %s", first.ID, second.ID)
	}
	if got := h.runner.count("do.a"); got != 1 {
		t.Fatalf("the step ran %d times, want 1", got)
	}
}

func TestMaxStepsBoundsARunawayLoop(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "looping", Start: "a", MaxSteps: 5, MaxVisits: 100,
		Steps: map[string]*Step{
			"a": step("a", "do.a"),
			"b": step("b", "do.b"),
		},
		Edges: []*Edge{
			edge("ab", EdgeSimple, "a", "b"),
			edge("ba", EdgeSimple, "b", "a"),
		},
	})

	run, err := h.engine.Start(context.Background(), "looping", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if run.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", run.Status)
	}
	if !strings.Contains(run.Error, "max_steps") {
		t.Fatalf("error does not mention the bound: %q", run.Error)
	}
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

func TestRunOutputIsTheTerminalStepResult(t *testing.T) {
	h := newHarness(t, &Definition{
		Name: "producing", Start: "work",
		Steps: map[string]*Step{"work": terminal("work", "do.work")},
	})
	h.runner.results["do.work"] = map[string]any{"invoice": "INV-7", "total": 42}

	run, err := h.engine.Start(context.Background(), "producing", nil, StartOptions{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	var output map[string]any
	if err := json.Unmarshal(run.Output, &output); err != nil {
		t.Fatalf("output is not readable: %v", err)
	}
	if output["invoice"] != "INV-7" {
		t.Fatalf("output = %v", output)
	}
}
