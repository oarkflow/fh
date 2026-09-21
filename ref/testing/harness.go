package testing

import (
	"context"
	"reflect"
	"testing"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// TestBuilder constructs test executions with pre-populated facts and mock inputs.
type TestBuilder struct {
	inv       *invocation.Invocation
	facts     map[fact.PlanSlot]any
	defToSlot map[fact.DefinitionID]fact.PlanSlot
	nextSlot  fact.PlanSlot
}

// NewTest initializes a new TestBuilder.
func NewTest() *TestBuilder {
	return &TestBuilder{
		inv: &invocation.Invocation{
			ID:        "test-inv-001",
			Intent:    "test.intent",
			Transport: invocation.Transport{Protocol: "test"},
		},
		facts:     make(map[fact.PlanSlot]any),
		defToSlot: make(map[fact.DefinitionID]fact.PlanSlot),
	}
}

// Given pre-populates a typed fact by its Key.
func Given[T any](tb *TestBuilder, key fact.Key[T], value T) *TestBuilder {
	slot, exists := tb.defToSlot[key.DefinitionID()]
	if !exists {
		slot = tb.nextSlot
		tb.nextSlot++
		tb.defToSlot[key.DefinitionID()] = slot
	}
	tb.facts[slot] = value
	return tb
}

// GivenSlot pre-populates a fact directly at a dense plan slot.
func GivenSlot[T any](tb *TestBuilder, slot fact.PlanSlot, value T) *TestBuilder {
	tb.facts[slot] = value
	if slot >= tb.nextSlot {
		tb.nextSlot = slot + 1
	}
	return tb
}

// WithInput sets the raw input payload.
func (tb *TestBuilder) WithInput(raw []byte, contentType string) *TestBuilder {
	tb.inv.Input = invocation.NewInput(raw, contentType)
	return tb
}

// WithIntent sets the intent name being tested.
func (tb *TestBuilder) WithIntent(name string) *TestBuilder {
	tb.inv.Intent = invocation.IntentID(name)
	return tb
}

// WithPrincipal sets the principal hint.
func (tb *TestBuilder) WithPrincipal(bearer, apiKey string) *TestBuilder {
	tb.inv.Principal = invocation.NewPrincipalHint(bearer, apiKey, nil, "")
	return tb
}

// BuildNodeContext creates an isolated NodeContext populated with given facts.
func (tb *TestBuilder) BuildNodeContext(ctx context.Context, slotCount int) *execution.NodeContext {
	if slotCount < int(tb.nextSlot) {
		slotCount = int(tb.nextSlot)
	}
	store := fact.NewStore(slotCount)
	for slot, val := range tb.facts {
		fact.Put(store, slot, val)
	}

	budget := execution.NewBudget(0, 0, 0, 0, 0)
	decisions := execution.NewDecisionSet()

	return execution.NewNodeContext(
		ctx,
		tb.inv,
		store,
		budget,
		decisions,
		0,
		tb.defToSlot,
	)
}

// AssertFact asserts that a typed fact is present in the node context and matches expected value.
func AssertFact[T any](t *testing.T, nc *execution.NodeContext, key fact.Key[T], expected T) {
	t.Helper()
	actual, err := execution.Require(nc, key)
	if err != nil {
		t.Fatalf("expected fact %q to be present: %v", key.Name, err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("fact %q value mismatch:\nexpected: %+v\nactual:   %+v", key.Name, expected, actual)
	}
}

// AssertOutcome asserts that the dispatch result value matches expected value.
func AssertOutcome[T any](t *testing.T, res *runtime.DispatchResult, expected T) {
	t.Helper()
	if res == nil {
		t.Fatalf("expected non-nil dispatch result")
	}
	actual, ok := res.Value.(T)
	if !ok {
		t.Fatalf("expected result of type %T, got %T (%+v)", expected, res.Value, res.Value)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("outcome value mismatch:\nexpected: %+v\nactual:   %+v", expected, actual)
	}
}
