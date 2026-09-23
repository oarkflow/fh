package effect_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/oarkflow/fh/ref/effect"
)

type mockEffect struct {
	name      string
	kind      effect.EffectKind
	committed atomic.Bool
	err       error
}

func (m *mockEffect) Name() string               { return m.name }
func (m *mockEffect) Kind() effect.EffectKind    { return m.kind }
func (m *mockEffect) Commit(ctx context.Context) error {
	m.committed.Store(true)
	return m.err
}

type mockCompensatingEffect struct {
	mockEffect
	compensated atomic.Bool
}

func (m *mockCompensatingEffect) Compensate(ctx context.Context) error {
	m.compensated.Store(true)
	return nil
}

func TestCommitPlanSuccess(t *testing.T) {
	store := effect.NewMemoryEffectStore()

	e1 := &mockEffect{name: "db-insert", kind: effect.LocalTransactional}
	e2 := &mockEffect{name: "webhook", kind: effect.DurableDelivery}
	e3 := &mockEffect{name: "metric", kind: effect.FireAndForget}

	err := effect.CommitPlan(context.Background(), store, "exec-1", []effect.Effect{e1, e2, e3})
	if err != nil {
		t.Fatalf("unexpected commit plan error: %v", err)
	}

	if !e1.committed.Load() || !e2.committed.Load() || !e3.committed.Load() {
		t.Errorf("all effects should have been committed")
	}
}

func TestCommitPlanCompensation(t *testing.T) {
	store := effect.NewMemoryEffectStore()

	e1 := &mockCompensatingEffect{mockEffect: mockEffect{name: "reserve-inventory", kind: effect.LocalTransactional}}
	e2 := &mockEffect{name: "charge-card", kind: effect.LocalTransactional, err: errors.New("insufficient funds")}

	err := effect.CommitPlan(context.Background(), store, "exec-2", []effect.Effect{e1, e2})
	if err == nil {
		t.Fatalf("expected error from charge-card failure, got nil")
	}

	if !e1.compensated.Load() {
		t.Errorf("expected e1 to be compensated when subsequent transactional effect failed")
	}
}

func TestCommitPlanErrorReporting(t *testing.T) {
	store := effect.NewMemoryEffectStore()

	var reported []effect.EffectError
	reporter := func(err effect.EffectError) {
		reported = append(reported, err)
	}

	// Durable effect whose Commit fails — error should be reported, not swallowed.
	e1 := &mockEffect{name: "webhook-fail", kind: effect.DurableDelivery, err: errors.New("connection refused")}
	e2 := &mockEffect{name: "metric", kind: effect.FireAndForget, err: errors.New("metric drop")}

	err := effect.CommitPlan(context.Background(), store, "exec-3", []effect.Effect{e1, e2}, reporter)
	if err != nil {
		t.Fatalf("non-fatal errors should not abort commit plan: %v", err)
	}

	if len(reported) != 2 {
		t.Fatalf("expected 2 reported errors, got %d", len(reported))
	}
	if reported[0].Phase != "durable_commit" || reported[0].Name != "webhook-fail" {
		t.Errorf("first report mismatch: %+v", reported[0])
	}
	if reported[1].Phase != "fire_and_forget" || reported[1].Name != "metric" {
		t.Errorf("second report mismatch: %+v", reported[1])
	}
}

func TestCommitPlanNoReporterSilent(t *testing.T) {
	store := effect.NewMemoryEffectStore()

	// Without a reporter, errors should be silently discarded (backward compat).
	e1 := &mockEffect{name: "webhook", kind: effect.DurableDelivery, err: errors.New("fail")}
	err := effect.CommitPlan(context.Background(), store, "exec-4", []effect.Effect{e1})
	if err != nil {
		t.Fatalf("non-fatal error should not abort commit plan: %v", err)
	}
}

func TestEffectErrorMessage(t *testing.T) {
	e := effect.EffectError{Phase: "schedule_delivery", Err: errors.New("db down")}
	if e.Error() != "ref: effect schedule_delivery: db down" {
		t.Errorf("unexpected error message: %s", e.Error())
	}
	if e.Unwrap() == nil {
		t.Error("expected Unwrap to return the underlying error")
	}

	e2 := effect.EffectError{Phase: "durable_commit", Name: "send-email", Err: errors.New("timeout")}
	if e2.Error() != `ref: effect durable_commit "send-email": timeout` {
		t.Errorf("unexpected error message: %s", e2.Error())
	}
}
