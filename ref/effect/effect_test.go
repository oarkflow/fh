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
