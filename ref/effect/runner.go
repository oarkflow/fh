package effect

import (
	"context"
	"fmt"
)

// Runner manages the commit and compensation of execution effect plans.
type Runner struct {
	store EffectStore
}

// NewRunner creates a new EffectRunner.
func NewRunner(store EffectStore) *Runner {
	if store == nil {
		store = NewMemoryEffectStore()
	}
	return &Runner{store: store}
}

// Store returns the underlying effect store.
func (r *Runner) Store() EffectStore {
	return r.store
}

// Run executes the given effect plan with two-phase commit and compensation.
func (r *Runner) Run(ctx context.Context, executionID string, plan EffectPlan) error {
	effects := plan.All()
	if len(effects) == 0 {
		return nil
	}
	if err := CommitPlan(ctx, r.store, executionID, effects); err != nil {
		return fmt.Errorf("ref: effect runner failed: %w", err)
	}
	return nil
}
