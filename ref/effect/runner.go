package effect

import (
	"context"
	"fmt"
)

// Runner manages the commit and compensation of execution effect plans.
type Runner struct {
	store EffectStore
	onErr EffectErrorFunc
}

// NewRunner creates a new EffectRunner.
func NewRunner(store EffectStore, opts ...RunnerOption) *Runner {
	if store == nil {
		store = NewMemoryEffectStore()
	}
	r := &Runner{store: store}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RunnerOption configures a Runner.
type RunnerOption func(*Runner)

// WithEffectErrorHandler sets a callback for non-fatal effect delivery errors.
func WithEffectErrorHandler(fn EffectErrorFunc) RunnerOption {
	return func(r *Runner) { r.onErr = fn }
}

// Store returns the underlying effect store.
func (r *Runner) Store() EffectStore {
	return r.store
}

// Run executes the given effect plan with two-phase commit and compensation.
func (r *Runner) Run(ctx context.Context, executionID string, plan EffectPlan) error {
	if plan.IsEmpty() {
		return nil
	}
	if err := CommitEffectPlan(ctx, r.store, executionID, plan, r.onErr); err != nil {
		return fmt.Errorf("ref: effect runner failed: %w", err)
	}
	return nil
}
