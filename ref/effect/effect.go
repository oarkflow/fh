package effect

import (
	"context"
	"fmt"
)

// Effect is an externally observable mutation.
type Effect interface {
	Name() string
	Kind() EffectKind
	Commit(context.Context) error
}

// EffectKind classifies effects for commit and delivery strategy.
type EffectKind uint8

const (
	// LocalTransactional effects commit atomically in a single DB transaction.
	// DB writes + transactional outbox entries.
	LocalTransactional EffectKind = iota

	// DurableDelivery effects are recorded in a journal and delivered with
	// retry and idempotency. Webhooks, emails, external API calls.
	DurableDelivery

	// FireAndForget effects are best-effort. Metrics, telemetry.
	FireAndForget
)

func (k EffectKind) String() string {
	switch k {
	case LocalTransactional:
		return "local_transactional"
	case DurableDelivery:
		return "durable_delivery"
	case FireAndForget:
		return "fire_and_forget"
	default:
		return "unknown"
	}
}

// CompensatingEffect is an effect that can be compensated (forward recovery).
type CompensatingEffect interface {
	Effect
	Compensate(context.Context) error
}

// EffectPlan groups planned mutations by delivery tier.
type EffectPlan struct {
	LocalTx       []Effect
	Durable       []Effect
	FireAndForget []Effect
}

// All returns all effects in this plan in order of execution.
func (ep EffectPlan) All() []Effect {
	total := len(ep.LocalTx) + len(ep.Durable) + len(ep.FireAndForget)
	if total == 0 {
		return nil
	}
	res := make([]Effect, 0, total)
	res = append(res, ep.LocalTx...)
	res = append(res, ep.Durable...)
	res = append(res, ep.FireAndForget...)
	return res
}

// IsEmpty reports whether the plan contains zero effects.
func (ep EffectPlan) IsEmpty() bool {
	return len(ep.LocalTx) == 0 && len(ep.Durable) == 0 && len(ep.FireAndForget) == 0
}

// EffectRecord is the durable representation of an effect in a store.
type EffectRecord struct {
	Name       string
	Kind       EffectKind
	Payload    []byte
	Idempotent bool
}

// PendingTransaction represents an incomplete transaction found during recovery.
type PendingTransaction struct {
	TxID        string
	ExecutionID string
	Effects     []EffectRecord
}

// EffectError captures a non-fatal error during effect delivery scheduling
// or best-effort commit. These errors do not abort the transaction but should
// be observable for operational health.
type EffectError struct {
	Phase string
	Name  string
	Err   error
}

func (e EffectError) Error() string {
	if e.Name != "" {
		return fmt.Sprintf("ref: effect %s %q: %v", e.Phase, e.Name, e.Err)
	}
	return fmt.Sprintf("ref: effect %s: %v", e.Phase, e.Err)
}

func (e EffectError) Unwrap() error { return e.Err }

// EffectErrorFunc is called when a non-fatal effect error occurs during
// delivery scheduling or best-effort commit. The function must be safe for
// concurrent use.
type EffectErrorFunc func(err EffectError)

// CommitPlan executes the crash-safe two-phase effect commit strategy:
//  1. Begin effect transaction
//  2. Record all DurableDelivery effects into the open transaction (outbox pattern)
//  3. Commit LocalTransactional effects atomically inside the same transaction
//  4. Atomic Commit of transaction (domain writes + durable records commit together)
//  5. Schedule async delivery worker for durable records
//  6. Execute FireAndForget effects best-effort
//
// If local transactional commit fails, compensating effects run and tx is aborted.
// Non-fatal errors (delivery scheduling, best-effort commit) are reported via onErr
// when non-nil, instead of being silently discarded.
func CommitPlan(ctx context.Context, store EffectStore, executionID string, effects []Effect, onErr ...EffectErrorFunc) error {
	txID, err := beginEffectTransaction(ctx, store, executionID)
	if err != nil {
		return err
	}
	var reportErr EffectErrorFunc
	if len(onErr) > 0 {
		reportErr = onErr[0]
	}
	var localBuf, durableBuf, fireBuf [8]Effect
	local, durable, fire := localBuf[:0], durableBuf[:0], fireBuf[:0]
	for _, e := range effects {
		switch e.Kind() {
		case LocalTransactional:
			local = append(local, e)
		case DurableDelivery:
			durable = append(durable, e)
		case FireAndForget:
			fire = append(fire, e)
		}
	}
	return commitEffectGroups(ctx, store, txID, local, durable, fire, reportErr)
}

// CommitEffectPlan commits effects already grouped by delivery semantics. It
// avoids flattening the plan and re-classifying each effect at runtime.
func CommitEffectPlan(ctx context.Context, store EffectStore, executionID string, plan EffectPlan, onErr ...EffectErrorFunc) error {
	txID, err := beginEffectTransaction(ctx, store, executionID)
	if err != nil {
		return err
	}
	var reportErr EffectErrorFunc
	if len(onErr) > 0 {
		reportErr = onErr[0]
	}
	return commitEffectGroups(ctx, store, txID, plan.LocalTx, plan.Durable, plan.FireAndForget, reportErr)
}

func beginEffectTransaction(ctx context.Context, store EffectStore, executionID string) (string, error) {
	if store == nil {
		return "", nil
	}
	txID, err := store.Begin(ctx, executionID)
	if err != nil {
		return "", fmt.Errorf("ref: effect store begin error: %w", err)
	}
	return txID, nil
}

func commitEffectGroups(ctx context.Context, store EffectStore, txID string, local, durable, fire []Effect, reportErr EffectErrorFunc) error {
	if store != nil && txID != "" {
		for _, e := range durable {
			if err := store.Record(ctx, txID, EffectRecord{Name: e.Name(), Kind: DurableDelivery}); err != nil {
				return fmt.Errorf("ref: failed to record durable effect %q: %w", e.Name(), err)
			}
		}
	}

	var committed [8]CompensatingEffect
	committedCount := 0
	var overflow []CompensatingEffect
	for _, e := range local {
		if err := e.Commit(ctx); err != nil {
			for i := committedCount - 1; i >= 0; i-- {
				if i < len(committed) {
					_ = committed[i].Compensate(ctx)
				} else {
					_ = overflow[i-len(committed)].Compensate(ctx)
				}
			}
			return fmt.Errorf("ref: local transactional effect %q failed: %w", e.Name(), err)
		}
		if ce, ok := e.(CompensatingEffect); ok {
			if committedCount < len(committed) {
				committed[committedCount] = ce
			} else {
				overflow = append(overflow, ce)
			}
			committedCount++
		}
	}

	if store != nil && txID != "" {
		if err := store.Commit(ctx, txID); err != nil {
			for i := committedCount - 1; i >= 0; i-- {
				if i < len(committed) {
					_ = committed[i].Compensate(ctx)
				} else {
					_ = overflow[i-len(committed)].Compensate(ctx)
				}
			}
			return fmt.Errorf("ref: effect store commit error: %w", err)
		}
		if err := store.ScheduleDelivery(ctx, txID); err != nil && reportErr != nil {
			reportErr(EffectError{Phase: "schedule_delivery", Err: err})
		}
	}

	for _, e := range durable {
		if err := e.Commit(ctx); err != nil && reportErr != nil {
			reportErr(EffectError{Phase: "durable_commit", Name: e.Name(), Err: err})
		}
	}
	for _, e := range fire {
		if err := e.Commit(ctx); err != nil && reportErr != nil {
			reportErr(EffectError{Phase: "fire_and_forget", Name: e.Name(), Err: err})
		}
	}
	return nil
}
