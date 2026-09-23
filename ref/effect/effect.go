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
	Phase   string
	Name    string
	Err     error
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
	var txID string
	var err error

	if store != nil {
		txID, err = store.Begin(ctx, executionID)
		if err != nil {
			return fmt.Errorf("ref: effect store begin error: %w", err)
		}
	}

	var reportErr func(EffectError)
	if len(onErr) > 0 && onErr[0] != nil {
		reportErr = onErr[0]
	}

	// Phase 1: Record all DurableDelivery effects into the transaction BEFORE committing.
	// This ensures that domain writes and durable outbox records share the exact same transaction.
	for _, e := range effects {
		if e.Kind() == DurableDelivery {
			if store != nil && txID != "" {
				if err := store.Record(ctx, txID, EffectRecord{
					Name: e.Name(),
					Kind: DurableDelivery,
				}); err != nil {
					return fmt.Errorf("ref: failed to record durable effect %q: %w", e.Name(), err)
				}
			}
		}
	}

	// Phase 2: Commit LocalTransactional effects
	var committedTx []CompensatingEffect
	for _, e := range effects {
		if e.Kind() == LocalTransactional {
			if err := e.Commit(ctx); err != nil {
				// Atomic failure — compensate already executed effects in reverse
				for i := len(committedTx) - 1; i >= 0; i-- {
					_ = committedTx[i].Compensate(ctx)
				}
				return fmt.Errorf("ref: local transactional effect %q failed: %w", e.Name(), err)
			}
			if ce, ok := e.(CompensatingEffect); ok {
				committedTx = append(committedTx, ce)
			}
		}
	}

	// Phase 3: Atomic commit of the transaction (domain writes + outbox records commit together)
	if store != nil && txID != "" {
		if err := store.Commit(ctx, txID); err != nil {
			for i := len(committedTx) - 1; i >= 0; i-- {
				_ = committedTx[i].Compensate(ctx)
			}
			return fmt.Errorf("ref: effect store commit error: %w", err)
		}
	}

	// Phase 4: Schedule delivery of the committed outbox records
	if store != nil && txID != "" {
		if err := store.ScheduleDelivery(ctx, txID); err != nil && reportErr != nil {
			reportErr(EffectError{Phase: "schedule_delivery", Err: err})
		}
	}

	for _, e := range effects {
		if e.Kind() == DurableDelivery {
			// Best-effort immediate dispatch; background worker retries if unfulfilled
			if err := e.Commit(ctx); err != nil && reportErr != nil {
				reportErr(EffectError{Phase: "durable_commit", Name: e.Name(), Err: err})
			}
		}
	}

	// Phase 5: Fire-and-forget
	for _, e := range effects {
		if e.Kind() == FireAndForget {
			if err := e.Commit(ctx); err != nil && reportErr != nil {
				reportErr(EffectError{Phase: "fire_and_forget", Name: e.Name(), Err: err})
			}
		}
	}

	return nil
}
