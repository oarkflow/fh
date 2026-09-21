package execution

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrBudgetExhausted = errors.New("ref: execution budget exhausted")
	ErrBudgetTimeout   = errors.New("ref: execution budget deadline exceeded")
)

// Budget holds per-execution resource limits with enforceable token acquisition.
type Budget struct {
	Deadline      time.Time
	maxDBQueries  int32
	maxExternalIO int32
	maxMemory     int64
	maxEffects    int32

	usedDBQueries  atomic.Int32
	usedExternalIO atomic.Int32
	usedMemory     atomic.Int64
	usedEffects    atomic.Int32
}

// NewBudget creates an enforceable budget.
func NewBudget(timeout time.Duration, maxDB, maxIO int, maxMem int64, maxFx int) *Budget {
	b := &Budget{
		maxDBQueries:  int32(maxDB),
		maxExternalIO: int32(maxIO),
		maxMemory:     maxMem,
		maxEffects:    int32(maxFx),
	}
	if timeout > 0 {
		b.Deadline = time.Now().Add(timeout)
	}
	return b
}

// AcquireDBQuery consumes DB query tokens. Returns error if exhausted or timed out.
func (b *Budget) AcquireDBQuery(n int32) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxDBQueries > 0 {
		if b.usedDBQueries.Add(n) > b.maxDBQueries {
			return ErrBudgetExhausted
		}
	} else {
		b.usedDBQueries.Add(n)
	}
	return nil
}

// AcquireExternalIO consumes external I/O tokens.
func (b *Budget) AcquireExternalIO(n int32) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxExternalIO > 0 {
		if b.usedExternalIO.Add(n) > b.maxExternalIO {
			return ErrBudgetExhausted
		}
	} else {
		b.usedExternalIO.Add(n)
	}
	return nil
}

// AcquireMemory consumes memory budget.
func (b *Budget) AcquireMemory(bytes int64) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxMemory > 0 {
		if b.usedMemory.Add(bytes) > b.maxMemory {
			return ErrBudgetExhausted
		}
	} else {
		b.usedMemory.Add(bytes)
	}
	return nil
}

// AcquireEffect consumes effect tokens.
func (b *Budget) AcquireEffect(n int32) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxEffects > 0 {
		if b.usedEffects.Add(n) > b.maxEffects {
			return ErrBudgetExhausted
		}
	} else {
		b.usedEffects.Add(n)
	}
	return nil
}

func (b *Budget) checkDeadline() error {
	if b == nil || b.Deadline.IsZero() {
		return nil
	}
	if time.Now().After(b.Deadline) {
		return ErrBudgetTimeout
	}
	return nil
}

// Remaining returns time left until deadline. MaxDuration if no deadline.
func (b *Budget) Remaining() time.Duration {
	if b == nil || b.Deadline.IsZero() {
		return time.Duration(^uint64(0) >> 1)
	}
	r := time.Until(b.Deadline)
	if r < 0 {
		return 0
	}
	return r
}

// Snapshot returns current usage for observability.
func (b *Budget) Snapshot() BudgetSnapshot {
	if b == nil {
		return BudgetSnapshot{}
	}
	return BudgetSnapshot{
		DBQueries:  b.usedDBQueries.Load(),
		ExternalIO: b.usedExternalIO.Load(),
		Memory:     b.usedMemory.Load(),
		Effects:    b.usedEffects.Load(),
	}
}

// BudgetSnapshot captures point-in-time budget metrics.
type BudgetSnapshot struct {
	DBQueries  int32
	ExternalIO int32
	Memory     int64
	Effects    int32
}
