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

	// Source-aware limits
	maxConcurrentIO int32 // max simultaneous source operations
	maxResultRows   int64 // max total rows/items from all sources
	maxResultBytes  int64 // max total bytes transferred from sources

	usedDBQueries  atomic.Int32
	usedExternalIO atomic.Int32
	usedMemory     atomic.Int64
	usedEffects    atomic.Int32

	// Source-aware counters
	activeConcurrentIO atomic.Int32
	usedResultRows     atomic.Int64
	usedResultBytes    atomic.Int64
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
// Tokens are only consumed on success — failed acquisitions do not leak.
func (b *Budget) AcquireDBQuery(n int32) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxDBQueries > 0 {
		for {
			old := b.usedDBQueries.Load()
			if old+n > b.maxDBQueries {
				return ErrBudgetExhausted
			}
			if b.usedDBQueries.CompareAndSwap(old, old+n) {
				return nil
			}
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
		for {
			old := b.usedExternalIO.Load()
			if old+n > b.maxExternalIO {
				return ErrBudgetExhausted
			}
			if b.usedExternalIO.CompareAndSwap(old, old+n) {
				return nil
			}
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
		for {
			old := b.usedMemory.Load()
			if old+bytes > b.maxMemory {
				return ErrBudgetExhausted
			}
			if b.usedMemory.CompareAndSwap(old, old+bytes) {
				return nil
			}
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
		for {
			old := b.usedEffects.Load()
			if old+n > b.maxEffects {
				return ErrBudgetExhausted
			}
			if b.usedEffects.CompareAndSwap(old, old+n) {
				return nil
			}
		}
	} else {
		b.usedEffects.Add(n)
	}
	return nil
}

// AcquireConcurrentIO increments the concurrent I/O counter.
// Returns error if the limit would be exceeded.
func (b *Budget) AcquireConcurrentIO() error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxConcurrentIO > 0 {
		for {
			old := b.activeConcurrentIO.Load()
			if old+1 > b.maxConcurrentIO {
				return ErrBudgetExhausted
			}
			if b.activeConcurrentIO.CompareAndSwap(old, old+1) {
				return nil
			}
		}
	}
	b.activeConcurrentIO.Add(1)
	return nil
}

// ReleaseConcurrentIO decrements the concurrent I/O counter.
func (b *Budget) ReleaseConcurrentIO() {
	if b == nil {
		return
	}
	b.activeConcurrentIO.Add(-1)
}

// AcquireResultRows consumes result row budget.
func (b *Budget) AcquireResultRows(n int64) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxResultRows > 0 {
		for {
			old := b.usedResultRows.Load()
			if old+n > b.maxResultRows {
				return ErrBudgetExhausted
			}
			if b.usedResultRows.CompareAndSwap(old, old+n) {
				return nil
			}
		}
	} else {
		b.usedResultRows.Add(n)
	}
	return nil
}

// AcquireResultBytes consumes result byte budget.
func (b *Budget) AcquireResultBytes(n int64) error {
	if b == nil {
		return nil
	}
	if err := b.checkDeadline(); err != nil {
		return err
	}
	if b.maxResultBytes > 0 {
		for {
			old := b.usedResultBytes.Load()
			if old+n > b.maxResultBytes {
				return ErrBudgetExhausted
			}
			if b.usedResultBytes.CompareAndSwap(old, old+n) {
				return nil
			}
		}
	} else {
		b.usedResultBytes.Add(n)
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
		DBQueries:    b.usedDBQueries.Load(),
		ExternalIO:   b.usedExternalIO.Load(),
		Memory:       b.usedMemory.Load(),
		Effects:      b.usedEffects.Load(),
		ConcurrentIO: b.activeConcurrentIO.Load(),
		ResultRows:   b.usedResultRows.Load(),
		ResultBytes:  b.usedResultBytes.Load(),
	}
}

// BudgetSnapshot captures point-in-time budget metrics.
type BudgetSnapshot struct {
	DBQueries    int32
	ExternalIO   int32
	Memory       int64
	Effects      int32
	ConcurrentIO int32
	ResultRows   int64
	ResultBytes  int64
}
