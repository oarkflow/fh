package effect

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// EffectStore is the dedicated persistence interface for durable effect delivery.
type EffectStore interface {
	// Begin starts a new effect transaction.
	Begin(ctx context.Context, executionID string) (txID string, err error)

	// Record persists a pending effect within a transaction.
	Record(ctx context.Context, txID string, record EffectRecord) error

	// Commit finalizes all LocalTransactional effects atomically.
	Commit(ctx context.Context, txID string) error

	// ScheduleDelivery enqueues DurableDelivery effects for retry.
	ScheduleDelivery(ctx context.Context, txID string) error

	// Recover re-processes incomplete transactions after a crash.
	Recover(ctx context.Context) ([]PendingTransaction, error)

	// Close shuts down the store.
	Close() error
}

type memTx struct {
	executionID string
	effects     []EffectRecord
	committed   bool
	scheduled   bool
}

// MemoryEffectStore is an in-memory implementation of EffectStore for testing and single-node setups.
type MemoryEffectStore struct {
	mu      sync.RWMutex
	counter atomic.Uint64
	txs     map[string]*memTx
	maxTxs  int
}

// NewMemoryEffectStore creates a new in-memory effect store.
func NewMemoryEffectStore() *MemoryEffectStore {
	return &MemoryEffectStore{
		txs:    make(map[string]*memTx),
		maxTxs: 10000,
	}
}

// cleanup removes old committed transactions to prevent unbounded growth.
func (s *MemoryEffectStore) cleanup() {
	if len(s.txs) <= s.maxTxs {
		return
	}
	for id, tx := range s.txs {
		if tx.committed {
			delete(s.txs, id)
			if len(s.txs) <= s.maxTxs {
				return
			}
		}
	}
}

func (s *MemoryEffectStore) Begin(ctx context.Context, executionID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Cleanup old transactions to prevent unbounded growth
	s.cleanup()

	id := fmt.Sprintf("tx-%d", s.counter.Add(1))
	s.txs[id] = &memTx{
		executionID: executionID,
	}
	return id, nil
}

func (s *MemoryEffectStore) Record(ctx context.Context, txID string, record EffectRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txs[txID]
	if !ok {
		return fmt.Errorf("ref: effect tx %q not found", txID)
	}
	tx.effects = append(tx.effects, record)
	return nil
}

func (s *MemoryEffectStore) Commit(ctx context.Context, txID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txs[txID]
	if !ok {
		return fmt.Errorf("ref: effect tx %q not found", txID)
	}
	tx.committed = true
	return nil
}

func (s *MemoryEffectStore) ScheduleDelivery(ctx context.Context, txID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.txs[txID]
	if !ok {
		return fmt.Errorf("ref: effect tx %q not found", txID)
	}
	tx.scheduled = true
	return nil
}

func (s *MemoryEffectStore) Recover(ctx context.Context) ([]PendingTransaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var pending []PendingTransaction
	for id, tx := range s.txs {
		if tx.committed && !tx.scheduled {
			pending = append(pending, PendingTransaction{
				TxID:        id,
				ExecutionID: tx.executionID,
				Effects:     append([]EffectRecord(nil), tx.effects...),
			})
		}
	}
	return pending, nil
}

func (s *MemoryEffectStore) Close() error {
	return nil
}
