package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/effect"
)

// The effect store: where REF's two-phase commit meets a real database.
//
// REF calls this store around every intent that plans effects:
//
//	Begin(executionID)            → this store opens a real *sql.Tx
//	Record(txID, durable effect)  → notes what will need delivering
//	  … LocalTransactional effects run, each writing through that same Tx …
//	Commit(txID)                  → the Tx commits: domain rows AND outbox rows
//	ScheduleDelivery(txID)        → wakes the delivery worker
//	Recover()                     → finds work the process died in the middle of
//
// The important consequence is that LocalTransactional effects are genuinely
// atomic. They are not "atomic if each happens to succeed": they share one
// transaction, so a failure in the third leaves no trace of the first two, and the
// outbox row that promises an email cannot commit unless the order did.
//
// An effect finds its transaction by execution id — Tx(executionID) — because
// REF's Effect interface passes only a context, and inventing a way to smuggle a
// *sql.Tx through it would be worse than looking it up.
type SQLEffectStore struct {
	db    *sql.DB
	queue *fh.DurableQueue
	log   *slog.Logger

	mu          sync.Mutex
	byTxID      map[string]*openTx
	byExecution map[string]*openTx

	stop chan struct{}
	done chan struct{}
}

type openTx struct {
	txID        string
	executionID string
	tx          *sql.Tx
	records     []effect.EffectRecord
	started     time.Time
}

// abandonedTxTimeout bounds how long an unfinished transaction may hold its rows.
//
// It exists because REF's EffectStore contract has no "abort" call: when a
// LocalTransactional effect fails, CommitPlan compensates and returns, and nothing
// tells the store. The host aborts explicitly on a failed dispatch (see
// AbortExecution), and this sweeper is the backstop for the paths that do not —
// without it, one failed effect would hold a Postgres transaction open until the
// connection died.
const abandonedTxTimeout = 30 * time.Second

func NewSQLEffectStore(db *sql.DB, queue *fh.DurableQueue, log *slog.Logger) *SQLEffectStore {
	store := &SQLEffectStore{
		db:          db,
		queue:       queue,
		log:         log,
		byTxID:      map[string]*openTx{},
		byExecution: map[string]*openTx{},
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	go store.sweep()
	return store
}

// Begin opens the transaction every effect of this execution will write through.
func (s *SQLEffectStore) Begin(ctx context.Context, executionID string) (string, error) {
	if executionID == "" {
		return "", fmt.Errorf("ref-app: an effect transaction needs an execution id")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	entry := &openTx{txID: newID("etx-"), executionID: executionID, tx: tx, started: time.Now()}

	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, ok := s.byExecution[executionID]; ok {
		// One execution, one transaction. A second Begin means the first was
		// abandoned; rolling it back here is safer than leaving it open.
		_ = previous.tx.Rollback()
		delete(s.byTxID, previous.txID)
	}
	s.byTxID[entry.txID] = entry
	s.byExecution[executionID] = entry
	return entry.txID, nil
}

// Tx returns the open transaction for an execution. Effects call this.
func (s *SQLEffectStore) Tx(executionID string) (*sql.Tx, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byExecution[executionID]
	if !ok {
		return nil, false
	}
	return entry.tx, true
}

// Record notes a durable effect. The row is written in Commit, inside the
// transaction, so the journal cannot disagree with the domain rows.
func (s *SQLEffectStore) Record(_ context.Context, txID string, record effect.EffectRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byTxID[txID]
	if !ok {
		return fmt.Errorf("ref-app: effect transaction %q is not open", txID)
	}
	entry.records = append(entry.records, record)
	return nil
}

// Commit writes the journal row and commits everything at once.
func (s *SQLEffectStore) Commit(ctx context.Context, txID string) error {
	s.mu.Lock()
	entry, ok := s.byTxID[txID]
	if ok {
		delete(s.byTxID, txID)
		delete(s.byExecution, entry.executionID)
	}
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("ref-app: effect transaction %q is not open", txID)
	}

	names := make([]string, 0, len(entry.records))
	for _, record := range entry.records {
		names = append(names, record.Name+":"+record.Kind.String())
	}
	encoded, err := json.Marshal(names)
	if err != nil {
		_ = entry.tx.Rollback()
		return err
	}
	// 'committed' rather than 'scheduled': the difference between the two is
	// exactly the crash window that Recover looks for.
	if _, err := entry.tx.ExecContext(ctx,
		`INSERT INTO effect_journal (tx_id,execution_id,effects,state)
		 VALUES ($1,$2,$3,'committed')`,
		entry.txID, entry.executionID, encoded); err != nil {
		_ = entry.tx.Rollback()
		return err
	}
	return entry.tx.Commit()
}

// ScheduleDelivery marks the journal row scheduled and wakes the delivery worker.
//
// The enqueue is an optimisation, not the mechanism: the worker also polls the
// outbox table, so a lost wake-up costs latency rather than a lost notification.
func (s *SQLEffectStore) ScheduleDelivery(ctx context.Context, txID string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE effect_journal SET state='scheduled', updated_at=NOW() WHERE tx_id=$1`, txID); err != nil {
		return err
	}
	if s.queue != nil {
		if _, err := s.queue.Enqueue(jobDeliverOutbox, map[string]string{"tx_id": txID}); err != nil {
			s.log.Warn("could not wake the delivery worker; the poller will pick it up",
				slog.String("tx_id", txID), slog.String("error", err.Error()))
		}
	}
	return nil
}

// Recover reports transactions that committed but were never scheduled — the
// process died between the commit and the enqueue. Their outbox rows are already
// in the table, so recovery is a matter of noticing and scheduling them.
func (s *SQLEffectStore) Recover(ctx context.Context) ([]effect.PendingTransaction, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT tx_id,execution_id,effects FROM effect_journal
		  WHERE state='committed' ORDER BY created_at LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pending []effect.PendingTransaction
	for rows.Next() {
		var (
			txID, executionID string
			encoded           []byte
		)
		if err := rows.Scan(&txID, &executionID, &encoded); err != nil {
			return nil, err
		}
		var names []string
		_ = json.Unmarshal(encoded, &names)
		records := make([]effect.EffectRecord, 0, len(names))
		for _, name := range names {
			records = append(records, effect.EffectRecord{Name: name, Kind: effect.DurableDelivery})
		}
		pending = append(pending, effect.PendingTransaction{TxID: txID, ExecutionID: executionID, Effects: records})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, item := range pending {
		if err := s.ScheduleDelivery(ctx, item.TxID); err != nil {
			return pending, err
		}
	}
	return pending, nil
}

// AbortExecution rolls back an execution's transaction. The host calls it when a
// dispatch fails, so a failed request releases its locks immediately rather than
// waiting for the sweeper.
func (s *SQLEffectStore) AbortExecution(executionID string) {
	s.mu.Lock()
	entry, ok := s.byExecution[executionID]
	if ok {
		delete(s.byExecution, executionID)
		delete(s.byTxID, entry.txID)
	}
	s.mu.Unlock()
	if ok {
		_ = entry.tx.Rollback()
	}
}

func (s *SQLEffectStore) sweep() {
	defer close(s.done)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-abandonedTxTimeout)
			s.mu.Lock()
			var stale []*openTx
			for _, entry := range s.byTxID {
				if entry.started.Before(cutoff) {
					stale = append(stale, entry)
					delete(s.byTxID, entry.txID)
					delete(s.byExecution, entry.executionID)
				}
			}
			s.mu.Unlock()
			for _, entry := range stale {
				_ = entry.tx.Rollback()
				s.log.Warn("rolled back an abandoned effect transaction",
					slog.String("tx_id", entry.txID), slog.String("execution_id", entry.executionID))
			}
		}
	}
}

// Close stops the sweeper and rolls back anything still open. Rolling back is the
// only correct choice: a transaction that was never committed describes work the
// application never promised.
func (s *SQLEffectStore) Close() error {
	close(s.stop)
	<-s.done

	s.mu.Lock()
	entries := make([]*openTx, 0, len(s.byTxID))
	for _, entry := range s.byTxID {
		entries = append(entries, entry)
	}
	s.byTxID = map[string]*openTx{}
	s.byExecution = map[string]*openTx{}
	s.mu.Unlock()

	for _, entry := range entries {
		_ = entry.tx.Rollback()
	}
	return nil
}
