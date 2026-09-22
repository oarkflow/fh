package main

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/graph"
)

// AuditObserver writes the execution trail to the database.
//
// It is attached as an Observer so that execution outcomes are recorded without any
// intent having to remember to do it. Policy decisions arrive by a second route:
// RecordDecision, called by the capabilities that make them.
//
// That split is not a design preference, it is what the kernel currently emits. The
// Observer interface has DecisionMade and EffectCommitted methods, and this type
// implements them, but ref/execution never calls either — it emits NodeStarted,
// NodeFinished and ExecutionFinished. Implementing the unused two keeps the trail
// correct if that changes; relying on them alone would have produced an audit table
// with no decisions in it and nothing to indicate that anything was missing.
//
// Rows are batched through a bounded channel. If the channel fills — a database
// slower than the request rate — events are dropped and counted rather than blocking
// execution. That is a deliberate choice for this example, and the wrong one for a
// regulated audit trail: there, the observer belongs in the Critical tier where a
// failure to record is a failure to serve. The counter makes the trade visible
// instead of silent.
type AuditObserver struct {
	db      *sql.DB
	log     *slog.Logger
	events  chan auditEvent
	dropped int64
	mu      sync.Mutex
	wg      sync.WaitGroup
	stop    chan struct{}
}

type auditEvent struct {
	intent     string
	policy     string
	verdict    string
	message    string
	durationMs float64
}

func NewAuditObserver(db *sql.DB, log *slog.Logger) *AuditObserver {
	observer := &AuditObserver{
		db:     db,
		log:    log,
		events: make(chan auditEvent, 1024),
		stop:   make(chan struct{}),
	}
	observer.wg.Add(1)
	go observer.run()
	return observer
}

func (a *AuditObserver) run() {
	defer a.wg.Done()
	batch := make([]auditEvent, 0, 64)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		a.write(ctx, batch)
		cancel()
		batch = batch[:0]
	}

	for {
		select {
		case <-a.stop:
			// Drain what is already queued before returning, so a clean shutdown
			// does not lose the last second of the trail.
			for {
				select {
				case event := <-a.events:
					batch = append(batch, event)
					continue
				default:
				}
				break
			}
			flush()
			return
		case event := <-a.events:
			batch = append(batch, event)
			if len(batch) >= 64 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (a *AuditObserver) write(ctx context.Context, batch []auditEvent) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		a.log.Warn("audit batch dropped: cannot begin", slog.String("error", err.Error()))
		return
	}
	for _, event := range batch {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO audit_log (execution_id,intent,policy,verdict,message,duration_ms)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			"", event.intent, nullable(event.policy), nullable(event.verdict),
			nullable(event.message), event.durationMs); err != nil {
			_ = tx.Rollback()
			a.log.Warn("audit batch dropped", slog.String("error", err.Error()))
			return
		}
	}
	if err := tx.Commit(); err != nil {
		a.log.Warn("audit batch dropped: cannot commit", slog.String("error", err.Error()))
	}
}

// RecordDecision is how a policy capability puts its verdict in the trail. It is
// called explicitly because the kernel does not emit decision events; see the type's
// documentation.
func (a *AuditObserver) RecordDecision(policy, verdict, message string) {
	a.emit(auditEvent{policy: policy, verdict: verdict, message: message})
}

func (a *AuditObserver) emit(event auditEvent) {
	select {
	case a.events <- event:
	default:
		a.mu.Lock()
		a.dropped++
		a.mu.Unlock()
	}
}

// Dropped reports how many audit events were lost to backpressure. /healthz serves
// it, because a number nobody can see is not a warning.
func (a *AuditObserver) Dropped() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dropped
}

func (a *AuditObserver) Close() error {
	close(a.stop)
	a.wg.Wait()
	return nil
}

// --- observer.Observer ------------------------------------------------------

func (a *AuditObserver) NodeStarted(graph.NodeInfo) {}

func (a *AuditObserver) NodeFinished(graph.NodeInfo, error) {}

// DecisionMade implements the Observer interface. The kernel does not call it
// today; RecordDecision is what actually populates the trail.
func (a *AuditObserver) DecisionMade(policy string, verdict uint8, message string) {
	a.RecordDecision(policy, verdictName(verdict), message)
}

func verdictName(verdict uint8) string {
	if execution.Verdict(verdict) == execution.VerdictDeny {
		return "deny"
	}
	return "allow"
}

func (a *AuditObserver) EffectCommitted(string, error) {}

func (a *AuditObserver) ExecutionFinished(intentName string, durationMs float64, err error) {
	event := auditEvent{intent: intentName, durationMs: durationMs}
	if err != nil {
		event.verdict = "failed"
		event.message = err.Error()
	}
	a.emit(event)
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
