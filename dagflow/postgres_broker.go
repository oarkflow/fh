package main

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type DurableQueueStore interface {
	EnqueueJob(context.Context, Job) error
	ClaimJob(context.Context, string, string, time.Duration) (Job, error)
	AckJob(context.Context, string) error
	NackJob(context.Context, string, error, time.Duration, int) error
	CompleteJob(context.Context, JobResult) error
	WaitJobResult(context.Context, string) (JobResult, error)
	RecoverExpiredJobs(context.Context) error
}

type PostgresBroker struct {
	store       DurableQueueStore
	workerID    string
	lease       time.Duration
	maxAttempts int
}

func NewPostgresBroker(store DurableQueueStore, workerID string) *PostgresBroker {
	if workerID == "" {
		workerID = "worker-" + newID("pg")
	}
	return &PostgresBroker{store: store, workerID: workerID, lease: 45 * time.Second, maxAttempts: 5}
}
func (b *PostgresBroker) Publish(ctx context.Context, j Job) error { return b.store.EnqueueJob(ctx, j) }
func (b *PostgresBroker) Subscribe(ctx context.Context, workflowID string) (<-chan Job, error) {
	ch := make(chan Job)
	go func() {
		defer close(ch)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			_ = b.store.RecoverExpiredJobs(ctx)
			job, err := b.store.ClaimJob(ctx, workflowID, b.workerID, b.lease)
			if err == nil {
				select {
				case ch <- job:
				case <-ctx.Done():
					return
				}
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				time.Sleep(500 * time.Millisecond)
			}
			select {
			case <-tick.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
func (b *PostgresBroker) Ack(ctx context.Context, jobID string) error {
	return b.store.AckJob(ctx, jobID)
}
func (b *PostgresBroker) Nack(ctx context.Context, jobID string, err error) error {
	return b.store.NackJob(ctx, jobID, err, 2*time.Second, b.maxAttempts)
}
func (b *PostgresBroker) Complete(ctx context.Context, r JobResult) error {
	return b.store.CompleteJob(ctx, r)
}
func (b *PostgresBroker) WaitResult(ctx context.Context, jobID string) (JobResult, error) {
	return b.store.WaitJobResult(ctx, jobID)
}
