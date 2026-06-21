package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/fh"
)

func (e *Engine) handleIdempotency(ctx context.Context, key, workflowID string, input any) (*Task, bool, error) {
	_ = ctx
	rec, err := e.store.GetIdempotency(key)
	if err != nil {
		return nil, false, nil
	}
	if rec.WorkflowID != workflowID {
		return nil, false, fmt.Errorf("idempotency key used for workflow %s", rec.WorkflowID)
	}
	if rec.InputHash != InputHash(input) {
		return nil, false, fmt.Errorf("idempotency key reused with different input")
	}
	t, err := e.store.Get(rec.TaskID)
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}

func (e *Engine) recordIdempotencyFromRequest(c *fh.Ctx, workflowID string, input any, task *Task) {
	if task == nil || c == nil {
		return
	}
	key := c.Get("Idempotency-Key")
	if key == "" {
		return
	}
	task.IdempotencyKey = key
	_ = e.store.Save(task)
	_ = e.store.PutIdempotency(IdempotencyRecord{Key: key, WorkflowID: workflowID, InputHash: InputHash(input), TaskID: task.ID, CreatedAt: time.Now()})
}
