package main

import (
	"context"
	"errors"
	"time"
)

type Job struct {
	ID         string         `json:"id"`
	TaskID     string         `json:"task_id"`
	WorkflowID string         `json:"workflow_id"`
	NodeID     string         `json:"node_id"`
	Handler    string         `json:"handler"`
	Type       NodeType       `json:"type"`
	Params     map[string]any `json:"params,omitempty"`
	Input      any            `json:"input,omitempty"`
	Attempt    int            `json:"attempt"`
	CreatedAt  time.Time      `json:"created_at"`
}

type JobResult struct {
	JobID      string    `json:"job_id"`
	TaskID     string    `json:"task_id"`
	WorkflowID string    `json:"workflow_id"`
	NodeID     string    `json:"node_id"`
	Result     any       `json:"result,omitempty"`
	Error      string    `json:"error,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

type Broker interface {
	Publish(context.Context, Job) error
	Subscribe(context.Context, string) (<-chan Job, error)
	Ack(context.Context, string) error
	Nack(context.Context, string, error) error
	Complete(context.Context, JobResult) error
	WaitResult(context.Context, string) (JobResult, error)
}

type MemoryBroker struct {
	mu      chan func()
	queues  map[string]chan Job
	waiters map[string][]chan JobResult
	results map[string]JobResult
}

func NewMemoryBroker() *MemoryBroker {
	b := &MemoryBroker{mu: make(chan func()), queues: map[string]chan Job{}, waiters: map[string][]chan JobResult{}, results: map[string]JobResult{}}
	go func() {
		for f := range b.mu {
			f()
		}
	}()
	return b
}
func (b *MemoryBroker) locked(f func()) {
	done := make(chan struct{})
	b.mu <- func() { f(); close(done) }
	<-done
}
func (b *MemoryBroker) queue(name string) chan Job {
	var q chan Job
	b.locked(func() {
		q = b.queues[name]
		if q == nil {
			q = make(chan Job, 1024)
			b.queues[name] = q
		}
	})
	return q
}
func (b *MemoryBroker) Publish(ctx context.Context, j Job) error {
	select {
	case b.queue(j.WorkflowID) <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *MemoryBroker) Subscribe(ctx context.Context, workflowID string) (<-chan Job, error) {
	return b.queue(workflowID), nil
}
func (b *MemoryBroker) Ack(context.Context, string) error         { return nil }
func (b *MemoryBroker) Nack(context.Context, string, error) error { return nil }
func (b *MemoryBroker) Complete(ctx context.Context, r JobResult) error {
	var waiters []chan JobResult
	b.locked(func() { b.results[r.JobID] = r; waiters = b.waiters[r.JobID]; delete(b.waiters, r.JobID) })
	for _, ch := range waiters {
		select {
		case ch <- r:
		default:
		}
		close(ch)
	}
	return nil
}
func (b *MemoryBroker) WaitResult(ctx context.Context, jobID string) (JobResult, error) {
	var existing JobResult
	var found bool
	ch := make(chan JobResult, 1)
	b.locked(func() {
		existing, found = b.results[jobID]
		if !found {
			b.waiters[jobID] = append(b.waiters[jobID], ch)
		}
	})
	if found {
		return existing, nil
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return JobResult{}, ctx.Err()
	}
}

type localRequest struct {
	job    Job
	result chan JobResult
}
type LocalQueue struct {
	engine  *Engine
	workers int
	jobs    chan localRequest
}

func NewLocalQueue(e *Engine, workers int) *LocalQueue {
	if workers <= 0 {
		workers = 1
	}
	return &LocalQueue{engine: e, workers: workers, jobs: make(chan localRequest, 1024)}
}
func (q *LocalQueue) Start(ctx context.Context) {
	for i := 0; i < q.workers; i++ {
		go func() {
			for {
				select {
				case req := <-q.jobs:
					jr := q.engine.executeJob(ctx, req.job)
					if req.result != nil {
						req.result <- jr
						close(req.result)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}
func (q *LocalQueue) SubmitAndWait(ctx context.Context, job Job) (JobResult, error) {
	ch := make(chan JobResult, 1)
	select {
	case q.jobs <- localRequest{job: job, result: ch}:
	case <-ctx.Done():
		return JobResult{}, ctx.Err()
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return JobResult{}, ctx.Err()
	}
}
func (q *LocalQueue) SubmitFireAndForget(ctx context.Context, job Job) error {
	select {
	case q.jobs <- localRequest{job: job}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) newJob(task *Task, node *Node, input any, attempt int) Job {
	return Job{ID: newID("job"), TaskID: task.ID, WorkflowID: task.WorkflowID, NodeID: node.ID, Handler: node.Handler, Type: node.Type, Params: node.Params, Input: input, Attempt: attempt, CreatedAt: time.Now()}
}
func (e *Engine) executeJob(ctx context.Context, job Job) JobResult {
	started := time.Now()
	leaseID := ""
	if s, ok := e.store.(ExtendedStore); ok {
		leaseID = newID("lease")
		_ = s.CreateLease(WorkerLease{ID: leaseID, TaskID: job.TaskID, NodeID: job.NodeID, JobID: job.ID, WorkerID: "worker-" + job.WorkflowID, ExpiresAt: time.Now().Add(30 * time.Second), BeatAt: time.Now()})
		done := make(chan struct{})
		defer close(done)
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					_ = s.HeartbeatLease(leaseID, 30*time.Second)
				case <-done:
					return
				}
			}
		}()
	}
	wf := &Workflow{ID: job.WorkflowID}
	task := &Task{ID: job.TaskID, WorkflowID: job.WorkflowID, NodeResults: map[string]any{}}
	node := &Node{ID: job.NodeID, Type: job.Type, Handler: job.Handler, Params: job.Params, Mode: ModeInline, Await: true}
	res, err := e.executeHandler(ctx, wf, task, node, job.Input, job.Attempt)
	jr := JobResult{JobID: job.ID, TaskID: job.TaskID, WorkflowID: job.WorkflowID, NodeID: job.NodeID, Result: res, StartedAt: started, FinishedAt: time.Now()}
	if err != nil {
		jr.Error = err.Error()
	}
	if s, ok := e.store.(ExtendedStore); ok && leaseID != "" {
		_ = s.DeleteLease(leaseID)
	}
	return jr
}
func (e *Engine) StartDistributedWorker(ctx context.Context, workflowID string, concurrency int) {
	if concurrency <= 0 {
		concurrency = 1
	}
	jobs, err := e.broker.Subscribe(ctx, workflowID)
	if err != nil {
		return
	}
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case job := <-jobs:
					jr := e.executeJob(ctx, job)
					if jr.Error != "" {
						_ = e.broker.Nack(ctx, job.ID, errors.New(jr.Error))
					} else {
						_ = e.broker.Ack(ctx, job.ID)
					}
					_ = e.broker.Complete(ctx, jr)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}
