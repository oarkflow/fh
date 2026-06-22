package dagflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type QueueSubmitOptions struct {
	Queue       string        `json:"queue,omitempty"`
	Await       bool          `json:"await,omitempty"`
	Timeout     time.Duration `json:"timeout,omitempty"`
	Priority    int           `json:"priority,omitempty"`
	MaxAttempts int           `json:"max_attempts,omitempty"`
}

func (e *Engine) EnqueueWorkflow(ctx context.Context, workflowID string, input any, opts QueueSubmitOptions) (*Task, error) {
	wf, err := e.workflow(workflowID)
	if err != nil {
		return nil, err
	}
	input, err = e.applyWorkflowInput(ctx, wf, input)
	if err != nil {
		return nil, err
	}
	queue := opts.Queue
	if queue == "" {
		queue = workflowID
	}
	task := newTask(workflowID, input)
	task.Status = TaskPending
	task.WorkflowVersion = wf.Version
	task.DefinitionHash = wf.Hash
	e.metrics.Inc("workflow_enqueued_total")
	log.Printf("dagflow queue enqueue requested workflow=%s queue=%s await=%v", workflowID, queue, opts.Await)
	e.audit(task, "queue.task.enqueued", "workflow task enqueued", map[string]any{"queue": queue, "input": Redact(input)})
	if err := e.applyTaskRules(ctx, wf, task, nil, "task.created", input); err != nil {
		e.finishTask(task, err)
		return task, err
	}
	if err := e.store.Create(task); err != nil {
		return nil, err
	}
	job := Job{ID: newID("job"), Kind: JobKindWorkflow, Queue: queue, TaskID: task.ID, WorkflowID: workflowID, NodeID: QueueWorkflowNode, Handler: workflowID, Type: NodeWorkflow, Input: input, Attempt: 1, MaxAttempts: opts.MaxAttempts, Priority: opts.Priority, CreatedAt: time.Now()}
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	if mb, ok := e.broker.(ManagedBroker); ok {
		if err := mb.PublishToQueue(ctx, queue, job); err != nil {
			log.Printf("dagflow queue enqueue failed workflow=%s queue=%s task=%s job=%s error=%v", workflowID, queue, task.ID, job.ID, err)
			return task, err
		}
	} else if err := e.broker.Publish(ctx, job); err != nil {
		log.Printf("dagflow queue enqueue failed workflow=%s queue=%s task=%s job=%s error=%v", workflowID, queue, task.ID, job.ID, err)
		return task, err
	}
	log.Printf("dagflow queue enqueue accepted workflow=%s queue=%s task=%s job=%s await=%v", workflowID, queue, task.ID, job.ID, opts.Await)
	if !opts.Await {
		return task, nil
	}
	log.Printf("dagflow queue await result workflow=%s queue=%s task=%s job=%s", workflowID, queue, task.ID, job.ID)
	jr, err := e.broker.WaitResult(ctx, job.ID)
	if err != nil {
		return task, err
	}
	if jr.Error != "" {
		fresh, getErr := e.store.Get(task.ID)
		if getErr == nil {
			task = fresh
		}
		return task, errors.New(jr.Error)
	}
	fresh, err := e.store.Get(task.ID)
	if err == nil {
		task = fresh
	}
	return task, nil
}

func (e *Engine) StartWorkflowQueueConsumer(ctx context.Context, cfg QueueConsumerConfig) error {
	log.Printf("dagflow consumer configure requested id=%s queue=%s workflow=%s concurrency=%d", cfg.ID, cfg.Queue, cfg.Workflow, cfg.Concurrency)
	if cfg.Workflow == "" {
		return fmt.Errorf("consumer %s requires workflow", cfg.ID)
	}
	if cfg.Queue == "" {
		cfg.Queue = cfg.Workflow
	}
	if cfg.ID == "" {
		cfg.ID = "workflow-consumer:" + cfg.Queue + ":" + cfg.Workflow
	}
	if _, err := e.workflow(cfg.Workflow); err != nil {
		return err
	}
	mb, ok := e.broker.(ManagedBroker)
	if !ok {
		return fmt.Errorf("broker does not support managed consumers")
	}
	err := mb.StartConsumer(ctx, cfg, e.executeJob)
	if err != nil {
		log.Printf("dagflow consumer configure failed id=%s queue=%s workflow=%s error=%v", cfg.ID, cfg.Queue, cfg.Workflow, err)
		return err
	}
	log.Printf("dagflow consumer configured id=%s queue=%s workflow=%s", cfg.ID, cfg.Queue, cfg.Workflow)
	return nil
}

func (e *Engine) QueueInfo() []QueueInfo {
	if mb, ok := e.broker.(ManagedBroker); ok {
		return mb.ListQueues()
	}
	return nil
}

func (e *Engine) ConsumerInfo() []QueueConsumerInfo {
	if mb, ok := e.broker.(ManagedBroker); ok {
		return mb.ListConsumers()
	}
	return nil
}

func (e *Engine) PauseConsumer(id string) error {
	if mb, ok := e.broker.(ManagedBroker); ok {
		return mb.PauseConsumer(id)
	}
	return fmt.Errorf("broker does not support managed consumers")
}

func (e *Engine) ResumeConsumer(id string) error {
	if mb, ok := e.broker.(ManagedBroker); ok {
		return mb.ResumeConsumer(id)
	}
	return fmt.Errorf("broker does not support managed consumers")
}

func (e *Engine) StopConsumer(id string) error {
	if mb, ok := e.broker.(ManagedBroker); ok {
		return mb.StopConsumer(id)
	}
	return fmt.Errorf("broker does not support managed consumers")
}
