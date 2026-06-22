package dagflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
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
	mu          sync.RWMutex
	consumers   map[string]*memoryConsumer
	queues      map[string]QueueConfig
	events      []BrokerEvent
	eventSeq    int64
}

func NewPostgresBroker(store DurableQueueStore, workerID string) *PostgresBroker {
	if workerID == "" {
		workerID = "worker-" + newID("pg")
	}
	return &PostgresBroker{store: store, workerID: workerID, lease: 45 * time.Second, maxAttempts: 5, consumers: map[string]*memoryConsumer{}, queues: map[string]QueueConfig{}}
}

func (b *PostgresBroker) record(event BrokerEvent) {
	if event.At.IsZero() {
		event.At = time.Now()
	}
	if event.ID == "" {
		b.eventSeq++
		event.ID = fmt.Sprintf("broker_evt_%d", b.eventSeq)
	}
	if event.Component == "" {
		event.Component = "postgres_broker"
	}
	b.events = append(b.events, event)
	if len(b.events) > 1000 {
		copy(b.events, b.events[len(b.events)-1000:])
		b.events = b.events[:1000]
	}
	if event.Error != "" {
		if event.Stack != "" {
			log.Printf("dagflow broker component=%s event=%s queue=%s consumer=%s job=%s task=%s workflow=%s node=%s attempt=%d error=%q message=%q\n%s", event.Component, event.Event, event.Queue, event.ConsumerID, event.JobID, event.TaskID, event.WorkflowID, event.NodeID, event.Attempt, event.Error, event.Message, event.Stack)
		} else {
			log.Printf("dagflow broker component=%s event=%s queue=%s consumer=%s job=%s task=%s workflow=%s node=%s attempt=%d error=%q message=%q", event.Component, event.Event, event.Queue, event.ConsumerID, event.JobID, event.TaskID, event.WorkflowID, event.NodeID, event.Attempt, event.Error, event.Message)
		}
		return
	}
	log.Printf("dagflow broker component=%s event=%s queue=%s consumer=%s job=%s task=%s workflow=%s node=%s attempt=%d status=%s message=%q", event.Component, event.Event, event.Queue, event.ConsumerID, event.JobID, event.TaskID, event.WorkflowID, event.NodeID, event.Attempt, event.Status, event.Message)
}

func (b *PostgresBroker) BrokerEvents(limit int) []BrokerEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if limit <= 0 || limit > len(b.events) {
		limit = len(b.events)
	}
	out := make([]BrokerEvent, 0, limit)
	for i := len(b.events) - limit; i < len(b.events); i++ {
		if i >= 0 {
			out = append(out, b.events[i])
		}
	}
	return out
}

func (b *PostgresBroker) EnsureQueue(cfg QueueConfig) error {
	if cfg.ID == "" {
		return errors.New("queue id is required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queues[cfg.ID] = cfg
	b.record(BrokerEvent{Event: "queue.ensure", Queue: cfg.ID, Message: "queue ready", Data: map[string]any{"capacity": cfg.Capacity, "max_attempts": cfg.MaxAttempts, "dlq": cfg.DLQ}})
	return nil
}

func (b *PostgresBroker) Publish(ctx context.Context, j Job) error {
	queue := j.Queue
	if queue == "" {
		queue = j.WorkflowID
	}
	return b.PublishToQueue(ctx, queue, j)
}
func (b *PostgresBroker) PublishToQueue(ctx context.Context, queue string, j Job) error {
	if queue == "" {
		queue = j.WorkflowID
	}
	if j.ID == "" {
		j.ID = newID("job")
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = time.Now()
	}
	if j.Kind == "" {
		j.Kind = JobKindNode
	}
	j.Queue = queue
	b.mu.Lock()
	if _, ok := b.queues[queue]; !ok {
		b.queues[queue] = QueueConfig{ID: queue}
	}
	b.record(BrokerEvent{Event: "job.published", Queue: queue, JobID: j.ID, TaskID: j.TaskID, WorkflowID: j.WorkflowID, NodeID: j.NodeID, Attempt: j.Attempt, Status: "queued", Message: "job published"})
	b.mu.Unlock()
	return b.store.EnqueueJob(ctx, j)
}
func (b *PostgresBroker) Subscribe(ctx context.Context, workflowID string) (<-chan Job, error) {
	return b.SubscribeQueue(ctx, workflowID)
}
func (b *PostgresBroker) SubscribeQueue(ctx context.Context, queue string) (<-chan Job, error) {
	b.mu.Lock()
	b.record(BrokerEvent{Event: "queue.subscribe", Queue: queue, Message: "subscription opened"})
	b.mu.Unlock()
	ch := make(chan Job)
	go func() {
		defer close(ch)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			_ = b.store.RecoverExpiredJobs(ctx)
			job, err := b.store.ClaimJob(ctx, queue, b.workerID, b.lease)
			if err == nil {
				b.mu.Lock()
				b.record(BrokerEvent{Event: "job.claimed", Queue: queue, JobID: job.ID, TaskID: job.TaskID, WorkflowID: job.WorkflowID, NodeID: job.NodeID, Attempt: job.Attempt, Status: "running", Message: "job claimed"})
				b.mu.Unlock()
				if job.Queue == "" {
					job.Queue = queue
				}
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
	b.mu.Lock()
	b.record(BrokerEvent{Event: "job.acked", JobID: jobID, Status: "acked", Message: "job acknowledged"})
	b.mu.Unlock()
	return b.store.AckJob(ctx, jobID)
}
func (b *PostgresBroker) Nack(ctx context.Context, jobID string, err error) error {
	b.mu.Lock()
	b.record(BrokerEvent{Event: "job.nacked", JobID: jobID, Status: "nacked", Error: fmt.Sprint(err), Message: "job negatively acknowledged"})
	b.mu.Unlock()
	return b.store.NackJob(ctx, jobID, err, 2*time.Second, b.maxAttempts)
}

func (b *PostgresBroker) nackJob(ctx context.Context, job Job, err error) bool {
	b.mu.RLock()
	cfg := b.queues[job.Queue]
	b.mu.RUnlock()
	maxAttempts := queueMaxAttempts(job, cfg, b.maxAttempts)
	if IsPermanentError(err) || isPermanentErrorText(err.Error()) {
		maxAttempts = 1
	}
	terminal := job.Attempt >= maxAttempts
	if delay := queueRetryDelay(job.Attempt); !terminal {
		b.mu.Lock()
		b.record(BrokerEvent{Event: "job.retry.scheduled", Queue: job.Queue, JobID: job.ID, TaskID: job.TaskID, WorkflowID: job.WorkflowID, NodeID: job.NodeID, Attempt: job.Attempt + 1, Status: "retry", Error: fmt.Sprint(err), Message: "job retry scheduled", Data: map[string]any{"delay": delay.String()}})
		b.mu.Unlock()
		_ = b.store.NackJob(ctx, job.ID, err, delay, maxAttempts)
		return false
	}
	b.mu.Lock()
	b.record(BrokerEvent{Event: "job.dead", Queue: job.Queue, JobID: job.ID, TaskID: job.TaskID, WorkflowID: job.WorkflowID, NodeID: job.NodeID, Attempt: job.Attempt, Status: "dead", Error: fmt.Sprint(err), Message: "job reached terminal failure"})
	b.mu.Unlock()
	_ = b.store.NackJob(ctx, job.ID, err, 0, maxAttempts)
	if cfg.DLQ != "" {
		if !cfg.DLQBusinessRejections && IsPermanentError(err) && errors.Is(err, ErrTaskRejected) {
			return true
		}
		dlq := job
		dlq.ID = newID("job")
		dlq.Queue = cfg.DLQ
		dlq.Attempt = 1
		dlq.CreatedAt = time.Now()
		dlq.AvailableAt = time.Now()
		_ = b.PublishToQueue(context.Background(), cfg.DLQ, dlq)
	}
	return true
}
func (b *PostgresBroker) Complete(ctx context.Context, r JobResult) error {
	b.mu.Lock()
	event := "job.completed"
	status := "completed"
	message := "job completed successfully"
	if r.Error != "" {
		event = "job.result.failed"
		status = "failed"
		message = "job result released with failure"
	}
	b.record(BrokerEvent{Event: event, Queue: r.Queue, JobID: r.JobID, TaskID: r.TaskID, WorkflowID: r.WorkflowID, NodeID: r.NodeID, Status: status, Error: r.Error, Stack: r.Stack, Message: message})
	b.mu.Unlock()
	return b.store.CompleteJob(ctx, r)
}
func (b *PostgresBroker) WaitResult(ctx context.Context, jobID string) (JobResult, error) {
	return b.store.WaitJobResult(ctx, jobID)
}
func (b *PostgresBroker) StartConsumer(ctx context.Context, cfg QueueConsumerConfig, handler QueueConsumerHandler) error {
	if cfg.ID == "" {
		return errors.New("consumer id is required")
	}
	if cfg.Queue == "" {
		return errors.New("consumer queue is required")
	}
	if handler == nil {
		return errors.New("consumer handler is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.Enabled != nil && !*cfg.Enabled {
		return nil
	}
	b.mu.Lock()
	if old := b.consumers[cfg.ID]; old != nil && old.info.Status != ConsumerStopped {
		b.mu.Unlock()
		return fmt.Errorf("consumer %s already running", cfg.ID)
	}
	if _, ok := b.queues[cfg.Queue]; !ok {
		b.queues[cfg.Queue] = QueueConfig{ID: cfg.Queue}
	}
	cctx, cancel := context.WithCancel(ctx)
	now := time.Now()
	mc := &memoryConsumer{info: QueueConsumerInfo{ID: cfg.ID, Queue: cfg.Queue, Workflow: cfg.Workflow, Concurrency: cfg.Concurrency, Status: ConsumerRunning, StartedAt: now, UpdatedAt: now, LastHeartbeat: now, Workers: cfg.Concurrency, LastEvent: "started"}, cancel: cancel, heartbeatInterval: parseDurationDefault(cfg.HeartbeatInterval, 15*time.Second), heartbeatLogInterval: parseDurationDefault(cfg.HeartbeatLogInterval, 30*time.Second), debug: cfg.Debug}
	b.consumers[cfg.ID] = mc
	b.record(BrokerEvent{Event: "consumer.started", Queue: cfg.Queue, ConsumerID: cfg.ID, WorkflowID: cfg.Workflow, Status: string(ConsumerRunning), Message: "consumer started", Data: map[string]any{"concurrency": cfg.Concurrency, "worker_id": b.workerID}})
	b.mu.Unlock()
	jobs, err := b.SubscribeQueue(cctx, cfg.Queue)
	if err != nil {
		return err
	}
	for i := 0; i < cfg.Concurrency; i++ {
		go b.consumerLoop(cctx, cfg.ID, jobs, handler)
	}
	return nil
}

func (b *PostgresBroker) consumerHeartbeatInterval(id string) time.Duration {
	const defaultHeartbeatInterval = 15 * time.Second

	if b == nil {
		return defaultHeartbeatInterval
	}

	// Keep this method intentionally conservative for PostgresBroker.
	// Consumer-specific heartbeat config is currently owned by Engine / queue config.
	// PostgresBroker only needs a safe broker-level fallback so workers compile and run.
	return defaultHeartbeatInterval
}

func (b *PostgresBroker) consumerLoop(ctx context.Context, id string, jobs <-chan Job, handler QueueConsumerHandler) {
	b.consumerEvent(id, "consumer.worker.started", "worker loop started", nil)
	heartbeat := time.NewTicker(b.consumerHeartbeatInterval(id))
	defer heartbeat.Stop()
	defer b.consumerEvent(id, "consumer.worker.stopped", "worker loop stopped", nil)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		b.mu.RLock()
		status := ConsumerStopped
		if c := b.consumers[id]; c != nil {
			status = c.info.Status
		}
		b.mu.RUnlock()
		if status == ConsumerStopped {
			return
		}
		if status == ConsumerPaused {
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return
			}
		}
		select {
		case <-heartbeat.C:
			b.consumerHeartbeat(id)
		case job, ok := <-jobs:
			if !ok {
				b.consumerEvent(id, "consumer.queue.closed", "job channel closed", nil)
				return
			}
			b.consumerJobStarted(id, job)
			jr := safeHandleJob(ctx, handler, job)
			if jr.Queue == "" {
				jr.Queue = job.Queue
			}
			if jr.Error != "" {
				terminal := b.nackJob(ctx, job, errors.New(jr.Error))
				b.bumpConsumer(id, false)
				b.consumerJobFinished(id, job, jr)
				if terminal {
					_ = b.Complete(ctx, jr)
				}
				continue
			}
			_ = b.Ack(ctx, job.ID)
			b.bumpConsumer(id, true)
			b.consumerJobFinished(id, job, jr)
			_ = b.Complete(ctx, jr)
		case <-ctx.Done():
			return
		}
	}
}

func (b *PostgresBroker) consumerHeartbeat(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.consumers[id]; c != nil {
		now := time.Now()
		c.info.LastHeartbeat = now
		c.info.UpdatedAt = now
		c.info.LastEvent = "heartbeat"
		shouldLog := c.debug
		if c.heartbeatLogInterval <= 0 {
			c.heartbeatLogInterval = 30 * time.Second
		}
		if !shouldLog && (c.lastHeartbeatLog.IsZero() || now.Sub(c.lastHeartbeatLog) >= c.heartbeatLogInterval) {
			shouldLog = true
		}
		if shouldLog {
			c.lastHeartbeatLog = now
			b.record(BrokerEvent{Event: "consumer.heartbeat", Queue: c.info.Queue, ConsumerID: id, WorkflowID: c.info.Workflow, Status: string(c.info.Status), Message: "consumer heartbeat"})
		}
	}
}

func (b *PostgresBroker) consumerEvent(id, event, message string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var be BrokerEvent
	be.Event = event
	be.ConsumerID = id
	be.Message = message
	if err != nil {
		be.Error = err.Error()
	}
	if c := b.consumers[id]; c != nil {
		c.info.LastHeartbeat = time.Now()
		c.info.UpdatedAt = c.info.LastHeartbeat
		c.info.LastEvent = event
		if err != nil {
			c.info.LastError = err.Error()
		}
		be.Queue = c.info.Queue
		be.WorkflowID = c.info.Workflow
		be.Status = string(c.info.Status)
	}
	b.record(be)
}

func (b *PostgresBroker) consumerJobStarted(id string, job Job) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.consumers[id]; c != nil {
		now := time.Now()
		c.info.InFlight++
		c.info.LastHeartbeat = now
		c.info.UpdatedAt = now
		c.info.LastJobAt = now
		c.info.LastJobID = job.ID
		c.info.LastEvent = "job.started"
		b.record(BrokerEvent{Event: "consumer.job.started", Queue: c.info.Queue, ConsumerID: id, JobID: job.ID, TaskID: job.TaskID, WorkflowID: job.WorkflowID, NodeID: job.NodeID, Attempt: job.Attempt, Status: "running", Message: "consumer picked job"})
	}
}

func (b *PostgresBroker) consumerJobFinished(id string, job Job, jr JobResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.consumers[id]; c != nil {
		now := time.Now()
		if c.info.InFlight > 0 {
			c.info.InFlight--
		}
		c.info.LastHeartbeat = now
		c.info.UpdatedAt = now
		c.info.LastJobAt = now
		c.info.LastJobID = job.ID
		c.info.LastEvent = "job.finished"
		status := "succeeded"
		if jr.Error != "" {
			status = "failed"
			c.info.LastError = jr.Error
		}
		b.record(BrokerEvent{Event: "consumer.job.finished", Queue: c.info.Queue, ConsumerID: id, JobID: job.ID, TaskID: job.TaskID, WorkflowID: job.WorkflowID, NodeID: job.NodeID, Attempt: job.Attempt, Status: status, Error: jr.Error, Stack: jr.Stack, Message: "consumer finished job"})
	}
}

func (b *PostgresBroker) bumpConsumer(id string, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.consumers[id]; c != nil {
		if ok {
			c.info.Processed++
		} else {
			c.info.Failed++
		}
		c.info.UpdatedAt = time.Now()
		c.info.LastHeartbeat = c.info.UpdatedAt
	}
}
func (b *PostgresBroker) PauseConsumer(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.consumers[id]
	if c == nil {
		return fmt.Errorf("consumer %s not found", id)
	}
	if c.info.Status == ConsumerStopped {
		return fmt.Errorf("consumer %s is stopped", id)
	}
	c.info.Status = ConsumerPaused
	c.info.UpdatedAt = time.Now()
	c.info.LastEvent = "paused"
	b.record(BrokerEvent{Event: "consumer.paused", Queue: c.info.Queue, ConsumerID: id, WorkflowID: c.info.Workflow, Status: string(c.info.Status), Message: "consumer paused"})
	return nil
}
func (b *PostgresBroker) ResumeConsumer(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.consumers[id]
	if c == nil {
		return fmt.Errorf("consumer %s not found", id)
	}
	if c.info.Status == ConsumerStopped {
		return fmt.Errorf("consumer %s is stopped", id)
	}
	c.info.Status = ConsumerRunning
	c.info.UpdatedAt = time.Now()
	c.info.LastEvent = "resumed"
	b.record(BrokerEvent{Event: "consumer.resumed", Queue: c.info.Queue, ConsumerID: id, WorkflowID: c.info.Workflow, Status: string(c.info.Status), Message: "consumer resumed"})
	return nil
}
func (b *PostgresBroker) StopConsumer(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.consumers[id]
	if c == nil {
		return fmt.Errorf("consumer %s not found", id)
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.info.Status = ConsumerStopped
	c.info.UpdatedAt = time.Now()
	c.info.LastEvent = "stopped"
	b.record(BrokerEvent{Event: "consumer.stopped", Queue: c.info.Queue, ConsumerID: id, WorkflowID: c.info.Workflow, Status: string(c.info.Status), Message: "consumer stopped"})
	return nil
}
func (b *PostgresBroker) ListConsumers() []QueueConsumerInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]QueueConsumerInfo, 0, len(b.consumers))
	for _, c := range b.consumers {
		out = append(out, c.info)
	}
	return out
}
func (b *PostgresBroker) ListQueues() []QueueInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]QueueInfo, 0, len(b.queues))
	for _, q := range b.queues {
		consumers := 0
		for _, c := range b.consumers {
			if c.info.Queue == q.ID && c.info.Status != ConsumerStopped {
				consumers++
			}
		}
		out = append(out, QueueInfo{ID: q.ID, Capacity: q.Capacity, Consumers: consumers, MaxAttempts: q.MaxAttempts, DLQ: q.DLQ})
	}
	return out
}
