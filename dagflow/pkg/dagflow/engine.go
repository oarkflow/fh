package dagflow

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

type Engine struct {
	mu              sync.RWMutex
	flows           map[string]*Workflow
	chains          map[string]*Chain
	conditions      map[string]ConditionSpec
	schemas         map[string]SchemaDef
	snapshots       map[string][]WorkflowSnapshot
	handlers        map[string]Handler
	scriptHandlers  map[string]string
	dataSources     map[string]DataSourceFunc
	metrics         *MetricsRegistry
	runtime         *ScriptRuntime
	store           TaskStore
	chainStore      ChainStore
	callbacks       []FinalCallback
	local           *LocalQueue
	broker          Broker
	activityLog     chan AuditEvent
	notifier        *NotificationDispatcher
	queueConfigs    []QueueConfig
	consumerConfigs []QueueConsumerConfig
	runtimeCancel   context.CancelFunc
	startedAt       time.Time
	wg              sync.WaitGroup
	taskLocks       sync.Map
}

func NewEngine(store TaskStore, chainStore ChainStore, broker Broker) *Engine {
	if store == nil {
		store = NewMemoryTaskStore()
	}
	if chainStore == nil {
		chainStore = NewMemoryChainStore()
	}
	if broker == nil {
		broker = NewMemoryBroker()
	}
	e := &Engine{flows: map[string]*Workflow{}, chains: map[string]*Chain{}, conditions: map[string]ConditionSpec{}, schemas: map[string]SchemaDef{}, snapshots: map[string][]WorkflowSnapshot{}, handlers: map[string]Handler{}, scriptHandlers: map[string]string{}, dataSources: map[string]DataSourceFunc{}, metrics: NewMetricsRegistry(), store: store, chainStore: chainStore, broker: broker, activityLog: make(chan AuditEvent, 4096), notifier: NewNotificationDispatcher()}
	e.startActivityLogger()
	e.local = NewLocalQueue(e, 8)
	e.runtime = NewScriptRuntime(e)
	return e
}

func (e *Engine) lockTask(taskID string) func() {
	if taskID == "" {
		return func() {}
	}
	v, _ := e.taskLocks.LoadOrStore(taskID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return func() {
		mu.Unlock()
	}
}
func (e *Engine) Register(name string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[name] = h
}
func (e *Engine) Notifier() *NotificationDispatcher {
	return e.notifier
}

func (e *Engine) RegisterNotificationCallback(channelID string, cb NotificationCallback) {
	if e.notifier == nil {
		e.notifier = NewNotificationDispatcher()
	}
	e.notifier.RegisterCallback(channelID, cb)
}

func (e *Engine) RegisterNotificationHandler(t NotificationChannelType, h NotificationHandler) {
	if e.notifier == nil {
		e.notifier = NewNotificationDispatcher()
	}
	e.notifier.RegisterHandler(t, h)
}

func (e *Engine) RegisterNotificationChannel(ch NotificationChannel) error {
	if e.notifier == nil {
		e.notifier = NewNotificationDispatcher()
	}
	return e.notifier.RegisterChannel(ch)
}

func (e *Engine) OnFinal(cb FinalCallback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callbacks = append(e.callbacks, cb)
}
func (e *Engine) Store() TaskStore       { return e.store }
func (e *Engine) ChainStore() ChainStore { return e.chainStore }
func (e *Engine) Broker() Broker         { return e.broker }

func (e *Engine) LoadConfig(cfg *Config) error {
	if cfg != nil {
		e.queueConfigs = append([]QueueConfig(nil), cfg.Queues...)
		e.consumerConfigs = append([]QueueConsumerConfig(nil), cfg.Consumers...)

		if qb, ok := e.broker.(interface{ EnsureQueue(QueueConfig) error }); ok {
			for _, qc := range cfg.Queues {
				if err := qb.EnsureQueue(qc); err != nil {
					log.Printf(
						"dagflow queue ensure failed id=%s error=%v",
						qc.ID,
						err,
					)
					return err
				}

				log.Printf(
					"dagflow queue ensured id=%s capacity=%d max_attempts=%d visibility_timeout=%s dlq=%s",
					qc.ID,
					qc.Capacity,
					qc.MaxAttempts,
					qc.VisibilityTimeout,
					qc.DLQ,
				)
			}
		}
	}
	for _, nc := range cfg.Notifications {
		if err := e.RegisterNotificationChannel(NotificationChannel(nc)); err != nil {
			return err
		}
	}
	for _, sc := range cfg.Schemas {
		if err := e.AddSchema(buildSchema(sc)); err != nil {
			return err
		}
	}
	for _, sc := range cfg.Scripts {
		e.scriptHandlers[sc.ID] = sc.Source
	}
	for _, cc := range cfg.Conditions {
		if err := e.AddCondition(buildCondition(cc)); err != nil {
			return err
		}
	}
	for _, wc := range cfg.Workflows {
		wf, err := buildWorkflow(wc)
		if err != nil {
			return err
		}
		wf.Hash = WorkflowHash(wf)
		if err := e.AddWorkflow(wf); err != nil {
			return err
		}
	}
	for _, cc := range cfg.Chains {
		if err := e.AddChain(buildChain(cc)); err != nil {
			return err
		}
	}
	return e.ValidateChainsAndWorkflowNodes()
}

func (e *Engine) Start(ctx context.Context) error {
	log.Printf("dagflow engine starting workflows=%d queues=%d consumers=%d", len(e.flows), len(e.queueConfigs), len(e.consumerConfigs))
	e.startedAt = time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	e.runtimeCancel = cancel
	e.local.Start(runCtx)
	if mb, ok := e.broker.(ManagedBroker); ok {
		for _, qc := range e.queueConfigs {
			log.Printf("dagflow queue configured id=%s capacity=%d max_attempts=%d dlq=%s", qc.ID, qc.Capacity, qc.MaxAttempts, qc.DLQ)
		}
		for _, cc := range e.consumerConfigs {
			if cc.ID == "" {
				cc.ID = "workflow-consumer:" + cc.Queue + ":" + cc.Workflow
			}
			if cc.Workflow != "" {
				log.Printf("dagflow consumer starting id=%s queue=%s workflow=%s concurrency=%d", cc.ID, cc.Queue, cc.Workflow, cc.Concurrency)
				if err := mb.StartConsumer(runCtx, cc, e.executeJob); err != nil {
					return err
				}
			}
		}
	}
	e.mu.RLock()
	ids := make([]string, 0, len(e.flows))
	for id, wf := range e.flows {
		if wf.Mode == ModeDistributed {
			ids = append(ids, id)
		}
	}
	e.mu.RUnlock()
	for _, id := range ids {
		log.Printf("dagflow distributed worker starting workflow=%s", id)
		e.StartDistributedWorker(runCtx, id, 4)
	}
	log.Printf("dagflow engine started workflows=%d consumers=%d", len(e.flows), len(e.ConsumerInfo()))
	return nil
}

func (e *Engine) AddWorkflow(wf *Workflow) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.flows[wf.ID]; ok {
		return fmt.Errorf("workflow %s already exists", wf.ID)
	}
	e.flows[wf.ID] = wf
	e.snapshots[wf.ID] = append(e.snapshots[wf.ID], WorkflowSnapshot{WorkflowID: wf.ID, Version: wf.Version, Hash: wf.Hash, Workflow: wf, CreatedAt: time.Now()})
	return nil
}
func (e *Engine) AddChain(ch *Chain) error {
	if ch.ID == "" {
		return errors.New("chain id is required")
	}
	if len(ch.Workflows) == 0 {
		return fmt.Errorf("chain %s has no workflows", ch.ID)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.chains[ch.ID]; ok {
		return fmt.Errorf("chain %s already exists", ch.ID)
	}
	e.chains[ch.ID] = ch
	return nil
}
func (e *Engine) ValidateChainsAndWorkflowNodes() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, ch := range e.chains {
		for _, id := range ch.Workflows {
			if e.flows[id] == nil {
				return fmt.Errorf("chain %s references missing workflow %s", ch.ID, id)
			}
		}
		if ch.Condition != "" {
			if _, ok := e.conditions[ch.Condition]; !ok {
				return fmt.Errorf("chain %s references missing condition %s", ch.ID, ch.Condition)
			}
		}
	}
	for _, wf := range e.flows {
		if err := e.validateWorkflowConditionRefsLocked(wf); err != nil {
			return err
		}
		for _, n := range wf.Nodes {
			if n.Type == NodeWorkflow && e.flows[n.Workflow] == nil {
				return fmt.Errorf("workflow %s node %s references missing workflow %s", wf.ID, n.ID, n.Workflow)
			}
		}
	}
	return nil
}

func (e *Engine) validateWorkflowConditionRefsLocked(wf *Workflow) error {
	if wf == nil {
		return nil
	}
	for _, r := range wf.Rules {
		if err := e.validateConditionRefLocked(r.Condition, "workflow %s rule %s", wf.ID, r.ID); err != nil {
			return err
		}
	}
	for _, nr := range wf.Notifications {
		if err := e.validateConditionRefLocked(nr.Condition, "workflow %s notification %s", wf.ID, nr.ID); err != nil {
			return err
		}
	}
	for _, n := range wf.Nodes {
		if err := e.validateConditionRefLocked(n.Condition, "workflow %s node %s", wf.ID, n.ID); err != nil {
			return err
		}
		for _, r := range n.Rules {
			if err := e.validateConditionRefLocked(r.Condition, "workflow %s node %s rule %s", wf.ID, n.ID, r.ID); err != nil {
				return err
			}
		}
		for _, nr := range n.Notifications {
			if err := e.validateConditionRefLocked(nr.Condition, "workflow %s node %s notification %s", wf.ID, n.ID, nr.ID); err != nil {
				return err
			}
		}
	}
	for _, edges := range wf.Outgoing {
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			if err := e.validateConditionRefLocked(edge.Condition, "workflow %s edge %s", wf.ID, edge.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) validateConditionRefLocked(condition, format string, args ...any) error {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return nil
	}
	if _, ok := e.conditions[condition]; ok {
		return nil
	}
	return fmt.Errorf(format+" references missing condition %s", append(args, condition)...)
}
func (e *Engine) workflow(id string) (*Workflow, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	wf := e.flows[id]
	if wf == nil {
		return nil, fmt.Errorf("workflow %s not found", id)
	}
	return wf, nil
}
func (e *Engine) chain(id string) (*Chain, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ch := e.chains[id]
	if ch == nil {
		return nil, fmt.Errorf("chain %s not found", id)
	}
	cp := *ch
	cp.Workflows = append([]string(nil), ch.Workflows...)
	return &cp, nil
}

func newTask(workflowID string, input any) *Task {
	now := time.Now()
	return &Task{ID: newID("task"), WorkflowID: workflowID, Status: TaskRunning, Input: input, NodeStates: map[string]*NodeState{}, NodeResults: map[string]any{}, JoinStates: map[string]*JoinState{}, Visits: map[string]int{}, CreatedAt: now, UpdatedAt: now}
}
func newChainRun(chainID string, workflowIDs []string, input any) *ChainRun {
	now := time.Now()
	return &ChainRun{ID: newID("chainrun"), ChainID: chainID, WorkflowIDs: append([]string(nil), workflowIDs...), Status: TaskRunning, Input: input, CreatedAt: now, UpdatedAt: now}
}

func (e *Engine) RunSync(ctx context.Context, workflowID string, input any) (*Task, error) {
	wf, err := e.workflow(workflowID)
	if err != nil {
		return nil, err
	}
	input, err = e.applyWorkflowInput(ctx, wf, input)
	if err != nil {
		return nil, err
	}
	task := newTask(workflowID, input)
	task.WorkflowVersion = wf.Version
	task.DefinitionHash = wf.Hash
	e.metrics.Inc("workflow_started_total")
	e.audit(task, "task.created", "task created", map[string]any{"input": Redact(input)})
	if err := e.applyTaskRules(ctx, wf, task, nil, "task.created", input); err != nil {
		e.finishTask(task, err)
		return task, err
	}
	if err := e.store.Create(task); err != nil {
		return nil, err
	}
	err = e.executeTask(ctx, wf, task, []RunItem{{NodeID: wf.First, Input: input}})
	if err == nil && task.Status != TaskWaiting && task.Status != TaskPaused && task.Status != TaskCancelled {
		task.Result, err = e.applyWorkflowOutput(ctx, wf, task, task.Result)
	}
	e.finishTask(task, err)
	return task, err
}
func (e *Engine) RunAsync(ctx context.Context, workflowID string, input any) (*Task, error) {
	wf, err := e.workflow(workflowID)
	if err != nil {
		return nil, err
	}
	input, err = e.applyWorkflowInput(ctx, wf, input)
	if err != nil {
		return nil, err
	}
	task := newTask(workflowID, input)
	task.WorkflowVersion = wf.Version
	task.DefinitionHash = wf.Hash
	e.metrics.Inc("workflow_started_total")
	e.audit(task, "task.created", "async task created", map[string]any{"input": Redact(input)})
	if err := e.applyTaskRules(ctx, wf, task, nil, "task.created", input); err != nil {
		e.finishTask(task, err)
		return task, err
	}
	if err := e.store.Create(task); err != nil {
		return nil, err
	}
	runCtx := context.WithoutCancel(ctx)
	e.safeGo(func() {
		err := e.executeTask(runCtx, wf, task, []RunItem{{NodeID: wf.First, Input: input}})
		if err == nil && task.Status != TaskWaiting && task.Status != TaskPaused && task.Status != TaskCancelled {
			task.Result, err = e.applyWorkflowOutput(runCtx, wf, task, task.Result)
		}
		e.finishTask(task, err)
	})
	return task, nil
}

func (e *Engine) RunChainSync(ctx context.Context, chainID string, input any) (*ChainRun, error) {
	ch, err := e.chain(chainID)
	if err != nil {
		return nil, err
	}
	if ch.When != "" || ch.Condition != "" {
		ok, err := e.evalChainCondition(ch, input)
		if err != nil {
			return nil, err
		}
		if !ok {
			run := newChainRun(chainID, ch.Workflows, input)
			run.Status = TaskCompleted
			run.Result = input
			now := time.Now()
			run.CompletedAt = &now
			run.UpdatedAt = now
			_ = e.chainStore.Create(run)
			return run, nil
		}
	}
	return e.runWorkflowIDsSync(ctx, chainID, ch.Workflows, input)
}
func (e *Engine) RunWorkflowIDsSync(ctx context.Context, workflowIDs []string, input any) (*ChainRun, error) {
	return e.runWorkflowIDsSync(ctx, "adhoc", workflowIDs, input)
}
func (e *Engine) runWorkflowIDsSync(ctx context.Context, chainID string, workflowIDs []string, input any) (*ChainRun, error) {
	run := newChainRun(chainID, workflowIDs, input)
	_ = e.chainStore.Create(run)
	current := input
	for _, workflowID := range workflowIDs {
		task, err := e.RunSync(ctx, workflowID, current)
		run.Tasks = append(run.Tasks, task)
		run.UpdatedAt = time.Now()
		_ = e.chainStore.Save(run)
		if err != nil || task.Status == TaskWaiting || task.Status == TaskPaused || task.Status == TaskFailed {
			e.finishChain(run, err)
			return run, err
		}
		current = task.Result
	}
	run.Result = current
	e.finishChain(run, nil)
	return run, nil
}
func (e *Engine) RunChainAsync(ctx context.Context, chainID string, input any) (*ChainRun, error) {
	ch, err := e.chain(chainID)
	if err != nil {
		return nil, err
	}
	run := newChainRun(chainID, ch.Workflows, input)
	_ = e.chainStore.Create(run)
	go func() {
		current := input
		var runErr error
		for _, workflowID := range ch.Workflows {
			task, err := e.RunSync(context.Background(), workflowID, current)
			run.Tasks = append(run.Tasks, task)
			run.UpdatedAt = time.Now()
			_ = e.chainStore.Save(run)
			if err != nil || task.Status == TaskWaiting || task.Status == TaskPaused || task.Status == TaskFailed {
				runErr = err
				break
			}
			current = task.Result
		}
		if runErr == nil {
			run.Result = current
		}
		e.finishChain(run, runErr)
	}()
	return run, nil
}

func (e *Engine) RunStandaloneNode(ctx context.Context, workflowID, nodeID string, input any) (*NodeState, any, error) {
	wf, err := e.workflow(workflowID)
	if err != nil {
		return nil, nil, err
	}
	node := wf.Nodes[nodeID]
	if node == nil {
		return nil, nil, fmt.Errorf("node %s not found", nodeID)
	}
	task := newTask(workflowID, input)
	task.ID = newID("standalone")
	input, err = e.applyNodeInput(ctx, wf, task, node, input)
	if err != nil {
		return nil, nil, err
	}
	res, err := e.runNode(ctx, wf, task, node, input, node.ID)
	if err == nil {
		res, err = e.applyNodeOutput(ctx, wf, task, node, res)
	}
	return task.NodeStates[node.ID], res, err
}

func (e *Engine) executeTask(ctx context.Context, wf *Workflow, task *Task, queue []RunItem) error {
	ensureTaskRuntimeState(task)
	if len(queue) == 0 {
		queue = task.Cursor
	}
	task.Status = TaskRunning
	task.Cursor = queue
	for len(queue) > 0 {
		if task.Status == TaskCancelled || task.Status == TaskPaused || task.Status == TaskWaiting {
			task.Cursor = queue
			_ = e.store.Save(task)
			return nil
		}
		item := queue[0]
		queue = queue[1:]
		task.Cursor = queue
		task.CurrentNode = item.NodeID
		task.CurrentNodes = nodeIDs(queue)
		task.UpdatedAt = time.Now()
		_ = e.store.Save(task)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		node := wf.Nodes[item.NodeID]
		if node == nil {
			return fmt.Errorf("node %s not found", item.NodeID)
		}
		if task.Visits == nil {
			task.Visits = map[string]int{}
		}
		task.Visits[node.ID]++
		if task.Visits[node.ID] > wf.MaxVisits {
			return fmt.Errorf("node %s exceeded max visits %d", node.ID, wf.MaxVisits)
		}
		if node.When != "" || node.Condition != "" {
			ok, err := e.evalNodeCondition(node, task, item.Input)
			if err != nil {
				return fmt.Errorf("node %s guard: %w", node.ID, err)
			}
			if !ok {
				e.markSkipped(task, node, item.Input)
				if node.SkipOnFalse {
					continue
				}
				next, err := e.resolveEdges(ctx, wf, task, node, item.Input, false)
				if err != nil {
					return err
				}
				queue = append(queue, next...)
				continue
			}
		}
		nodeInput, derr := e.applyNodeInput(ctx, wf, task, node, item.Input)
		if derr == nil {
			resumeQueue := append([]RunItem{{NodeID: node.ID, Input: nodeInput, From: item.From, EdgeID: item.EdgeID}}, queue...)
			task.Cursor = resumeQueue
			if rerr := e.applyTaskRules(ctx, wf, task, node, "node.before", nodeInput); rerr != nil {
				if errors.Is(rerr, ErrApprovalRequired) {
					task.Cursor = resumeQueue
					_ = e.store.Save(task)
					return nil
				}
				return rerr
			}
			task.Cursor = queue
		}
		if derr != nil {
			if errors.Is(derr, ErrDataFiltered) {
				e.markSkipped(task, node, item.Input)
				continue
			}
			return fmt.Errorf("node %s input data: %w", node.ID, derr)
		}
		result, err := e.runNode(ctx, wf, task, node, nodeInput, node.ID)
		if err != nil {
			e.recordNodeError(task, node, nodeInput, err)
			if st := task.NodeStates[node.ID]; st != nil {
				e.storeDLQ(task, node, nodeInput, err, st.Attempts)
			}
			if node.ContinueOnError {
				e.audit(task, "node.error.continued", "continuing after node error", map[string]any{"node": node.ID, "error": err.Error()})
				next, rerr := e.resolveEdges(ctx, wf, task, node, map[string]any{"error": err.Error(), "input": nodeInput}, true)
				if rerr != nil {
					return rerr
				}
				queue = append(queue, next...)
				continue
			}
			task.Cursor = queue
			task.FailedNodeID = node.ID
			task.FailedInput = nodeInput
			_ = e.store.Save(task)
			return fmt.Errorf("node %s failed: %w", node.ID, err)
		}
		result, err = e.applyNodeOutput(ctx, wf, task, node, result)
		if err != nil {
			if errors.Is(err, ErrDataFiltered) {
				e.markSkipped(task, node, nodeInput)
				continue
			}
			return fmt.Errorf("node %s output data: %w", node.ID, err)
		}
		if st := task.NodeStates[node.ID]; st != nil {
			st.Result = result
		}
		if rerr := e.applyTaskRules(ctx, wf, task, node, "node.completed", result); rerr != nil {
			if errors.Is(rerr, ErrApprovalRequired) {
				_ = e.store.Save(task)
				return nil
			}
			return rerr
		}
		task.PreviousNode = node.ID
		task.PreviousNodes = append(task.PreviousNodes, node.ID)
		task.LastResult = result
		task.NodeResults[node.ID] = result
		if node.Last || len(wf.Outgoing[node.ID]) == 0 {
			task.Result = result
			continue
		}
		next, err := e.resolveEdges(ctx, wf, task, node, result, false)
		if err != nil {
			return err
		}
		queue = append(queue, next...)
		task.Cursor = queue
		task.CurrentNodes = nodeIDs(queue)
		task.UpdatedAt = time.Now()
		_ = e.store.Save(task)
	}
	return nil
}

func (e *Engine) runNode(parent context.Context, wf *Workflow, task *Task, node *Node, input any, stateKey string) (any, error) {
	ensureTaskRuntimeState(task)
	state := task.NodeStates[stateKey]
	if state == nil {
		state = &NodeState{NodeID: stateKey, Status: NodePending}
		task.NodeStates[stateKey] = state
	}
	inputHash := InputHash(input)
	dedupKey := wf.ID + ":" + wf.Version + ":" + task.ID + ":" + node.ID + ":" + inputHash
	if state.Status == NodeCompleted && state.DedupKey == dedupKey {
		e.audit(task, "node.idempotency.hit", "using completed node result from task state", map[string]any{"node": node.ID, "dedup_key": dedupKey})
		return state.Result, nil
	}
	if ds, ok := e.store.(NodeDedupStore); ok {
		if rec, err := ds.GetNodeDedup(dedupKey); err == nil && rec.Status == "completed" {
			state.Status = NodeCompleted
			state.Result = rec.Result
			state.DedupKey = dedupKey
			task.NodeResults[stateKey] = rec.Result
			e.audit(task, "node.idempotency.hit", "using completed node result from storage", map[string]any{"node": node.ID, "dedup_key": dedupKey})
			return rec.Result, nil
		}
		_ = ds.PutNodeDedup(NodeDedupRecord{DedupKey: dedupKey, TaskID: task.ID, WorkflowID: wf.ID, NodeID: node.ID, InputHash: inputHash, Status: "running"})
	}
	state.Input = input
	state.ExecutionID = newID("exec")
	state.AttemptID = newID("attempt")
	state.DedupKey = dedupKey
	state.Mode = node.Mode
	state.StartedAt = time.Now()
	state.Status = NodeRunning
	state.Error = ""
	e.metrics.Inc("node_started_total")
	e.audit(task, "node.started", "node started", map[string]any{"node": node.ID, "type": node.Type, "mode": node.Mode, "input": input})
	if node.Pause || node.Type == NodePage && node.Handler == "" {
		state.Status = NodeWaiting
		state.FinishedAt = time.Now()
		state.Duration = state.FinishedAt.Sub(state.StartedAt)
		task.Status = TaskWaiting
		task.WaitingNodeID = node.ID
		task.ResumeToken = SignResumeToken(task.ID, wf.ID, node.ID, 24*time.Hour)
		task.Cursor = nil
		e.audit(task, "task.waiting", "task paused at wait/page node", map[string]any{"node": node.ID, "resume_token": task.ResumeToken})
		_ = e.store.Save(task)
		return input, nil
	}
	ctx := parent
	cancel := func() {}
	if node.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, node.Timeout)
	}
	defer cancel()
	attempts := max(1, node.RetryPolicy.MaxAttempts)
	var result any
	var err error
	for i := 0; i < attempts; i++ {
		state.Attempts++
		result, err = e.dispatchNode(ctx, wf, task, node, input, state, i+1)
		if err == nil {
			break
		}
		e.audit(task, "node.retry", "node attempt failed", map[string]any{"node": node.ID, "attempt": i + 1, "error": err.Error()})
		if i+1 < attempts {
			delay := retryDelay(node.RetryPolicy, i+1)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	state.FinishedAt = time.Now()
	state.Duration = state.FinishedAt.Sub(state.StartedAt)
	if err != nil {
		state.Status = NodeFailed
		e.metrics.Inc("node_failed_total")
		state.Error = err.Error()
		if ds, ok := e.store.(NodeDedupStore); ok {
			_ = ds.PutNodeDedup(NodeDedupRecord{DedupKey: state.DedupKey, TaskID: task.ID, WorkflowID: wf.ID, NodeID: node.ID, InputHash: InputHash(input), Status: "failed", Error: err.Error()})
		}
		e.audit(task, "node.failed", "node failed", map[string]any{"node": node.ID, "error": err.Error()})
		_ = e.store.Save(task)
		return nil, err
	}
	if err := e.ValidateAgainstSchema(node.OutputSchema, result); err != nil {
		state.Status = NodeFailed
		state.Error = err.Error()
		return nil, err
	}
	state.Status = NodeCompleted
	e.metrics.Inc("node_completed_total")
	state.Result = result
	if ds, ok := e.store.(NodeDedupStore); ok {
		_ = ds.PutNodeDedup(NodeDedupRecord{DedupKey: state.DedupKey, TaskID: task.ID, WorkflowID: wf.ID, NodeID: node.ID, InputHash: InputHash(input), Status: "completed", Result: result})
	}
	task.NodeResults[stateKey] = result
	e.audit(task, "node.completed", "node completed", map[string]any{"node": node.ID, "result": result})
	_ = e.store.Save(task)
	return result, nil
}

func (e *Engine) dispatchNode(ctx context.Context, wf *Workflow, task *Task, node *Node, input any, state *NodeState, attempt int) (any, error) {
	if node.Type == NodeWorkflow {
		child, err := e.RunSync(ctx, node.Workflow, input)
		if err != nil {
			return nil, err
		}
		return child.Result, nil
	}
	switch node.Mode {
	case ModeInline:
		return e.executeHandler(ctx, wf, task, node, input, attempt)
	case ModeBackground:
		job := e.newJob(task, node, input, attempt)
		state.JobID = job.ID
		state.Status = NodeQueued
		if !node.Await {
			if err := e.local.SubmitFireAndForget(ctx, job); err != nil {
				return nil, err
			}
			return input, nil
		}
		res, err := e.local.SubmitAndWait(ctx, job)
		if err != nil {
			return nil, err
		}
		if res.Error != "" {
			return nil, errors.New(res.Error)
		}
		return res.Result, nil
	case ModeDistributed:
		job := e.newJob(task, node, input, attempt)
		state.JobID = job.ID
		state.Status = NodeQueued
		if err := e.broker.Publish(ctx, job); err != nil {
			return nil, err
		}
		if !node.Await {
			return input, nil
		}
		res, err := e.broker.WaitResult(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		if res.Error != "" {
			return nil, errors.New(res.Error)
		}
		return res.Result, nil
	default:
		return nil, fmt.Errorf("unsupported mode %q", node.Mode)
	}
}

func (e *Engine) executeHandler(ctx context.Context, wf *Workflow, task *Task, node *Node, input any, attempt int) (any, error) {
	ensureTaskRuntimeState(task)
	if err := e.ValidateAgainstSchema(node.InputSchema, input); err != nil {
		return nil, err
	}
	switch node.Type {
	case NodeNoop, NodeJoin:
		return input, nil
	case NodeScript:
		return e.runtime.ExecuteNode(ctx, wf, task, node, input, attempt)
	case NodeFunction, NodePage:
		e.mu.RLock()
		h := e.handlers[node.Handler]
		e.mu.RUnlock()
		if h == nil {
			return nil, fmt.Errorf("handler %q not registered", node.Handler)
		}
		ec := &ExecutionContext{ContextID: newID("ctx"), TaskID: task.ID, WorkflowID: wf.ID, NodeID: node.ID, Attempt: attempt, Values: map[string]any{"task_id": task.ID, "workflow_id": wf.ID, "node_id": node.ID}, NodeParams: node.Params, TaskInput: task.Input, LastResult: task.LastResult, NodeResults: task.NodeResults, PreviousNode: task.PreviousNode, DataContext: e.dataFacts(&DataContext{Workflow: wf, Task: task, Node: node, Input: input, Result: input})}
		return h(ec, input)
	default:
		return nil, fmt.Errorf("unsupported node type %q", node.Type)
	}
}

func (e *Engine) resolveEdges(ctx context.Context, wf *Workflow, task *Task, node *Node, result any, errorMode bool) ([]RunItem, error) {
	var out []RunItem
	for _, edge := range wf.Outgoing[node.ID] {
		if errorMode && edge.Type != EdgeError {
			continue
		}
		if !errorMode && edge.Type == EdgeError {
			continue
		}
		switch edge.Type {
		case EdgeSimple, EdgeError, EdgeFallback, EdgeTimeout, EdgeRetry, EdgeCompensate:
			edgeInput, err := e.applyEdgeData(ctx, wf, task, node, edge, result)
			if err != nil {
				if errors.Is(err, ErrDataFiltered) {
					continue
				}
				return nil, err
			}
			for _, target := range edge.Targets {
				out = append(out, RunItem{NodeID: target, Input: edgeInput, From: node.ID, EdgeID: edge.ID})
			}
		case EdgeBranch:
			ok, err := e.evalEdgeCondition(edge, task, node, result)
			if err != nil {
				return nil, fmt.Errorf("edge %s condition: %w", edge.ID, err)
			}
			if ok {
				edgeInput, err := e.applyEdgeData(ctx, wf, task, node, edge, result)
				if err != nil {
					if errors.Is(err, ErrDataFiltered) {
						continue
					}
					return nil, err
				}
				for _, target := range edge.Targets {
					out = append(out, RunItem{NodeID: target, Input: edgeInput, From: node.ID, EdgeID: edge.ID})
				}
			}
		case EdgeFanOut:
			edgeInput, err := e.applyEdgeData(ctx, wf, task, node, edge, result)
			if err != nil {
				if errors.Is(err, ErrDataFiltered) {
					continue
				}
				return nil, err
			}
			for _, target := range edge.Targets {
				out = append(out, RunItem{NodeID: target, Input: edgeInput, From: node.ID, EdgeID: edge.ID})
			}
		case EdgeFanIn, EdgeJoin:
			items, ready, err := e.recordFanIn(task, edge, node.ID, result)
			if err != nil {
				return nil, err
			}
			if ready {
				for i := range items {
					if !edge.Data.Empty() || len(edge.Map) > 0 {
						transformed, derr := e.applyData(ctx, edgeDataSpec(edge), &DataContext{Workflow: wf, Task: task, Node: node, Edge: edge, Input: items[i].Input, Result: items[i].Input}, items[i].Input)
						if derr != nil {
							if errors.Is(derr, ErrDataFiltered) {
								continue
							}
							return nil, derr
						}
						items[i].Input = transformed
					}
					out = append(out, items[i])
				}
			}
		case EdgeParallel:
			edgeInput, err := e.applyEdgeData(ctx, wf, task, node, edge, result)
			if err != nil {
				if errors.Is(err, ErrDataFiltered) {
					continue
				}
				return nil, err
			}
			items, err := e.runParallelTargets(ctx, wf, task, edge, edgeInput)
			if err != nil {
				return nil, err
			}
			out = append(out, items...)
		case EdgeRace:
			edgeInput, err := e.applyEdgeData(ctx, wf, task, node, edge, result)
			if err != nil {
				if errors.Is(err, ErrDataFiltered) {
					continue
				}
				return nil, err
			}
			items, err := e.runRaceTargets(ctx, wf, task, edge, edgeInput)
			if err != nil {
				return nil, err
			}
			out = append(out, items...)
		case EdgeIterator:
			edgeInput, err := e.applyEdgeData(ctx, wf, task, node, edge, result)
			if err != nil {
				if errors.Is(err, ErrDataFiltered) {
					continue
				}
				return nil, err
			}
			items, err := toSlice(edgeInput)
			if err != nil {
				return nil, fmt.Errorf("edge %s iterator input: %w", edge.ID, err)
			}
			target := wf.Nodes[edge.To]
			agg := make([]any, 0, len(items))
			parentState := task.NodeStates[target.ID]
			if parentState == nil {
				parentState = &NodeState{NodeID: target.ID, Mode: target.Mode, Status: NodeRunning}
				task.NodeStates[target.ID] = parentState
			}
			for i, item := range items {
				started := time.Now()
				stateKey := fmt.Sprintf("%s[%d]", target.ID, i)
				itemInput, derr := e.applyNodeInput(ctx, wf, task, target, item)
				if derr != nil {
					if errors.Is(derr, ErrDataFiltered) {
						continue
					}
					return nil, derr
				}
				res, err := e.runNode(ctx, wf, task, target, itemInput, stateKey)
				if err == nil {
					res, err = e.applyNodeOutput(ctx, wf, task, target, res)
				}
				if st := task.NodeStates[stateKey]; st != nil {
					st.Result = res
				}
				iter := IterationState{Index: i, Input: item, Result: res, StartedAt: started, FinishedAt: time.Now(), Status: NodeCompleted}
				iter.Duration = iter.FinishedAt.Sub(iter.StartedAt)
				if err != nil {
					iter.Status = NodeFailed
					iter.Error = err.Error()
					parentState.Iterations = append(parentState.Iterations, iter)
					return nil, err
				}
				parentState.Iterations = append(parentState.Iterations, iter)
				agg = append(agg, res)
			}
			parentState.Status = NodeCompleted
			parentState.Result = agg
			parentState.FinishedAt = time.Now()
			task.NodeResults[target.ID] = agg
			nextItems, err := e.resolveEdges(ctx, wf, task, target, agg, false)
			if err != nil {
				return nil, err
			}
			out = append(out, nextItems...)
		}
	}
	return out, nil
}

func (e *Engine) markSkipped(task *Task, node *Node, input any) {
	st := task.NodeStates[node.ID]
	if st == nil {
		st = &NodeState{NodeID: node.ID}
		task.NodeStates[node.ID] = st
	}
	st.Status = NodeSkipped
	st.Input = input
	st.FinishedAt = time.Now()
	e.audit(task, "node.skipped", "node guard evaluated false", map[string]any{"node": node.ID})
	_ = e.store.Save(task)
}
func (e *Engine) recordNodeError(task *Task, node *Node, input any, err error) {
	task.LastError = err.Error()
	task.Error = err.Error()
	task.FailedNodeID = node.ID
	task.FailedInput = input
	task.Errors = append(task.Errors, TaskError{NodeID: node.ID, Error: err.Error(), Input: input, At: time.Now()})
}
func (e *Engine) startActivityLogger() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("dagflow activity logger panic: %v", r)
			}
		}()
		for ev := range e.activityLog {
			log.Printf("dagflow activity task=%s workflow=%s node=%s event=%s message=%q", ev.TaskID, ev.WorkflowID, ev.NodeID, ev.Event, ev.Message)
		}
	}()
}

func (e *Engine) safeGo(fn func()) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("dagflow goroutine panic: %v", r)
			}
		}()
		fn()
	}()
}

func (e *Engine) Shutdown(ctx context.Context) error {
	if e.runtimeCancel != nil {
		e.runtimeCancel()
	}
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) audit(task *Task, event, message string, data map[string]any) {
	if task == nil {
		return
	}
	var safe map[string]any
	if data != nil {
		if m, ok := Redact(data).(map[string]any); ok {
			safe = m
		} else {
			safe = map[string]any{"data": Redact(data)}
		}
	}
	ev := AuditEvent{ID: newID("audit"), TaskID: task.ID, WorkflowID: task.WorkflowID, NodeID: task.CurrentNode, Event: event, Message: message, Data: safe, At: time.Now()}
	task.Audit = append(task.Audit, ev)
	select {
	case e.activityLog <- ev:
	default:
		// Never block workflow execution because an audit log sink is congested.
	}
	e.emitAuditNotifications(context.Background(), task, ev)
}

func (e *Engine) applyWorkflowInput(ctx context.Context, wf *Workflow, input any) (any, error) {
	return e.applyData(ctx, wf.InputData, &DataContext{Workflow: wf, Input: input, Result: input}, input)
}
func (e *Engine) applyWorkflowOutput(ctx context.Context, wf *Workflow, task *Task, result any) (any, error) {
	return e.applyData(ctx, wf.OutputData, &DataContext{Workflow: wf, Task: task, Input: result, Result: result}, result)
}
func (e *Engine) applyNodeInput(ctx context.Context, wf *Workflow, task *Task, node *Node, input any) (any, error) {
	return e.applyData(ctx, node.InputData, &DataContext{Workflow: wf, Task: task, Node: node, Input: input, Result: input}, input)
}
func (e *Engine) applyNodeOutput(ctx context.Context, wf *Workflow, task *Task, node *Node, result any) (any, error) {
	return e.applyData(ctx, node.OutputData, &DataContext{Workflow: wf, Task: task, Node: node, Input: result, Result: result}, result)
}
func edgeDataSpec(edge *Edge) DataSpec {
	spec := edge.Data
	if spec.Empty() && len(edge.Map) > 0 {
		spec.Map = edge.Map
	}
	return spec
}

func (e *Engine) applyEdgeData(ctx context.Context, wf *Workflow, task *Task, node *Node, edge *Edge, result any) (any, error) {
	spec := edgeDataSpec(edge)
	return e.applyData(ctx, spec, &DataContext{Workflow: wf, Task: task, Node: node, Edge: edge, Input: result, Result: result}, result)
}

func (e *Engine) finishTask(task *Task, err error) {
	ensureTaskRuntimeState(task)
	if task.Status == TaskWaiting || task.Status == TaskPaused || task.Status == TaskCancelled {
		task.UpdatedAt = time.Now()
		_ = e.store.Save(task)
		return
	}
	if err != nil {
		e.metrics.Inc("workflow_failed_total")
		task.Status = TaskFailed
		task.Error = err.Error()
		task.LastError = err.Error()
		e.audit(task, "task.failed", "task failed", map[string]any{"error": err.Error()})
	} else {
		task.Status = TaskCompleted
		e.metrics.Inc("workflow_completed_total")
		done := time.Now()
		task.CompletedAt = &done
		e.audit(task, "task.completed", "task completed", map[string]any{"result": task.Result})
	}
	if wf, werr := e.workflow(task.WorkflowID); werr == nil {
		_ = e.applyTaskRules(context.Background(), wf, task, nil, "task."+string(task.Status), task.Result)
	}
	task.UpdatedAt = time.Now()
	_ = e.store.Save(task)
	e.fireCallbacks(task)
}
func (e *Engine) finishChain(run *ChainRun, err error) {
	if err != nil {
		run.Status = TaskFailed
		run.Error = err.Error()
	} else if run.Status != TaskWaiting {
		run.Status = TaskCompleted
		done := time.Now()
		run.CompletedAt = &done
	}
	run.UpdatedAt = time.Now()
	_ = e.chainStore.Save(run)
}
func (e *Engine) fireCallbacks(t *Task) {
	e.mu.RLock()
	callbacks := append([]FinalCallback(nil), e.callbacks...)
	e.mu.RUnlock()
	for _, cb := range callbacks {
		cb(cloneTask(t))
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// RegisterHandlers registers multiple Go handlers at once. It is intended for app packages.
func (e *Engine) RegisterHandlers(h map[string]Handler) {
	for name, fn := range h {
		e.Register(name, fn)
	}
}

// RegisterScript registers an interpreter-backed handler by name.
func (e *Engine) RegisterScript(name, source string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.scriptHandlers == nil {
		e.scriptHandlers = map[string]string{}
	}
	e.scriptHandlers[name] = source
}

func (e *Engine) HandlerNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.handlers))
	for k := range e.handlers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (e *Engine) ScriptNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.scriptHandlers))
	for k := range e.scriptHandlers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
