package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Engine struct {
	mu             sync.RWMutex
	flows          map[string]*Workflow
	chains         map[string]*Chain
	conditions     map[string]ConditionSpec
	schemas        map[string]SchemaDef
	snapshots      map[string][]WorkflowSnapshot
	handlers       map[string]Handler
	scriptHandlers map[string]string
	metrics        *MetricsRegistry
	runtime        *ScriptRuntime
	store          TaskStore
	chainStore     ChainStore
	callbacks      []FinalCallback
	local          *LocalQueue
	broker         Broker
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
	e := &Engine{flows: map[string]*Workflow{}, chains: map[string]*Chain{}, conditions: map[string]ConditionSpec{}, schemas: map[string]SchemaDef{}, snapshots: map[string][]WorkflowSnapshot{}, handlers: map[string]Handler{}, scriptHandlers: map[string]string{}, metrics: NewMetricsRegistry(), store: store, chainStore: chainStore, broker: broker}
	e.local = NewLocalQueue(e, 8)
	e.runtime = NewScriptRuntime(e)
	return e
}

func (e *Engine) Register(name string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[name] = h
}
func (e *Engine) OnFinal(cb FinalCallback) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.callbacks = append(e.callbacks, cb)
}
func (e *Engine) Store() TaskStore       { return e.store }
func (e *Engine) ChainStore() ChainStore { return e.chainStore }

func (e *Engine) LoadConfig(cfg *Config) error {
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
	e.local.Start(ctx)
	e.mu.RLock()
	ids := make([]string, 0, len(e.flows))
	for id := range e.flows {
		ids = append(ids, id)
	}
	e.mu.RUnlock()
	for _, id := range ids {
		e.StartDistributedWorker(ctx, id, 4)
	}
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
	}
	for _, wf := range e.flows {
		for _, n := range wf.Nodes {
			if n.Type == NodeWorkflow && e.flows[n.Workflow] == nil {
				return fmt.Errorf("workflow %s node %s references missing workflow %s", wf.ID, n.ID, n.Workflow)
			}
		}
	}
	return nil
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
	task := newTask(workflowID, input)
	task.WorkflowVersion = wf.Version
	task.DefinitionHash = wf.Hash
	e.metrics.Inc("workflow_started_total")
	e.audit(task, "task.created", "task created", map[string]any{"input": Redact(input)})
	if err := e.store.Create(task); err != nil {
		return nil, err
	}
	err = e.executeTask(ctx, wf, task, []RunItem{{NodeID: wf.First, Input: input}})
	e.finishTask(task, err)
	return task, err
}
func (e *Engine) RunAsync(ctx context.Context, workflowID string, input any) (*Task, error) {
	wf, err := e.workflow(workflowID)
	if err != nil {
		return nil, err
	}
	task := newTask(workflowID, input)
	task.WorkflowVersion = wf.Version
	task.DefinitionHash = wf.Hash
	e.metrics.Inc("workflow_started_total")
	e.audit(task, "task.created", "async task created", map[string]any{"input": Redact(input)})
	if err := e.store.Create(task); err != nil {
		return nil, err
	}
	go func() {
		err := e.executeTask(context.Background(), wf, task, []RunItem{{NodeID: wf.First, Input: input}})
		e.finishTask(task, err)
	}()
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
	res, err := e.runNode(ctx, wf, task, node, input, node.ID)
	return task.NodeStates[node.ID], res, err
}

func (e *Engine) executeTask(ctx context.Context, wf *Workflow, task *Task, queue []RunItem) error {
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
		result, err := e.runNode(ctx, wf, task, node, item.Input, node.ID)
		if err != nil {
			e.recordNodeError(task, node, item.Input, err)
			if st := task.NodeStates[node.ID]; st != nil {
				e.storeDLQ(task, node, item.Input, err, st.Attempts)
			}
			if node.ContinueOnError {
				e.audit(task, "node.error.continued", "continuing after node error", map[string]any{"node": node.ID, "error": err.Error()})
				next, rerr := e.resolveEdges(ctx, wf, task, node, map[string]any{"error": err.Error(), "input": item.Input}, true)
				if rerr != nil {
					return rerr
				}
				queue = append(queue, next...)
				continue
			}
			task.Cursor = queue
			task.FailedNodeID = node.ID
			task.FailedInput = item.Input
			_ = e.store.Save(task)
			return fmt.Errorf("node %s failed: %w", node.ID, err)
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
		ec := &ExecutionContext{ContextID: newID("ctx"), TaskID: task.ID, WorkflowID: wf.ID, NodeID: node.ID, Attempt: attempt, Values: map[string]any{"task_id": task.ID, "workflow_id": wf.ID, "node_id": node.ID}, NodeParams: node.Params, TaskInput: task.Input, LastResult: task.LastResult, NodeResults: task.NodeResults, PreviousNode: task.PreviousNode}
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
			for _, target := range edge.Targets {
				out = append(out, RunItem{NodeID: target, Input: result, From: node.ID, EdgeID: edge.ID})
			}
		case EdgeBranch:
			ok, err := e.evalEdgeCondition(edge, task, node, result)
			if err != nil {
				return nil, fmt.Errorf("edge %s condition: %w", edge.ID, err)
			}
			if ok {
				for _, target := range edge.Targets {
					out = append(out, RunItem{NodeID: target, Input: result, From: node.ID, EdgeID: edge.ID})
				}
			}
		case EdgeFanOut:
			for _, target := range edge.Targets {
				out = append(out, RunItem{NodeID: target, Input: result, From: node.ID, EdgeID: edge.ID})
			}
		case EdgeFanIn, EdgeJoin:
			items, ready, err := e.recordFanIn(task, edge, node.ID, result)
			if err != nil {
				return nil, err
			}
			if ready {
				out = append(out, items...)
			}
		case EdgeParallel:
			items, err := e.runParallelTargets(ctx, wf, task, edge, result)
			if err != nil {
				return nil, err
			}
			out = append(out, items...)
		case EdgeRace:
			items, err := e.runRaceTargets(ctx, wf, task, edge, result)
			if err != nil {
				return nil, err
			}
			out = append(out, items...)
		case EdgeIterator:
			items, err := toSlice(result)
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
				res, err := e.runNode(ctx, wf, task, target, item, stateKey)
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
func (e *Engine) audit(task *Task, event, message string, data map[string]any) {
	if task == nil {
		return
	}
	task.Audit = append(task.Audit, AuditEvent{ID: newID("audit"), TaskID: task.ID, WorkflowID: task.WorkflowID, NodeID: task.CurrentNode, Event: event, Message: message, Data: data, At: time.Now()})
}

func (e *Engine) finishTask(task *Task, err error) {
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
