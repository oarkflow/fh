package main

import (
	"context"
	"fmt"
	"time"
)

func (e *Engine) PauseTask(taskID string) (*Task, error) {
	task, err := e.store.Get(taskID)
	if err != nil {
		return nil, err
	}
	if task.Status == TaskCompleted || task.Status == TaskCancelled {
		return nil, fmt.Errorf("cannot pause task in status %s", task.Status)
	}
	task.Status = TaskPaused
	e.audit(task, "task.paused", "task paused by operation", nil)
	task.UpdatedAt = time.Now()
	_ = e.store.Save(task)
	return task, nil
}

func (e *Engine) CancelTask(taskID string) (*Task, error) {
	task, err := e.store.Get(taskID)
	if err != nil {
		return nil, err
	}
	if task.Status == TaskCompleted {
		return nil, fmt.Errorf("cannot cancel completed task")
	}
	task.Status = TaskCancelled
	now := time.Now()
	task.CompletedAt = &now
	task.UpdatedAt = now
	e.audit(task, "task.cancelled", "task cancelled by operation", nil)
	_ = e.store.Save(task)
	return task, nil
}

func (e *Engine) ResumeTask(ctx context.Context, taskID string, input any) (*Task, error) {
	task, err := e.store.Get(taskID)
	if err != nil {
		return nil, err
	}
	wf, err := e.workflow(task.WorkflowID)
	if err != nil {
		return nil, err
	}
	if task.Status != TaskWaiting && task.Status != TaskPaused {
		return nil, fmt.Errorf("task %s is not waiting/paused: %s", taskID, task.Status)
	}
	task.Status = TaskRunning
	e.audit(task, "task.resumed", "task resumed by operation", map[string]any{"input": input})
	var queue []RunItem
	if task.WaitingNodeID != "" {
		node := wf.Nodes[task.WaitingNodeID]
		if node == nil {
			return nil, fmt.Errorf("waiting node %s not found", task.WaitingNodeID)
		}
		st := task.NodeStates[node.ID]
		if st == nil {
			st = &NodeState{NodeID: node.ID}
			task.NodeStates[node.ID] = st
		}
		st.Status = NodeCompleted
		st.Result = input
		st.Input = input
		st.FinishedAt = time.Now()
		task.NodeResults[node.ID] = input
		task.LastResult = input
		task.PreviousNode = node.ID
		task.PreviousNodes = append(task.PreviousNodes, node.ID)
		task.WaitingNodeID = ""
		task.ResumeToken = ""
		next, err := e.resolveEdges(ctx, wf, task, node, input, false)
		if err != nil {
			return nil, err
		}
		queue = next
	} else {
		queue = task.Cursor
		if input != nil {
			task.LastResult = input
		}
	}
	_ = e.store.Save(task)
	err = e.executeTask(ctx, wf, task, queue)
	e.finishTask(task, err)
	return task, err
}

func (e *Engine) ContinueTask(ctx context.Context, taskID string, strategy ErrorStrategy, result any) (*Task, error) {
	if strategy == "" {
		strategy = ContinueSkip
	}
	task, err := e.store.Get(taskID)
	if err != nil {
		return nil, err
	}
	wf, err := e.workflow(task.WorkflowID)
	if err != nil {
		return nil, err
	}
	if task.Status != TaskFailed {
		return nil, fmt.Errorf("task %s is not failed: %s", taskID, task.Status)
	}
	failed := wf.Nodes[task.FailedNodeID]
	if failed == nil {
		return nil, fmt.Errorf("failed node %s not found", task.FailedNodeID)
	}
	task.Status = TaskRunning
	task.Error = ""
	e.audit(task, "task.continue", "continue failed task", map[string]any{"strategy": strategy, "failed_node": failed.ID, "override_result": result})
	var queue []RunItem
	switch strategy {
	case ContinueRetry:
		queue = append([]RunItem{{NodeID: failed.ID, Input: task.FailedInput}}, task.Cursor...)
	case ContinueResult:
		if result == nil {
			result = map[string]any{"continued": true, "previous_error": task.LastError, "input": task.FailedInput}
		}
		st := task.NodeStates[failed.ID]
		if st == nil {
			st = &NodeState{NodeID: failed.ID}
			task.NodeStates[failed.ID] = st
		}
		st.Status = NodeCompleted
		st.Result = result
		st.Error = ""
		st.FinishedAt = time.Now()
		task.NodeResults[failed.ID] = result
		next, err := e.resolveEdges(ctx, wf, task, failed, result, false)
		if err != nil {
			return nil, err
		}
		queue = append(next, task.Cursor...)
	case ContinueSkip:
		fallthrough
	default:
		errorResult := map[string]any{"continued": true, "skipped_node": failed.ID, "previous_error": task.LastError, "input": task.FailedInput}
		next, err := e.resolveEdges(ctx, wf, task, failed, errorResult, true)
		if err != nil {
			return nil, err
		}
		if len(next) == 0 {
			next, err = e.resolveEdges(ctx, wf, task, failed, errorResult, false)
			if err != nil {
				return nil, err
			}
		}
		queue = append(next, task.Cursor...)
	}
	task.FailedNodeID = ""
	task.FailedInput = nil
	task.Cursor = queue
	task.UpdatedAt = time.Now()
	_ = e.store.Save(task)
	err = e.executeTask(ctx, wf, task, queue)
	e.finishTask(task, err)
	return task, err
}

func (e *Engine) RestartTask(ctx context.Context, taskID string) (*Task, error) {
	old, err := e.store.Get(taskID)
	if err != nil {
		return nil, err
	}
	wf, err := e.workflow(old.WorkflowID)
	if err != nil {
		return nil, err
	}
	task := newTask(old.WorkflowID, old.Input)
	task.RestartedFrom = old.ID
	task.ParentTaskID = old.ParentTaskID
	e.audit(task, "task.restarted", "task restarted from previous task", map[string]any{"from": old.ID})
	_ = e.store.Create(task)
	err = e.executeTask(ctx, wf, task, []RunItem{{NodeID: wf.First, Input: old.Input}})
	e.finishTask(task, err)
	return task, err
}
