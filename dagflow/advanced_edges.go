package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func retryDelay(p RetryPolicy, attempt int) time.Duration {
	if p.InitialDelay <= 0 {
		p.InitialDelay = 50 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 5 * time.Second
	}
	var d time.Duration
	switch p.Strategy {
	case "linear":
		d = time.Duration(attempt) * p.InitialDelay
	case "exponential", "exponential_jitter", "decorrelated_jitter":
		d = p.InitialDelay << (attempt - 1)
	default:
		d = p.InitialDelay
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	if p.Jitter || p.Strategy == "exponential_jitter" || p.Strategy == "decorrelated_jitter" {
		j := time.Duration(rand.Int63n(int64(d/2 + 1)))
		d = d/2 + j
	}
	return d
}

func (e *Engine) recordFanIn(task *Task, edge *Edge, source string, result any) ([]RunItem, bool, error) {
	if task.JoinStates == nil {
		task.JoinStates = map[string]*JoinState{}
	}
	st := task.JoinStates[edge.ID]
	if st == nil {
		st = &JoinState{EdgeID: edge.ID, Sources: append([]string(nil), edge.Sources...), CompletedSources: map[string]bool{}, Results: map[string]any{}, Errors: map[string]string{}}
		task.JoinStates[edge.ID] = st
	}
	if st.Emitted {
		return nil, false, nil
	}
	st.CompletedSources[source] = true
	st.Results[source] = result
	e.audit(task, "fanin.source.completed", "fanin source completed", map[string]any{"edge": edge.ID, "source": source})
	ready := false
	switch edge.Strategy {
	case "any", "first":
		ready = len(st.CompletedSources) >= 1
	case "quorum":
		q := edge.Quorum
		if q <= 0 {
			q = len(edge.Sources)/2 + 1
		}
		ready = len(st.CompletedSources) >= q
	default:
		ready = len(st.CompletedSources) >= len(edge.Sources)
	}
	if !ready {
		return nil, false, nil
	}
	st.Emitted = true
	input := map[string]any{"results": st.Results, "sources": st.Sources, "strategy": edge.Strategy}
	var out []RunItem
	for _, target := range edge.Targets {
		out = append(out, RunItem{NodeID: target, Input: input, From: source, EdgeID: edge.ID})
	}
	e.audit(task, "fanin.emitted", "fanin emitted aggregate", map[string]any{"edge": edge.ID, "targets": edge.Targets})
	return out, true, nil
}

func (e *Engine) runParallelTargets(ctx context.Context, wf *Workflow, task *Task, edge *Edge, input any) ([]RunItem, error) {
	if len(edge.Targets) == 0 {
		return nil, nil
	}
	limit := edge.MaxConcurrency
	if limit <= 0 || limit > len(edge.Targets) {
		limit = len(edge.Targets)
	}
	sem := make(chan struct{}, limit)
	type item struct {
		target string
		result any
		err    error
	}
	resCh := make(chan item, len(edge.Targets))
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, targetID := range edge.Targets {
		target := wf.Nodes[targetID]
		if target == nil {
			return nil, fmt.Errorf("parallel target %s not found", targetID)
		}
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx2.Done():
				resCh <- item{target: n.ID, err: ctx2.Err()}
				return
			}
			stateKey := n.ID
			result, err := e.runNode(ctx2, wf, task, n, input, stateKey)
			if err != nil && edge.FailFast {
				cancel()
			}
			resCh <- item{target: n.ID, result: result, err: err}
		}(target)
	}
	wg.Wait()
	close(resCh)
	var out []RunItem
	var firstErr error
	for r := range resCh {
		if r.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("parallel target %s failed: %w", r.target, r.err)
			}
			if edge.FailFast {
				return nil, firstErr
			}
			continue
		}
		target := wf.Nodes[r.target]
		next, err := e.resolveEdges(ctx, wf, task, target, r.result, false)
		if err != nil {
			return nil, err
		}
		out = append(out, next...)
	}
	if firstErr != nil && edge.FailFast {
		return nil, firstErr
	}
	return out, nil
}

func (e *Engine) runRaceTargets(ctx context.Context, wf *Workflow, task *Task, edge *Edge, input any) ([]RunItem, error) {
	if len(edge.Targets) == 0 {
		return nil, nil
	}
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	type item struct {
		target string
		result any
		err    error
	}
	resCh := make(chan item, len(edge.Targets))
	for _, targetID := range edge.Targets {
		target := wf.Nodes[targetID]
		if target == nil {
			return nil, fmt.Errorf("race target %s not found", targetID)
		}
		go func(n *Node) {
			res, err := e.runNode(ctx2, wf, task, n, input, n.ID)
			resCh <- item{target: n.ID, result: res, err: err}
		}(target)
	}
	var errs []error
	for i := 0; i < len(edge.Targets); i++ {
		r := <-resCh
		if r.err == nil {
			if edge.CancelLosers {
				cancel()
			}
			target := wf.Nodes[r.target]
			e.audit(task, "race.winner", "race target won", map[string]any{"edge": edge.ID, "target": r.target})
			return e.resolveEdges(ctx, wf, task, target, r.result, false)
		}
		errs = append(errs, r.err)
	}
	return nil, errors.Join(errs...)
}

func (e *Engine) storeDLQ(task *Task, node *Node, input any, err error, attempts int) {
	if e.store == nil || err == nil {
		return
	}
	item := DLQItem{ID: newID("dlq"), TaskID: task.ID, WorkflowID: task.WorkflowID, NodeID: node.ID, Input: input, Error: err.Error(), Attempts: attempts, CreatedAt: time.Now()}
	_ = e.store.AddDLQ(item)
}
