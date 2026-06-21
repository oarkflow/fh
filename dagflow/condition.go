package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/oarkflow/bcl"
)

func (e *Engine) AddCondition(c ConditionSpec) error {
	if c.Name == "" {
		return fmt.Errorf("condition id is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.conditions[c.Name]; exists {
		return fmt.Errorf("condition %s already exists", c.Name)
	}
	e.conditions[c.Name] = c
	return nil
}

func (e *Engine) evalNodeCondition(node *Node, task *Task, input any) (bool, error) {
	facts := e.workflowFacts(task, node, input, nil)
	return e.evalNamedOrInline(node.Condition, node.When, facts)
}

func (e *Engine) evalEdgeCondition(edge *Edge, task *Task, node *Node, result any) (bool, error) {
	facts := e.workflowFacts(task, node, result, map[string]any{"edge": map[string]any{"id": edge.ID, "from": edge.From, "to": edge.To, "type": string(edge.Type)}})
	return e.evalNamedOrInline(edge.Condition, edge.When, facts)
}

func (e *Engine) evalChainCondition(ch *Chain, input any) (bool, error) {
	facts := map[string]any{"input": input, "result": input, "chain": map[string]any{"id": ch.ID, "workflow_ids": ch.Workflows}}
	return e.evalNamedOrInline(ch.Condition, ch.When, facts)
}

func (e *Engine) evalRouteCondition(rc RouteConfig, facts map[string]any) (bool, error) {
	return e.evalNamedOrInline(rc.Condition, rc.When, facts)
}

func (e *Engine) evalMiddlewareCondition(c MiddlewareConfig, r *http.Request) (bool, error) {
	if c.Condition == "" && c.When == "" {
		return true, nil
	}
	facts := httpConditionFacts(r, nil, nil, RouteConfig{})
	return e.evalNamedOrInline(c.Condition, c.When, facts)
}

func (e *Engine) evalNamedOrInline(name, expr string, facts map[string]any) (bool, error) {
	if name != "" {
		e.mu.RLock()
		spec, ok := e.conditions[name]
		e.mu.RUnlock()
		if !ok {
			return false, fmt.Errorf("condition %q not found", name)
		}
		return e.evalConditionSpec(spec, facts)
	}
	return evalBCLBool(expr, facts)
}

func (e *Engine) evalConditionSpec(spec ConditionSpec, facts map[string]any) (bool, error) {
	facts = cloneFacts(facts)
	facts["condition"] = map[string]any{"id": spec.Name, "description": spec.Description}
	if strings.TrimSpace(spec.Expr) != "" {
		ok, err := evalBCLBool(spec.Expr, facts)
		if err != nil || !ok {
			return ok, err
		}
	}
	for _, expr := range spec.All {
		ok, err := evalBCLBool(expr, facts)
		if err != nil || !ok {
			return ok, err
		}
	}
	if len(spec.Any) > 0 {
		matched := false
		for _, expr := range spec.Any {
			ok, err := evalBCLBool(expr, facts)
			if err != nil {
				return false, err
			}
			if ok {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	for _, expr := range spec.None {
		ok, err := evalBCLBool(expr, facts)
		if err != nil {
			return false, err
		}
		if ok {
			return false, nil
		}
	}
	for _, expr := range spec.Not {
		ok, err := evalBCLBool(expr, facts)
		if err != nil {
			return false, err
		}
		if ok {
			return false, nil
		}
	}
	return true, nil
}

func evalBCLBool(expr string, facts map[string]any) (bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, nil
	}
	ok1, err := bcl.Eval(expr, facts)
	if err != nil {
		return false, err
	}
	ok, ok2 := ok1.(bool)
	if !ok2 {
		return false, fmt.Errorf("expression %q did not return a boolean", expr)
	}
	return ok, nil
}

func (e *Engine) workflowFacts(task *Task, node *Node, result any, extra map[string]any) map[string]any {
	states := map[string]any{}
	results := map[string]any{}
	if task != nil {
		for id, st := range task.NodeStates {
			states[id] = map[string]any{"status": string(st.Status), "attempts": st.Attempts, "error": st.Error, "job_id": st.JobID, "duration": st.Duration.String()}
		}
		for id, res := range task.NodeResults {
			results[id] = res
		}
	}
	facts := map[string]any{
		"result": result,
		"input":  result,
		"node":   map[string]any{"id": safeNodeID(node), "handler": safeNodeHandler(node), "type": safeNodeType(node)},
		"task": map[string]any{
			"id": taskString(task, func(t *Task) string { return t.ID }), "workflow_id": taskString(task, func(t *Task) string { return t.WorkflowID }), "status": taskString(task, func(t *Task) string { return string(t.Status) }),
			"current_node": taskString(task, func(t *Task) string { return t.CurrentNode }), "previous_node": taskString(task, func(t *Task) string { return t.PreviousNode }), "previous_nodes": safePreviousNodes(task), "last_error": taskString(task, func(t *Task) string { return t.LastError }), "visits": safeTaskVisits(task),
		},
		"nodes":   states,
		"results": results,
		"last":    map[string]any{"result": safeLastResult(task), "error": taskString(task, func(t *Task) string { return t.LastError })},
	}
	for k, v := range extra {
		facts[k] = v
	}
	return facts
}

func httpConditionFacts(r *http.Request, params map[string]string, input any, rc RouteConfig) map[string]any {
	headers := map[string]any{}
	if r != nil {
		for k, v := range r.Header {
			if len(v) == 1 {
				headers[k] = v[0]
			} else {
				headers[k] = v
			}
		}
	}
	query := map[string]any{}
	if r != nil {
		for k, v := range r.URL.Query() {
			if len(v) == 1 {
				query[k] = v[0]
			} else {
				query[k] = v
			}
		}
	}
	method, path, remote := "", "", ""
	if r != nil {
		method, path, remote = r.Method, r.URL.Path, r.RemoteAddr
	}
	return map[string]any{"request": map[string]any{"method": method, "path": path, "headers": headers, "query": query, "remote_addr": remote, "client_ip": clientIP(r), "body": input}, "route": map[string]any{"id": rc.ID, "method": rc.Method, "path": rc.Path, "workflow": rc.Workflow, "chain": rc.Chain}, "path": params, "input": input, "result": input}
}

func cloneFacts(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func safeNodeID(n *Node) string {
	if n == nil {
		return ""
	}
	return n.ID
}
func safeNodeHandler(n *Node) string {
	if n == nil {
		return ""
	}
	return n.Handler
}
func safeNodeType(n *Node) string {
	if n == nil {
		return ""
	}
	return string(n.Type)
}
func taskString(t *Task, f func(*Task) string) string {
	if t == nil {
		return ""
	}
	return f(t)
}
func safeTaskVisits(t *Task) map[string]int {
	if t == nil || t.Visits == nil {
		return map[string]int{}
	}
	return t.Visits
}
func safePreviousNodes(t *Task) []string {
	if t == nil {
		return nil
	}
	return t.PreviousNodes
}
func safeLastResult(t *Task) any {
	if t == nil {
		return nil
	}
	return t.LastResult
}
