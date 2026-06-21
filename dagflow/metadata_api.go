package main

import (
	"encoding/json"
	"sort"

	"github.com/oarkflow/fh"
)

type WorkflowMetadata struct {
	ID              string                  `json:"id"`
	Name            string                  `json:"name,omitempty"`
	Version         string                  `json:"version,omitempty"`
	Hash            string                  `json:"hash,omitempty"`
	First           string                  `json:"first"`
	NodeCount       int                     `json:"node_count"`
	EdgeCount       int                     `json:"edge_count"`
	Nodes           []*Node                 `json:"nodes"`
	Edges           []*Edge                 `json:"edges"`
	Mode            RunMode                 `json:"mode"`
	MaxVisits       int                     `json:"max_visits"`
	MigrationPolicy WorkflowMigrationPolicy `json:"migration_policy,omitempty"`
}

func (e *Engine) WorkflowMetadata(id string) (*WorkflowMetadata, error) {
	wf, err := e.workflow(id)
	if err != nil {
		return nil, err
	}
	nodes := make([]*Node, 0, len(wf.Nodes))
	for _, n := range wf.Nodes {
		cp := *n
		nodes = append(nodes, &cp)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return &WorkflowMetadata{ID: wf.ID, Name: wf.Name, Version: wf.Version, Hash: wf.Hash, First: wf.First, NodeCount: len(wf.Nodes), EdgeCount: len(wf.Edges), Nodes: nodes, Edges: wf.Edges, Mode: wf.Mode, MaxVisits: wf.MaxVisits, MigrationPolicy: wf.MigrationPolicy}, nil
}
func (e *Engine) ListWorkflowMetadata() []WorkflowMetadata {
	e.mu.RLock()
	ids := make([]string, 0, len(e.flows))
	for id := range e.flows {
		ids = append(ids, id)
	}
	e.mu.RUnlock()
	sort.Strings(ids)
	out := make([]WorkflowMetadata, 0, len(ids))
	for _, id := range ids {
		if m, err := e.WorkflowMetadata(id); err == nil {
			out = append(out, *m)
		}
	}
	return out
}
func (e *Engine) ListChains() []*Chain {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Chain, 0, len(e.chains))
	for _, c := range e.chains {
		cp := *c
		cp.Workflows = append([]string(nil), c.Workflows...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (e *Engine) ListConditions() []ConditionSpec {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]ConditionSpec, 0, len(e.conditions))
	for _, c := range e.conditions {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (e *Engine) WorkflowSnapshots(id string) []WorkflowSnapshot {
	e.mu.RLock()
	out := append([]WorkflowSnapshot(nil), e.snapshots[id]...)
	e.mu.RUnlock()
	if es, ok := e.store.(ExtendedStore); ok {
		out = append(out, es.ListSnapshots(id)...)
	}
	return out
}

func opsWorkflows(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.ListWorkflowMetadata()) }
}
func opsWorkflow(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		m, err := engine.WorkflowMetadata(c.Param("id"))
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, m)
	}
}
func opsWorkflowGraph(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		svg, err := engine.WorkflowSVG(c.Param("id"), c.Query("nested") == "true")
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		c.Type("image/svg+xml")
		return c.SendString(svg)
	}
}
func opsWorkflowVersions(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.WorkflowSnapshots(c.Param("id"))) }
}
func opsMetadata(engine *Engine, cfg *Config) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		return writeJSON(c, fh.StatusOK, map[string]any{"workflows": engine.ListWorkflowMetadata(), "chains": engine.ListChains(), "conditions": engine.ListConditions(), "schemas": engine.Schemas(), "routes": FlattenRoutes(cfg), "groups": cfg.RouteGroups, "metrics": engine.Metrics()})
	}
}
func opsOpenAPI(cfg *Config) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, GenerateOpenAPI(cfg)) }
}
func opsValidate(cfg *Config, engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if err := ValidateConfig(cfg, engine); err != nil {
			return writeJSON(c, fh.StatusBadRequest, map[string]any{"valid": false, "error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, map[string]any{"valid": true})
	}
}
func dlqGet(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		id := c.Param("id")
		for _, it := range engine.Store().ListDLQ() {
			if it.ID == id {
				return writeJSON(c, fh.StatusOK, it)
			}
		}
		return writeJSON(c, fh.StatusNotFound, map[string]any{"error": "dlq item not found"})
	}
}
func dlqDiscard(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		id := c.Param("id")
		err := engine.Store().DeleteDLQ(id)
		if err != nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": err.Error()})
		}
		return writeJSON(c, fh.StatusOK, map[string]any{"discarded": id})
	}
}
func dlqReplay(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		id := c.Param("id")
		var body struct {
			WorkflowID string `json:"workflow_id"`
			Input      any    `json:"input"`
		}
		_ = json.Unmarshal(c.Body(), &body)
		var found *DLQItem
		for _, it := range engine.Store().ListDLQ() {
			if it.ID == id {
				x := it
				found = &x
				break
			}
		}
		if found == nil {
			return writeJSON(c, fh.StatusNotFound, map[string]any{"error": "dlq item not found"})
		}
		input := found.Input
		if body.Input != nil {
			input = body.Input
		}
		wf := found.WorkflowID
		if body.WorkflowID != "" {
			wf = body.WorkflowID
		}
		task, err := engine.RunAsync(c.Context(), wf, input)
		if err != nil {
			return writeJSON(c, fh.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
		_ = engine.Store().DeleteDLQ(id)
		return writeJSON(c, fh.StatusAccepted, task)
	}
}
func opsMetrics(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error { return writeJSON(c, fh.StatusOK, engine.Metrics()) }
}
func opsOutbox(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if s, ok := engine.Store().(ExtendedStore); ok {
			return writeJSON(c, fh.StatusOK, s.ListOutbox())
		}
		return writeJSON(c, fh.StatusOK, []OutboxEvent{})
	}
}
func opsLeases(engine *Engine) fh.HandlerFunc {
	return func(c *fh.Ctx) error {
		if s, ok := engine.Store().(ExtendedStore); ok {
			return writeJSON(c, fh.StatusOK, s.ListLeases())
		}
		return writeJSON(c, fh.StatusOK, []WorkerLease{})
	}
}
