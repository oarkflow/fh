package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
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

func opsWorkflows(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, engine.ListWorkflowMetadata())
	}
}
func opsWorkflow(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ops/workflows/"), "/")
		parts := strings.Split(path, "/")
		if len(parts) == 0 || parts[0] == "" {
			writeJSON(w, 400, map[string]any{"error": "workflow id required"})
			return
		}
		id := parts[0]
		if len(parts) == 1 {
			m, err := engine.WorkflowMetadata(id)
			if err != nil {
				writeJSON(w, 404, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, 200, m)
			return
		}
		switch parts[1] {
		case "graph.svg":
			nested := r.URL.Query().Get("nested") == "true"
			svg, err := engine.WorkflowSVG(id, nested)
			if err != nil {
				writeJSON(w, 404, map[string]any{"error": err.Error()})
				return
			}
			w.Header().Set("Content-Type", "image/svg+xml")
			_, _ = w.Write([]byte(svg))
		case "versions":
			writeJSON(w, 200, engine.WorkflowSnapshots(id))
		case "metadata":
			m, err := engine.WorkflowMetadata(id)
			if err != nil {
				writeJSON(w, 404, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, 200, m)
		default:
			writeJSON(w, 404, map[string]any{"error": "unknown workflow operation"})
		}
	}
}
func opsMetadata(engine *Engine, cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"workflows": engine.ListWorkflowMetadata(), "chains": engine.ListChains(), "conditions": engine.ListConditions(), "schemas": engine.Schemas(), "routes": FlattenRoutes(cfg), "groups": cfg.RouteGroups, "metrics": engine.Metrics()})
	}
}
func opsOpenAPI(cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, GenerateOpenAPI(cfg)) }
}
func opsValidate(cfg *Config, engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := ValidateConfig(cfg, engine); err != nil {
			writeJSON(w, 400, map[string]any{"valid": false, "error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"valid": true})
	}
}
func dlqOps(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ops/dlq/"), "/")
		if path == "" {
			writeJSON(w, 200, engine.Store().ListDLQ())
			return
		}
		parts := strings.Split(path, "/")
		id := parts[0]
		if len(parts) == 1 {
			for _, it := range engine.Store().ListDLQ() {
				if it.ID == id {
					writeJSON(w, 200, it)
					return
				}
			}
			writeJSON(w, 404, map[string]any{"error": "dlq item not found"})
			return
		}
		switch parts[1] {
		case "discard":
			err := engine.Store().DeleteDLQ(id)
			if err != nil {
				writeJSON(w, 404, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, 200, map[string]any{"discarded": id})
		case "replay":
			var body struct {
				WorkflowID string `json:"workflow_id"`
				Input      any    `json:"input"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			var found *DLQItem
			for _, it := range engine.Store().ListDLQ() {
				if it.ID == id {
					x := it
					found = &x
					break
				}
			}
			if found == nil {
				writeJSON(w, 404, map[string]any{"error": "dlq item not found"})
				return
			}
			input := found.Input
			if body.Input != nil {
				input = body.Input
			}
			wf := found.WorkflowID
			if body.WorkflowID != "" {
				wf = body.WorkflowID
			}
			task, err := engine.RunAsync(r.Context(), wf, input)
			if err != nil {
				writeJSON(w, 500, map[string]any{"error": err.Error()})
				return
			}
			_ = engine.Store().DeleteDLQ(id)
			writeJSON(w, 202, task)
		default:
			writeJSON(w, 404, map[string]any{"error": "unknown dlq operation"})
		}
	}
}
func opsMetrics(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, engine.Metrics()) }
}
func opsOutbox(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s, ok := engine.Store().(ExtendedStore); ok {
			writeJSON(w, 200, s.ListOutbox())
			return
		}
		writeJSON(w, 200, []OutboxEvent{})
	}
}
func opsLeases(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s, ok := engine.Store().(ExtendedStore); ok {
			writeJSON(w, 200, s.ListLeases())
			return
		}
		writeJSON(w, 200, []WorkerLease{})
	}
}
