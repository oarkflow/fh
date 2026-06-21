package main

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

func buildSchema(c SchemaConfig) SchemaDef {
	s := SchemaDef{ID: c.ID, Type: c.Type, Required: append([]string(nil), c.Required...), Fields: map[string]SchemaField{}}
	if s.Type == "" {
		s.Type = "object"
	}
	for _, f := range c.Fields {
		s.Fields[f.ID] = SchemaField{Type: f.Type, Required: f.Required, Format: f.Format}
		if f.Required && !containsString(s.Required, f.ID) {
			s.Required = append(s.Required, f.ID)
		}
	}
	sort.Strings(s.Required)
	return s
}

func containsString(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func (e *Engine) AddSchema(s SchemaDef) error {
	if s.ID == "" {
		return fmt.Errorf("schema id is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.schemas == nil {
		e.schemas = map[string]SchemaDef{}
	}
	e.schemas[s.ID] = s
	return nil
}

func (e *Engine) Schema(id string) (SchemaDef, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.schemas[id]
	return s, ok
}
func (e *Engine) Schemas() []SchemaDef {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]SchemaDef, 0, len(e.schemas))
	for _, s := range e.schemas {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (e *Engine) ValidateAgainstSchema(schemaID string, value any) error {
	if schemaID == "" {
		return nil
	}
	s, ok := e.Schema(schemaID)
	if !ok {
		return fmt.Errorf("schema %s not found", schemaID)
	}
	m, ok := value.(map[string]any)
	if !ok && s.Type == "object" {
		return fmt.Errorf("schema %s expects object", schemaID)
	}
	for _, req := range s.Required {
		if _, ok := m[req]; !ok {
			return fmt.Errorf("schema %s missing required field %s", schemaID, req)
		}
	}
	for name, f := range s.Fields {
		v, ok := m[name]
		if !ok {
			continue
		}
		if err := validateFieldType(name, f, v); err != nil {
			return err
		}
	}
	return nil
}

func validateFieldType(name string, f SchemaField, v any) error {
	switch f.Type {
	case "", "any":
		return nil
	case "string":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("field %s expects string", name)
		}
	case "number":
		switch v.(type) {
		case float64, float32, int, int64, int32, uint, uint64, uint32:
		default:
			return fmt.Errorf("field %s expects number", name)
		}
	case "bool", "boolean":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("field %s expects bool", name)
		}
	case "array":
		if _, ok := v.([]any); !ok {
			return fmt.Errorf("field %s expects array", name)
		}
	case "object":
		if _, ok := v.(map[string]any); !ok {
			return fmt.Errorf("field %s expects object", name)
		}
	}
	if f.Format == "email" {
		s, _ := v.(string)
		if !strings.Contains(s, "@") || strings.HasPrefix(s, "@") || strings.HasSuffix(s, "@") {
			return fmt.Errorf("field %s expects email", name)
		}
	}
	return nil
}

func (e *Engine) Metrics() MetricSnapshot { return e.metrics.Snapshot() }

type MetricsRegistry struct {
	counters map[string]*atomic.Uint64
	gauges   map[string]*atomic.Int64
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{counters: map[string]*atomic.Uint64{}, gauges: map[string]*atomic.Int64{}}
}
func (m *MetricsRegistry) Inc(name string) {
	if m == nil {
		return
	}
	c := m.counters[name]
	if c == nil {
		c = &atomic.Uint64{}
		m.counters[name] = c
	}
	c.Add(1)
}
func (m *MetricsRegistry) SetGauge(name string, v int64) {
	if m == nil {
		return
	}
	g := m.gauges[name]
	if g == nil {
		g = &atomic.Int64{}
		m.gauges[name] = g
	}
	g.Store(v)
}
func (m *MetricsRegistry) Snapshot() MetricSnapshot {
	out := MetricSnapshot{Counters: map[string]uint64{}, Gauges: map[string]int64{}}
	if m == nil {
		return out
	}
	for k, v := range m.counters {
		out.Counters[k] = v.Load()
	}
	for k, v := range m.gauges {
		out.Gauges[k] = v.Load()
	}
	return out
}
