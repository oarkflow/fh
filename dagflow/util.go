package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/oarkflow/fh"
)

func newID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

func shortJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%T", v)
	}
	if len(b) > 240 {
		return string(b[:240]) + "..."
	}
	return string(b)
}

func toSlice(v any) ([]any, error) {
	if v == nil {
		return nil, errors.New("nil is not iterable")
	}
	if xs, ok := v.([]any); ok {
		return xs, nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice/array, got %T", v)
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, nil
}

func nodeIDs(items []RunItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.NodeID)
	}
	return out
}

func clientIP(c *fh.Ctx) string {
	if c == nil {
		return ""
	}
	if x := strings.TrimSpace(c.Get("X-Forwarded-For")); x != "" {
		return strings.TrimSpace(strings.Split(x, ",")[0])
	}
	return c.IP()
}

func writeJSON(c *fh.Ctx, code int, v any) error {
	return c.Status(code).JSON(v)
}

func readJSONBody(c *fh.Ctx) (any, error) {
	var input any
	dec := json.NewDecoder(strings.NewReader(string(c.Body())))
	dec.UseNumber()
	if err := dec.Decode(&input); err != nil {
		return nil, err
	}
	return input, nil
}

func stringIn(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func WorkflowHash(wf *Workflow) string {
	return InputHash(map[string]any{"id": wf.ID, "version": wf.Version, "first": wf.First, "nodes": wf.Nodes, "edges": wf.Edges})
}
