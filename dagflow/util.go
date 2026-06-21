package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
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

func clientIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	if x := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); x != "" {
		return strings.TrimSpace(strings.Split(x, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSONBody(r *http.Request) (any, error) {
	var input any
	dec := json.NewDecoder(r.Body)
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
