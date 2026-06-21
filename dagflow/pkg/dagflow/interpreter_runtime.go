package dagflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/oarkflow/interpreter"
)

type ScriptRuntime struct{ engine *Engine }

func NewScriptRuntime(e *Engine) *ScriptRuntime { return &ScriptRuntime{engine: e} }

func (r *ScriptRuntime) ExecuteNode(ctx context.Context, wf *Workflow, task *Task, node *Node, input any, attempt int) (any, error) {
	source := node.Script
	if source == "" && node.Handler != "" {
		r.engine.mu.RLock()
		source = r.engine.scriptHandlers[node.Handler]
		r.engine.mu.RUnlock()
	}
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("script node %s requires script source or handler script", node.ID)
	}
	data := map[string]interface{}{
		"input":    input,
		"params":   node.Params,
		"task":     task,
		"result":   task.LastResult,
		"nodes":    task.NodeResults,
		"workflow": map[string]any{"id": wf.ID, "version": wf.Version, "hash": wf.Hash},
		"node":     map[string]any{"id": node.ID, "attempt": attempt},
	}
	obj, err := interpreter.ExecWithOptions(source, data, interpreter.ExecOptions{Context: ctx, Profile: "trusted"})
	if err != nil {
		return nil, err
	}
	return interpreterObjectToAny(obj), nil
}

func interpreterObjectToAny(obj interpreter.Object) any {
	if obj == nil {
		return nil
	}
	// Prefer JSON when the object inspect value is JSON-like; otherwise return text.
	text := obj.Inspect()
	var v any
	if err := json.Unmarshal([]byte(text), &v); err == nil {
		return v
	}
	switch o := obj.(type) {
	case *interpreter.String:
		return o.Value
	case *interpreter.Integer:
		return o.Value
	case *interpreter.Float:
		return o.Value
	case *interpreter.Boolean:
		return o.Value
	case *interpreter.Null:
		return nil
	default:
		return text
	}
}
