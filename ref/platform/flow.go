package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

// In-graph control flow.
//
// A REF intent is a dependency DAG: the compiler proves every fact has exactly
// one producer, rejects cycles, and runs independent nodes concurrently. What it
// deliberately does not have is traversal — every node in a compiled plan runs,
// gated only by policy decisions and speculation class. There is no "skip this
// subgraph".
//
// That is a feature, not a gap. It is what makes the plan provable. But an
// application still needs to branch, iterate, race and retry, so the control flow
// lives *inside* a node: a flow.* action invokes one or more child intents through
// the same engine, and the branch not taken is simply never dispatched. The
// caller's plan stays a DAG with one producer per fact; the child's plan is
// separately compiled and separately proven.
//
// Three properties fall out of doing it this way, and all three matter:
//
//   - The compiler still knows the whole reachable set. Every child intent is
//     named in configuration, so a missing intent is a load-time error and the
//     catalog can show the real call graph.
//   - Recursion is bounded. Each nested call increments a depth counter and a
//     per-invocation call budget, so a cyclic flow.subflow fails with a clear
//     error instead of exhausting the stack.
//   - Nothing here is durable. Every flow.* action completes within the request.
//     Anything that must survive a restart — an approval, a timer, a saga —
//     belongs in a process, and this file never pretends otherwise.

// maxFlowDepth bounds nesting. Ten levels is far more than any legible
// configuration uses, and well short of anything that threatens the stack.
const maxFlowDepth = 10

// maxFlowCalls bounds the total child invocations one request may make, so a
// foreach over an unexpectedly large collection degrades into a clear error
// rather than a slow-motion outage.
const maxFlowCalls = 1000

// ErrFlowBudget reports that an invocation exceeded its child-call budget.
var ErrFlowBudget = errors.New("ref/platform: too many nested intent calls")

func registerFlowActions(r *Registry) {
	mustAction(r, "flow.subflow", flowSubflowAction, ActionInfo{
		Family:   "flow",
		Summary:  "Invoke one child intent and publish its result",
		Provides: "The child intent's result",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intent", Type: "intent", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input", Summary: "What the child receives as its input"},
			{Name: "input", Type: "map", Summary: "Literal input, with templated string values"},
		},
	})

	mustAction(r, "flow.branch", flowBranchAction, ActionInfo{
		Family:   "flow",
		Summary:  "Run the child intent of the first case whose condition holds",
		Provides: "An object with outcome, case and result",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "cases", Type: "[]object", Required: true, Summary: `Ordered: cases [ { name … condition … intent … } … ]`},
			{Name: "default_intent", Type: "intent", Summary: "Run when no case matches"},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "required", Type: "bool", Default: "true", Summary: "Fail when nothing matches and there is no default"},
		},
	})

	mustAction(r, "flow.switch", flowSwitchAction, ActionInfo{
		Family:   "flow",
		Summary:  "Match a value against case labels and run the matching child intent",
		Provides: "An object with outcome, case and result",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "on", Type: "expression", Required: true},
			{Name: "cases", Type: "[]object", Required: true, Summary: `cases [ { label … intent … } … ], matched against the value of on`},
			{Name: "default_intent", Type: "intent"},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "required", Type: "bool", Default: "true"},
		},
	})

	mustAction(r, "flow.foreach", flowForeachAction, ActionInfo{
		Family:   "flow",
		Summary:  "Run a child intent once per element, with bounded concurrency",
		Provides: "An object with results, errors and counts",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intent", Type: "intent", Required: true},
			{Name: "items_fact", Type: "fact", Required: true},
			{Name: "concurrency", Type: "int", Default: "1", Summary: "1 runs sequentially, which is what an ordered side effect needs"},
			{Name: "continue_on_error", Type: "bool", Summary: "Collect failures instead of stopping at the first"},
			{Name: "max_items", Type: "int", Default: "500"},
			{Name: "item_key", Type: "string", Default: "item", Summary: "Key the element is passed under"},
		},
	})

	mustAction(r, "flow.parallel_map", flowForeachAction, ActionInfo{
		Family:   "flow",
		Summary:  "flow.foreach with concurrency defaulting to the element count",
		Provides: "An object with results, errors and counts",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intent", Type: "intent", Required: true},
			{Name: "items_fact", Type: "fact", Required: true},
			{Name: "concurrency", Type: "int", Default: "8"},
			{Name: "continue_on_error", Type: "bool"},
			{Name: "max_items", Type: "int", Default: "500"},
		},
	})

	mustAction(r, "flow.parallel", flowParallelAction, ActionInfo{
		Family:   "flow",
		Summary:  "Run named child intents concurrently and publish their results together",
		Provides: "An object keyed by branch name",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "branches", Type: "[]object", Required: true, Summary: `branches [ { name … intent … } … ]`},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "fail_fast", Type: "bool", Default: "true", Summary: "Cancel siblings as soon as one fails"},
			{Name: "continue_on_error", Type: "bool"},
		},
	})

	mustAction(r, "flow.race", flowRaceAction, ActionInfo{
		Family:   "flow",
		Summary:  "Run branches concurrently and publish the first successful result",
		Provides: "An object with winner and result",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "branches", Type: "[]object", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "cancel_losers", Type: "bool", Default: "true", Summary: "Turn off when the losing branches have effects worth completing"},
			{Name: "timeout", Type: "duration"},
		},
	})

	mustAction(r, "flow.quorum", flowQuorumAction, ActionInfo{
		Family:   "flow",
		Summary:  "Run branches concurrently and succeed once enough of them do",
		Provides: "An object with results, errors and whether quorum was reached",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "branches", Type: "[]object", Required: true},
			{Name: "quorum", Type: "int", Summary: "Successes required. Zero means a simple majority."},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "timeout", Type: "duration"},
		},
	})

	mustAction(r, "flow.retry", flowRetryAction, ActionInfo{
		Family:   "flow",
		Summary:  "Invoke a child intent, retrying transient failures with backoff",
		Provides: "The child intent's result",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intent", Type: "intent", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "max_attempts", Type: "int", Default: "3"},
			{Name: "strategy", Type: "string", Default: "exponential", Summary: "fixed, linear, exponential or exponential_jitter"},
			{Name: "initial_delay", Type: "duration", Default: "100ms"},
			{Name: "max_delay", Type: "duration", Default: "5s"},
			{Name: "jitter", Type: "bool"},
			{Name: "retry_on", Type: "[]string", Summary: "Failure categories to retry. Default is the transient ones only."},
		},
	})

	mustAction(r, "flow.timeout", flowTimeoutAction, ActionInfo{
		Family:   "flow",
		Summary:  "Invoke a child intent under a deadline",
		Provides: "The child intent's result",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intent", Type: "intent", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "timeout", Type: "duration", Required: true},
			{Name: "fallback", Type: "any", Summary: "Published instead of failing when the deadline passes"},
		},
	})

	mustAction(r, "flow.fallback", flowFallbackAction, ActionInfo{
		Family:   "flow",
		Summary:  "Try child intents in order until one succeeds",
		Provides: "An object with result, intent and attempts",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intents", Type: "[]intent", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "fallback", Type: "any", Summary: "Published when every alternative fails"},
		},
	})

	mustAction(r, "flow.pipeline", flowPipelineAction, ActionInfo{
		Family:   "flow",
		Summary:  "Invoke child intents in sequence, each receiving the previous result",
		Provides: "The final result, with every stage's output",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intents", Type: "[]intent", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
		},
	})

	mustAction(r, "flow.loop_until", flowLoopUntilAction, ActionInfo{
		Family:   "flow",
		Summary:  "Invoke a child intent repeatedly until a condition holds",
		Provides: "An object with result, cycles and whether the condition was met",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "intent", Type: "intent", Required: true},
			{Name: "input_fact", Type: "fact", Default: "input"},
			{Name: "condition", Type: "expression", Required: true, Summary: "Sees the last result as `result` and the cycle number as `cycle`"},
			{Name: "max_cycles", Type: "int", Default: "10"},
			{Name: "delay", Type: "duration", Summary: "Pause between cycles"},
			{Name: "required", Type: "bool", Default: "true", Summary: "Fail when max_cycles passes without the condition holding"},
		},
	})

	mustAction(r, "flow.terminate", flowTerminateAction, ActionInfo{
		Family:  "flow",
		Summary: "End the invocation immediately with a value, skipping the rest of the plan",
		Kind:    "pure",
		Config: []ConfigField{
			{Name: "value_fact", Type: "fact", Summary: "Value to return"},
			{Name: "value", Type: "any", Summary: "Literal value to return"},
			{Name: "condition", Type: "expression", Summary: "Only terminate when this holds. Spelled condition because BCL reserves when."},
		},
	})
}

// ---------------------------------------------------------------------------
// Child invocation
// ---------------------------------------------------------------------------

// flowCallState tracks one request's nested-call budget. It lives in the context
// so it is shared by every flow node in the invocation, including nodes inside
// child intents.
type flowCallState struct {
	mu    sync.Mutex
	calls int
}

type flowStateKey struct{}

func (s *flowCallState) consume() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls > maxFlowCalls {
		return ErrFlowBudget
	}
	return nil
}

// CallIntent invokes another intent within the current request.
//
// It exists so flow.* actions — and only they — can compose intents. The
// principal, tenant and depth travel with the call, so a child intent's
// tenant-scoped query and authorization gate see the same identity the parent
// did, and a nested cycle is caught rather than crashing.
func (p *Platform) CallIntent(ctx context.Context, name string, input any, parent *ActionContext) (any, error) {
	if p == nil || p.Engine == nil {
		return nil, unavailable("the platform is not running")
	}
	depth := 0
	if parent != nil {
		depth = parent.Depth
	}
	if depth >= maxFlowDepth {
		return nil, invalidInput("intent %q is nested more than %d levels deep; there is probably a cycle in your flow nodes", name, maxFlowDepth)
	}

	state, ok := ctx.Value(flowStateKey{}).(*flowCallState)
	if !ok {
		state = &flowCallState{}
		ctx = context.WithValue(ctx, flowStateKey{}, state)
	}
	if err := state.consume(); err != nil {
		return nil, invalidInput("this request made more than %d nested intent calls", maxFlowCalls)
	}

	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, invalidInput("the input for intent %q cannot be serialised: %v", name, err)
	}

	inv := &invocation.Invocation{
		ID:       invocation.ID(newPrefixedID("sub")),
		Intent:   invocation.IntentID(name),
		Input:    invocation.NewInput(encoded, "application/json"),
		Received: time.Now(),
	}
	if parent != nil && parent.Invocation != nil {
		// The child inherits the parent's credentials and transport metadata, so
		// request.param and auth nodes behave the same inside a subflow as outside
		// one. Inheriting rather than clearing is deliberate: a subflow is part of
		// the same request, not a new one.
		inv.Principal = parent.Invocation.Principal
		inv.Metadata = parent.Invocation.Metadata
		inv.Transport = parent.Invocation.Transport
	}

	// Depth is carried on the context because the child's own ActionContexts are
	// constructed by the engine, not by this call.
	ctx = context.WithValue(ctx, flowDepthKey{}, depth+1)
	result, err := p.Engine.Dispatch(ctx, inv)
	if err != nil {
		return nil, err
	}
	return result.Value, nil
}

type flowDepthKey struct{}

// flowDepth reads the nesting depth a context carries.
func flowDepth(ctx context.Context) int {
	depth, _ := ctx.Value(flowDepthKey{}).(int)
	return depth
}

// childInput assembles what a child intent receives: either a named fact, a
// literal templated object, or the caller's whole fact map.
type childInput struct {
	fact    string
	literal map[string]any
	tmpl    map[string]*Template
}

func compileChildInput(spec NodeSpec, config map[string]any, defaultFact string) (*childInput, error) {
	input := &childInput{fact: configString(config, "input_fact", defaultFact)}
	if literal := configMap(config, "input"); len(literal) > 0 {
		input.literal = literal
		input.tmpl = map[string]*Template{}
		for key, value := range literal {
			text, ok := value.(string)
			if !ok {
				continue
			}
			tmpl, err := CompileTemplate(text)
			if err != nil {
				return nil, fmt.Errorf("node %q: input.%s: %w", spec.Name, key, err)
			}
			if tmpl.HasHoles() {
				input.tmpl[key] = tmpl
			}
		}
	}
	return input, nil
}

func (c *childInput) resolve(ctx *ActionContext, env Env) (any, error) {
	if c.literal != nil {
		payload := make(map[string]any, len(c.literal))
		for key, value := range c.literal {
			if tmpl, templated := c.tmpl[key]; templated {
				rendered, err := tmpl.Render(env)
				if err != nil {
					return nil, err
				}
				payload[key] = rendered
				continue
			}
			payload[key] = value
		}
		return payload, nil
	}
	if c.fact == "" {
		return ctx.Inputs, nil
	}
	value, found := resolvePath(ctx.Inputs, c.fact)
	if !found {
		// Passing nothing is better than failing: a child intent whose input is
		// optional should still be callable, and one that requires it will say so
		// in its own validation with a message about its own fields.
		return map[string]any{}, nil
	}
	return value, nil
}

// requireIntent checks at build time that a named child intent exists in the
// document, so a typo fails the deployment rather than the one request that took
// that branch.
func requireIntent(build BuildContext, spec NodeSpec, name string) error {
	if name == "" {
		return fmt.Errorf("node %q: an intent name is required", spec.Name)
	}
	if build.Document == nil {
		return nil
	}
	if slices.ContainsFunc(build.Document.Intents, func(item IntentSpec) bool { return item.Name == name }) {
		return nil
	}
	return fmt.Errorf("node %q: intent %q is not declared in this application", spec.Name, name)
}

// ---------------------------------------------------------------------------
// Sequential composition
// ---------------------------------------------------------------------------

var flowSubflowAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	name := configString(spec.Config, "intent", "")
	if err := requireIntent(build, spec, name); err != nil {
		return nil, err
	}
	input, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		payload, err := input.resolve(ctx, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		result, err := ctx.Platform.CallIntent(ctx.Context, name, payload, ctx)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, result), nil
	}), nil
})

var flowPipelineAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	names := configStrings(spec.Config, "intents")
	if len(names) == 0 {
		return nil, fmt.Errorf("node %q: flow.pipeline needs config.intents", spec.Name)
	}
	for _, name := range names {
		if err := requireIntent(build, spec, name); err != nil {
			return nil, err
		}
	}
	input, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		payload, err := input.resolve(ctx, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		stages := make(map[string]any, len(names))
		for _, name := range names {
			result, err := ctx.Platform.CallIntent(ctx.Context, name, payload, ctx)
			if err != nil {
				return ActionResult{}, fmt.Errorf("pipeline stage %q: %w", name, err)
			}
			stages[name] = result
			// Each stage receives the previous stage's output, which is what makes
			// this a pipeline rather than a fan-out.
			payload = result
		}
		return singleOutput(spec, map[string]any{"result": payload, "stages": stages}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Branching
// ---------------------------------------------------------------------------

// flowCase is one arm of a branch or switch.
type flowCase struct {
	name   string
	label  string
	when   *Expression
	intent string
	input  *childInput
}

func compileFlowCases(build BuildContext, spec NodeSpec, needWhen bool) ([]flowCase, error) {
	blocks := configBlocks(spec.Config, "cases", "case")
	if len(blocks) == 0 {
		return nil, fmt.Errorf("node %q: %s needs a config.cases list, e.g. cases [ { condition \"…\" intent \"…\" } ]", spec.Name, spec.Uses)
	}
	cases := make([]flowCase, 0, len(blocks))
	for i, block := range blocks {
		name := configString(block, "name", fmt.Sprintf("case_%d", i))
		target := configString(block, "intent", "")
		if err := requireIntent(build, spec, target); err != nil {
			return nil, fmt.Errorf("case %q: %w", name, err)
		}
		when, err := CompileExpr(configString(block, "condition", ""))
		if err != nil {
			return nil, fmt.Errorf("node %q: case %q: %w", spec.Name, name, err)
		}
		if needWhen && when == nil {
			return nil, fmt.Errorf("node %q: case %q needs a condition", spec.Name, name)
		}
		input, err := compileChildInput(spec, block, configString(spec.Config, "input_fact", "input"))
		if err != nil {
			return nil, err
		}
		cases = append(cases, flowCase{
			name:   name,
			label:  configString(block, "label", configString(block, "match", name)),
			when:   when,
			intent: target,
			input:  input,
		})
	}
	return cases, nil
}

var flowBranchAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	cases, err := compileFlowCases(build, spec, true)
	if err != nil {
		return nil, err
	}
	defaultIntent := configString(spec.Config, "default_intent", "")
	if defaultIntent != "" {
		if err := requireIntent(build, spec, defaultIntent); err != nil {
			return nil, err
		}
	}
	defaultInput, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}
	required := configBool(spec.Config, "required", true)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		for _, arm := range cases {
			matched, err := arm.when.Bool(env)
			if err != nil {
				return ActionResult{}, fmt.Errorf("case %q: %w", arm.name, err)
			}
			if !matched {
				continue
			}
			payload, err := arm.input.resolve(ctx, env)
			if err != nil {
				return ActionResult{}, err
			}
			result, err := ctx.Platform.CallIntent(ctx.Context, arm.intent, payload, ctx)
			if err != nil {
				return ActionResult{}, err
			}
			return singleOutput(spec, map[string]any{
				"matched": true,
				"case":    arm.name,
				"intent":  arm.intent,
				"result":  result,
			}), nil
		}
		return flowNoMatch(ctx, spec, env, defaultIntent, defaultInput, required)
	}), nil
})

var flowSwitchAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	on, err := requiredExpr(spec.Config, "on")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	cases, err := compileFlowCases(build, spec, false)
	if err != nil {
		return nil, err
	}
	defaultIntent := configString(spec.Config, "default_intent", "")
	if defaultIntent != "" {
		if err := requireIntent(build, spec, defaultIntent); err != nil {
			return nil, err
		}
	}
	defaultInput, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}
	required := configBool(spec.Config, "required", true)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		value, err := on.String(env)
		if err != nil {
			return ActionResult{}, err
		}
		for _, arm := range cases {
			if arm.label != value {
				continue
			}
			payload, err := arm.input.resolve(ctx, env)
			if err != nil {
				return ActionResult{}, err
			}
			result, err := ctx.Platform.CallIntent(ctx.Context, arm.intent, payload, ctx)
			if err != nil {
				return ActionResult{}, err
			}
			return singleOutput(spec, map[string]any{
				"matched": true,
				"case":    arm.name,
				"value":   value,
				"intent":  arm.intent,
				"result":  result,
			}), nil
		}
		return flowNoMatch(ctx, spec, env, defaultIntent, defaultInput, required)
	}), nil
})

// flowNoMatch handles the case where nothing matched: run the default, or fail.
//
// Failing by default is the right posture. A branch that silently does nothing
// because the author forgot a case is a bug that produces a successful-looking
// response, which is much harder to notice than an error.
func flowNoMatch(ctx *ActionContext, spec NodeSpec, env Env, defaultIntent string, input *childInput, required bool) (ActionResult, error) {
	if defaultIntent != "" {
		payload, err := input.resolve(ctx, env)
		if err != nil {
			return ActionResult{}, err
		}
		result, err := ctx.Platform.CallIntent(ctx.Context, defaultIntent, payload, ctx)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, map[string]any{
			"matched": false,
			"case":    "default",
			"intent":  defaultIntent,
			"result":  result,
		}), nil
	}
	if required {
		return ActionResult{}, invalidInput("no branch matched in %q, and it declares no default_intent", spec.Name)
	}
	return singleOutput(spec, map[string]any{"matched": false, "result": nil}), nil
}

// ---------------------------------------------------------------------------
// Iteration
// ---------------------------------------------------------------------------

var flowForeachAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	name := configString(spec.Config, "intent", "")
	if err := requireIntent(build, spec, name); err != nil {
		return nil, err
	}
	itemsFact, err := requiredString(spec.Config, "items_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	defaultConcurrency := 1
	if spec.Uses == "flow.parallel_map" {
		defaultConcurrency = 8
	}
	concurrency, err := configInt(spec.Config, "concurrency", defaultConcurrency)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if concurrency < 1 {
		concurrency = 1
	}
	maxItems, err := configInt(spec.Config, "max_items", 500)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	continueOnError := configBool(spec.Config, "continue_on_error", false)
	itemKey := configString(spec.Config, "item_key", "item")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, itemsFact)
		items, err := requiredList(value, itemsFact)
		if err != nil {
			return ActionResult{}, err
		}
		if len(items) > maxItems {
			return ActionResult{}, invalidInput("%q has %d items, over this node's max_items of %d", itemsFact, len(items), maxItems)
		}
		if len(items) == 0 {
			return singleOutput(spec, map[string]any{"results": []any{}, "errors": []any{}, "succeeded": 0, "failed": 0}), nil
		}

		results := make([]any, len(items))
		failures := make([]any, len(items))
		var (
			wg        sync.WaitGroup
			semaphore = make(chan struct{}, min(concurrency, len(items)))
			firstErr  error
			errOnce   sync.Once
		)
		// Cancelling siblings on the first failure is only correct when we are not
		// collecting errors; with continue_on_error the caller asked for every
		// result, including the ones after a failure.
		runCtx := ctx.Context
		var cancel context.CancelFunc
		if !continueOnError {
			runCtx, cancel = context.WithCancel(ctx.Context)
			defer cancel()
		}

		for index, item := range items {
			wg.Add(1)
			go func(index int, item any) {
				defer wg.Done()
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-runCtx.Done():
					return
				}
				payload := map[string]any{itemKey: item, "index": index, "total": len(items)}
				if object, ok := item.(map[string]any); ok {
					// A record element is passed through directly as well as under
					// item, so a child intent written against the record's own shape
					// works without an adapter.
					for key, nested := range object {
						if _, taken := payload[key]; !taken {
							payload[key] = nested
						}
					}
				}
				result, err := ctx.Platform.CallIntent(runCtx, name, payload, ctx)
				if err != nil {
					failures[index] = map[string]any{"index": index, "error": err.Error()}
					errOnce.Do(func() { firstErr = err })
					if !continueOnError && cancel != nil {
						cancel()
					}
					return
				}
				results[index] = result
			}(index, item)
		}
		wg.Wait()

		if firstErr != nil && !continueOnError {
			return ActionResult{}, firstErr
		}
		succeeded, collected := 0, make([]any, 0, len(items))
		errorList := make([]any, 0)
		for index := range items {
			if failures[index] != nil {
				errorList = append(errorList, failures[index])
				continue
			}
			collected = append(collected, results[index])
			succeeded++
		}
		return singleOutput(spec, map[string]any{
			"results":   collected,
			"errors":    errorList,
			"succeeded": succeeded,
			"failed":    len(errorList),
			"total":     len(items),
		}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

// flowBranch is one named concurrent branch.
type flowBranch struct {
	name   string
	intent string
	input  *childInput
	weight float64
}

func compileFlowBranches(build BuildContext, spec NodeSpec) ([]flowBranch, error) {
	blocks := configBlocks(spec.Config, "branches", "branch")
	if len(blocks) == 0 {
		return nil, fmt.Errorf("node %q: %s needs a config.branches list, e.g. branches [ { name \"…\" intent \"…\" } ]", spec.Name, spec.Uses)
	}
	branches := make([]flowBranch, 0, len(blocks))
	for i, block := range blocks {
		name := configString(block, "name", fmt.Sprintf("branch_%d", i))
		target := configString(block, "intent", "")
		if err := requireIntent(build, spec, target); err != nil {
			return nil, fmt.Errorf("branch %q: %w", name, err)
		}
		input, err := compileChildInput(spec, block, configString(spec.Config, "input_fact", "input"))
		if err != nil {
			return nil, err
		}
		weight, err := configFloat(block, "weight", 1)
		if err != nil {
			return nil, fmt.Errorf("node %q: branch %q: %w", spec.Name, name, err)
		}
		branches = append(branches, flowBranch{name: name, intent: target, input: input, weight: weight})
	}
	return branches, nil
}

// branchOutcome is one concurrent branch's result.
type branchOutcome struct {
	name   string
	result any
	err    error
}

// runBranches executes branches concurrently and collects every outcome. The
// caller decides what the collection means — all must succeed, one must, or
// enough must.
func runBranches(ctx *ActionContext, branches []flowBranch, timeout time.Duration, cancelOnFirstSuccess, cancelOnFirstFailure bool) []branchOutcome {
	runCtx := ctx.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(runCtx, timeout)
	} else {
		runCtx, cancel = context.WithCancel(runCtx)
	}
	defer cancel()

	env := actionEnv(ctx)
	outcomes := make([]branchOutcome, len(branches))
	var wg sync.WaitGroup
	var settled sync.Once

	for index, branch := range branches {
		wg.Add(1)
		go func(index int, branch flowBranch) {
			defer wg.Done()
			outcomes[index].name = branch.name
			payload, err := branch.input.resolve(ctx, env)
			if err != nil {
				outcomes[index].err = err
				return
			}
			result, err := ctx.Platform.CallIntent(runCtx, branch.intent, payload, ctx)
			outcomes[index].result, outcomes[index].err = result, err
			switch {
			case err == nil && cancelOnFirstSuccess:
				settled.Do(cancel)
			case err != nil && cancelOnFirstFailure:
				settled.Do(cancel)
			}
		}(index, branch)
	}
	wg.Wait()
	return outcomes
}

var flowParallelAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	branches, err := compileFlowBranches(build, spec)
	if err != nil {
		return nil, err
	}
	continueOnError := configBool(spec.Config, "continue_on_error", false)
	failFast := configBool(spec.Config, "fail_fast", !continueOnError)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		outcomes := runBranches(ctx, branches, 0, false, failFast && !continueOnError)
		results := make(map[string]any, len(outcomes))
		errorList := make([]any, 0)
		var firstErr error
		for _, outcome := range outcomes {
			if outcome.err != nil {
				errorList = append(errorList, map[string]any{"branch": outcome.name, "error": outcome.err.Error()})
				if firstErr == nil {
					firstErr = fmt.Errorf("branch %q: %w", outcome.name, outcome.err)
				}
				continue
			}
			results[outcome.name] = outcome.result
		}
		if firstErr != nil && !continueOnError {
			return ActionResult{}, firstErr
		}
		return singleOutput(spec, map[string]any{
			"results":   results,
			"errors":    errorList,
			"succeeded": len(results),
			"failed":    len(errorList),
		}), nil
	}), nil
})

var flowRaceAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	branches, err := compileFlowBranches(build, spec)
	if err != nil {
		return nil, err
	}
	cancelLosers := configBool(spec.Config, "cancel_losers", true)
	timeout, err := configDuration(spec.Config, "timeout", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		outcomes := runBranches(ctx, branches, timeout, cancelLosers, false)
		// Branch order decides the winner among several successes, so the result is
		// deterministic for a given configuration rather than depending on
		// goroutine scheduling.
		for _, outcome := range outcomes {
			if outcome.err == nil {
				return singleOutput(spec, map[string]any{"winner": outcome.name, "result": outcome.result}), nil
			}
		}
		messages := make([]string, 0, len(outcomes))
		for _, outcome := range outcomes {
			if outcome.err != nil {
				messages = append(messages, outcome.name+": "+outcome.err.Error())
			}
		}
		return ActionResult{}, unavailable("every branch failed: %s", strings.Join(messages, "; "))
	}), nil
})

var flowQuorumAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	branches, err := compileFlowBranches(build, spec)
	if err != nil {
		return nil, err
	}
	quorum, err := configInt(spec.Config, "quorum", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if quorum <= 0 {
		quorum = len(branches)/2 + 1
	}
	if quorum > len(branches) {
		return nil, fmt.Errorf("node %q: quorum of %d cannot be met by %d branches", spec.Name, quorum, len(branches))
	}
	timeout, err := configDuration(spec.Config, "timeout", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		outcomes := runBranches(ctx, branches, timeout, false, false)
		results := make(map[string]any, len(outcomes))
		errorList := make([]any, 0)
		for _, outcome := range outcomes {
			if outcome.err != nil {
				errorList = append(errorList, map[string]any{"branch": outcome.name, "error": outcome.err.Error()})
				continue
			}
			results[outcome.name] = outcome.result
		}
		if len(results) < quorum {
			return ActionResult{}, unavailable("only %d of %d branches succeeded, and %d were required", len(results), len(branches), quorum)
		}
		return singleOutput(spec, map[string]any{
			"results":   results,
			"errors":    errorList,
			"succeeded": len(results),
			"quorum":    quorum,
			"reached":   true,
		}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Reliability
// ---------------------------------------------------------------------------

// retryPolicy is a compiled RetrySpec.
type retryPolicy struct {
	attempts     int
	strategy     string
	initialDelay time.Duration
	maxDelay     time.Duration
	jitter       bool
	categories   map[intent.Category]bool
}

// transientCategories are the failures worth retrying by default. Retrying an
// invalid input or a permission denial would fail identically every time and
// multiply the load while doing it.
var transientCategories = map[intent.Category]bool{
	intent.CategoryUnavailable: true,
	intent.CategoryTimeout:     true,
	intent.CategoryConflict:    true,
	intent.CategoryRateLimit:   true,
}

func compileRetryFromConfig(spec NodeSpec) (*retryPolicy, error) {
	attempts, err := configInt(spec.Config, "max_attempts", 3)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	initial, err := configDuration(spec.Config, "initial_delay", 100*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	maxDelay, err := configDuration(spec.Config, "max_delay", 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	policy, err := compileRetrySpec(&RetrySpec{
		MaxAttempts: attempts,
		Strategy:    configString(spec.Config, "strategy", "exponential"),
		Jitter:      configBool(spec.Config, "jitter", false),
		RetryOn:     configStrings(spec.Config, "retry_on"),
	})
	if err != nil {
		return nil, err
	}
	policy.initialDelay, policy.maxDelay = initial, maxDelay
	return policy, nil
}

// compileRetrySpec validates a retry policy. It is shared by flow.retry, intent
// nodes and process steps, so all three behave identically.
func compileRetrySpec(spec *RetrySpec) (*retryPolicy, error) {
	if spec == nil {
		return nil, nil
	}
	initial, err := spec.InitialDelay.Parse("retry initial_delay", 100*time.Millisecond)
	if err != nil {
		return nil, err
	}
	maxDelay, err := spec.MaxDelay.Parse("retry max_delay", 5*time.Second)
	if err != nil {
		return nil, err
	}
	policy := &retryPolicy{
		attempts:     max(spec.MaxAttempts, 1),
		strategy:     strings.ToLower(strings.TrimSpace(spec.Strategy)),
		initialDelay: initial,
		maxDelay:     maxDelay,
		jitter:       spec.Jitter,
	}
	if policy.strategy == "" {
		policy.strategy = "exponential"
	}
	if !slices.Contains([]string{"fixed", "linear", "exponential", "exponential_jitter", "decorrelated_jitter"}, policy.strategy) {
		return nil, fmt.Errorf("unknown retry strategy %q", spec.Strategy)
	}
	if policy.initialDelay <= 0 {
		policy.initialDelay = 100 * time.Millisecond
	}
	if policy.maxDelay <= 0 {
		policy.maxDelay = 5 * time.Second
	}
	if len(spec.RetryOn) > 0 {
		policy.categories = map[intent.Category]bool{}
		for _, name := range spec.RetryOn {
			category, err := parseFailureCategory(name)
			if err != nil {
				return nil, err
			}
			policy.categories[category] = true
		}
	}
	return policy, nil
}

// shouldRetry decides whether a failure is worth another attempt.
func (p *retryPolicy) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// A cancelled context means the caller has gone or the deadline passed.
		// Retrying inside it cannot succeed.
		return false
	}
	var failure intent.Failure
	if !errors.As(err, &failure) {
		// An unclassified error is retried: it is most likely an infrastructure
		// failure that did not go through intent.Failure, and those are the ones
		// retries exist for.
		return true
	}
	if p.categories != nil {
		return p.categories[failure.Category]
	}
	return transientCategories[failure.Category]
}

// delay computes the wait before a given attempt, capped and optionally
// jittered. Jitter matters when many replicas retry the same dependency: without
// it they retry in lockstep and the dependency never gets a quiet moment.
func (p *retryPolicy) delay(attempt int) time.Duration {
	var wait time.Duration
	switch p.strategy {
	case "fixed":
		wait = p.initialDelay
	case "linear":
		wait = time.Duration(attempt) * p.initialDelay
	default: // exponential and its jittered variants
		wait = p.initialDelay << min(attempt-1, 16)
	}
	if wait > p.maxDelay || wait <= 0 {
		wait = p.maxDelay
	}
	if p.jitter || p.strategy == "exponential_jitter" || p.strategy == "decorrelated_jitter" {
		half := wait / 2
		wait = half + time.Duration(rand.Int64N(int64(half)+1))
	}
	return wait
}

func parseFailureCategory(name string) (intent.Category, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "invalid_input":
		return intent.CategoryInvalidInput, nil
	case "not_found":
		return intent.CategoryNotFound, nil
	case "conflict":
		return intent.CategoryConflict, nil
	case "permission":
		return intent.CategoryPermission, nil
	case "auth":
		return intent.CategoryAuth, nil
	case "rate_limit":
		return intent.CategoryRateLimit, nil
	case "unavailable":
		return intent.CategoryUnavailable, nil
	case "timeout":
		return intent.CategoryTimeout, nil
	case "internal":
		return intent.CategoryInternal, nil
	default:
		return 0, fmt.Errorf("unknown failure category %q", name)
	}
}

var flowRetryAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	name := configString(spec.Config, "intent", "")
	if err := requireIntent(build, spec, name); err != nil {
		return nil, err
	}
	policy, err := compileRetryFromConfig(spec)
	if err != nil {
		return nil, err
	}
	input, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		payload, err := input.resolve(ctx, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		var lastErr error
		for attempt := 1; attempt <= policy.attempts; attempt++ {
			result, err := ctx.Platform.CallIntent(ctx.Context, name, payload, ctx)
			if err == nil {
				return singleOutput(spec, result), nil
			}
			lastErr = err
			if attempt == policy.attempts || !policy.shouldRetry(err) {
				break
			}
			select {
			case <-ctx.Context.Done():
				return ActionResult{}, ctx.Context.Err()
			case <-time.After(policy.delay(attempt)):
			}
		}
		return ActionResult{}, lastErr
	}), nil
})

var flowTimeoutAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	name := configString(spec.Config, "intent", "")
	if err := requireIntent(build, spec, name); err != nil {
		return nil, err
	}
	timeout, err := configDuration(spec.Config, "timeout", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("node %q: flow.timeout needs a positive timeout", spec.Name)
	}
	fallback, hasFallback := spec.Config["fallback"]
	input, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		payload, err := input.resolve(ctx, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		runCtx, cancel := context.WithTimeout(ctx.Context, timeout)
		defer cancel()

		result, err := ctx.Platform.CallIntent(runCtx, name, payload, ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || runCtx.Err() != nil {
				if hasFallback {
					return singleOutput(spec, fallback), nil
				}
				return ActionResult{}, intent.Failure{
					Code:     "TIMEOUT",
					Category: intent.CategoryTimeout,
					Message:  fmt.Sprintf("%s did not finish within %s", name, timeout),
					Cause:    err,
				}
			}
			return ActionResult{}, err
		}
		return singleOutput(spec, result), nil
	}), nil
})

var flowFallbackAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	names := configStrings(spec.Config, "intents")
	if len(names) < 2 {
		return nil, fmt.Errorf("node %q: flow.fallback needs at least two intents", spec.Name)
	}
	for _, name := range names {
		if err := requireIntent(build, spec, name); err != nil {
			return nil, err
		}
	}
	fallback, hasFallback := spec.Config["fallback"]
	input, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		payload, err := input.resolve(ctx, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		attempts := make([]any, 0, len(names))
		for _, name := range names {
			result, err := ctx.Platform.CallIntent(ctx.Context, name, payload, ctx)
			if err == nil {
				return singleOutput(spec, map[string]any{
					"result":   result,
					"intent":   name,
					"attempts": attempts,
				}), nil
			}
			attempts = append(attempts, map[string]any{"intent": name, "error": err.Error()})
			// An invalid input or a permission denial will fail identically for
			// every alternative, so there is nothing to fall back to.
			var failure intent.Failure
			if errors.As(err, &failure) &&
				(failure.Category == intent.CategoryInvalidInput || failure.Category == intent.CategoryPermission) {
				if hasFallback {
					return singleOutput(spec, map[string]any{"result": fallback, "intent": "", "attempts": attempts}), nil
				}
				return ActionResult{}, err
			}
		}
		if hasFallback {
			return singleOutput(spec, map[string]any{"result": fallback, "intent": "", "attempts": attempts}), nil
		}
		return ActionResult{}, unavailable("every alternative failed in %q", spec.Name)
	}), nil
})

var flowLoopUntilAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	name := configString(spec.Config, "intent", "")
	if err := requireIntent(build, spec, name); err != nil {
		return nil, err
	}
	condition, err := requiredExpr(spec.Config, "condition")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	maxCycles, err := configInt(spec.Config, "max_cycles", 10)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if maxCycles < 1 {
		return nil, fmt.Errorf("node %q: max_cycles must be at least 1", spec.Name)
	}
	delay, err := configDuration(spec.Config, "delay", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	required := configBool(spec.Config, "required", true)
	input, err := compileChildInput(spec, spec.Config, "input")
	if err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		payload, err := input.resolve(ctx, env)
		if err != nil {
			return ActionResult{}, err
		}
		var last any
		for cycle := 1; cycle <= maxCycles; cycle++ {
			result, err := ctx.Platform.CallIntent(ctx.Context, name, payload, ctx)
			if err != nil {
				return ActionResult{}, err
			}
			last = result

			loopEnv := make(Env, len(env)+2)
			for key, value := range env {
				loopEnv[key] = value
			}
			loopEnv["result"] = result
			loopEnv["cycle"] = cycle
			done, err := condition.Bool(loopEnv)
			if err != nil {
				return ActionResult{}, err
			}
			if done {
				return singleOutput(spec, map[string]any{"result": result, "cycles": cycle, "condition_met": true}), nil
			}
			// The next cycle feeds on this one's result, which is what lets a loop
			// converge rather than repeat the same call.
			payload = result
			if cycle < maxCycles && delay > 0 {
				select {
				case <-ctx.Context.Done():
					return ActionResult{}, ctx.Context.Err()
				case <-time.After(delay):
				}
			}
		}
		if required {
			return ActionResult{}, unavailable("%q ran %d cycles without its condition holding", spec.Name, maxCycles)
		}
		return singleOutput(spec, map[string]any{"result": last, "cycles": maxCycles, "condition_met": false}), nil
	}), nil
})

var flowTerminateAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	valueFact := configString(spec.Config, "value_fact", "")
	literal, hasLiteral := spec.Config["value"]
	condition, err := configExpr(spec.Config, "condition")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if valueFact == "" && !hasLiteral {
		return nil, fmt.Errorf("node %q: flow.terminate needs config.value_fact or config.value", spec.Name)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		if condition != nil {
			should, err := condition.Bool(actionEnv(ctx))
			if err != nil {
				return ActionResult{}, err
			}
			if !should {
				return acknowledgement(spec, false), nil
			}
		}
		value := literal
		if valueFact != "" {
			if resolved, found := resolvePath(ctx.Inputs, valueFact); found {
				value = resolved
			}
		}
		// Short-circuiting ends the whole invocation with this value. Every node
		// still running is cancelled, and no further effect commits.
		ctx.Node.SetShortCircuit(value)
		return acknowledgement(spec, true), nil
	}), nil
})
