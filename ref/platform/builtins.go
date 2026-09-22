package platform

import (
	"fmt"
	"maps"
)

// registerBuiltins seeds a Registry with the whole built-in catalog: the node
// and edge type taxonomies, every stdlib-backed resource provider, and every
// action.
//
// Registration order matters only in that node and edge types come first, so a
// resource or action registration can reference a type name that already
// exists. Everything else is independent.
func registerBuiltins(r *Registry) {
	registerNodeTypes(r)
	registerEdgeTypes(r)

	registerBuiltinResources(r)

	registerCoreActions(r)
	registerDataActions(r)
	registerDatabaseActions(r)
	registerCacheActions(r)
	registerSessionActions(r)
	registerQueueActions(r)
	registerAuthActions(r)
	registerDecisionActions(r)
	registerCoordinationActions(r)
	registerServiceActions(r)
	registerStorageActions(r)
	registerObservabilityActions(r)
	registerFlowActions(r)
	registerProcessActions(r)
}

// registerCoreActions installs the handful of primitives every application uses
// regardless of domain: publish a constant, gather facts, allow, deny, evaluate
// an expression.
func registerCoreActions(r *Registry) {
	mustAction(r, "constant", ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
		value, ok := spec.Config["value"]
		if !ok {
			return nil, fmt.Errorf("node %q: constant action requires config.value", spec.Name)
		}
		if err := exactlyOneOutput(spec); err != nil {
			return nil, err
		}
		provides := spec.Provides[0]
		return ActionFunc(func(*ActionContext) (ActionResult, error) {
			return ActionResult{Outputs: map[string]any{provides: value}}, nil
		}), nil
	}), ActionInfo{
		Family:  "compute",
		Summary: "Publish a fixed configured value as a fact",
		Config:  []ConfigField{{Name: "value", Type: "any", Required: true, Summary: "The value to publish"}},
		Kind:    "pure",
	})

	mustAction(r, "collect", ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
		if err := exactlyOneOutput(spec); err != nil {
			return nil, err
		}
		provides := spec.Provides[0]
		// A single required fact collects to that fact's own value rather than a
		// one-key wrapper. It is what an author means by "the response is this",
		// and it keeps response shapes from growing an accidental envelope.
		unwrap := len(spec.Requires) == 1 && configBool(spec.Config, "unwrap", false)
		only := ""
		if unwrap {
			only = spec.Requires[0]
		}
		return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
			if unwrap {
				return ActionResult{Outputs: map[string]any{provides: ctx.Inputs[only]}}, nil
			}
			return ActionResult{Outputs: map[string]any{provides: maps.Clone(ctx.Inputs)}}, nil
		}), nil
	}), ActionInfo{
		Family:  "compute",
		Summary: "Gather the node's required facts into one object",
		Config: []ConfigField{
			{Name: "unwrap", Type: "bool", Summary: "With exactly one required fact, publish its value directly", Default: "false"},
		},
		Kind: "pure",
	})

	mustAction(r, "allow", decisionAction(true), ActionInfo{
		Family:  "decision",
		Summary: "Record an unconditional policy allow",
		Config:  []ConfigField{{Name: "message", Type: "string"}},
		Kind:    "decision",
	})
	mustAction(r, "deny", decisionAction(false), ActionInfo{
		Family:  "decision",
		Summary: "Record an unconditional policy deny, blocking every effect in the plan",
		Config:  []ConfigField{{Name: "message", Type: "string"}},
		Kind:    "decision",
	})

	mustAction(r, "expression", ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
		raw, _ := spec.Config["expression"].(string)
		if raw == "" {
			return nil, fmt.Errorf("node %q: expression action requires config.expression", spec.Name)
		}
		if err := exactlyOneOutput(spec); err != nil {
			return nil, err
		}
		expr, err := CompileExpr(raw)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", spec.Name, err)
		}
		provides := spec.Provides[0]
		return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
			value, err := expr.Eval(actionEnv(ctx))
			if err != nil {
				return ActionResult{}, err
			}
			return ActionResult{Outputs: map[string]any{provides: value}}, nil
		}), nil
	}), ActionInfo{
		Family:  "compute",
		Summary: "Evaluate an expression over the node's facts and publish the result",
		Config:  []ConfigField{{Name: "expression", Type: "expression", Required: true}},
		Kind:    "pure",
	})
}

func decisionAction(allow bool) ActionFactory {
	return ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
		message, _ := spec.Config["message"].(string)
		return ActionFunc(func(*ActionContext) (ActionResult, error) {
			return ActionResult{Decision: &Decision{Allow: allow, Message: message}}, nil
		}), nil
	})
}

// mustAction registers a built-in action, panicking on a duplicate name. A
// duplicate here is a programming error in this package, discovered the first
// time any test constructs a Registry — never something a deployment can hit.
func mustAction(r *Registry, name string, factory ActionFactory, info ...ActionInfo) {
	if err := r.RegisterAction(name, factory, info...); err != nil {
		panic("ref/platform: built-in action: " + err.Error())
	}
}

// mustResource registers a built-in resource provider, panicking on a duplicate
// kind for the same reason as mustAction.
func mustResource(r *Registry, kind string, factory ResourceFactory, info ...ResourceKindInfo) {
	if err := r.RegisterResource(kind, factory, info...); err != nil {
		panic("ref/platform: built-in resource: " + err.Error())
	}
}
