package platform

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Data-shaping and validation actions.
//
// These are the actions that make a graph readable. Without them an author
// reaches for an expression node every time they need the first row of a result
// or a subset of fields, and the graph fills up with one-line expressions whose
// purpose is not obvious from their name. `data.first` says what it does.

func registerDataActions(r *Registry) {
	mustAction(r, "data.transform", dataTransformAction, ActionInfo{
		Family:   "data",
		Summary:  "Run a data pipeline over a value: extract, set, rename, coerce, redact, validate",
		Provides: "The reshaped value",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Summary: "What to reshape; omitted means every required fact"},
			{Name: "data", Type: "block", Required: true, Summary: "A data block, with the same keys as input_data and output_data"},
		},
	})

	mustAction(r, "data.get", dataGetAction, ActionInfo{
		Family:   "data",
		Summary:  "Publish the value at one dotted path",
		Provides: "The value at the path",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "path", Type: "fact", Required: true},
			{Name: "required", Type: "bool", Summary: "Fail when the path is absent, instead of publishing null"},
			{Name: "default", Type: "any"},
			{Name: "message", Type: "string", Summary: "Caller-facing message when a required path is missing"},
		},
	})

	mustAction(r, "data.first", dataFirstAction, ActionInfo{
		Family:   "data",
		Summary:  "Publish the first element of a list, failing when it is empty",
		Provides: "The first element",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "message", Type: "string", Summary: "Caller-facing not-found message"},
			{Name: "optional", Type: "bool", Summary: "Publish null instead of failing on an empty list"},
		},
	})

	mustAction(r, "data.count", dataCountAction, ActionInfo{
		Family:   "data",
		Summary:  "Publish the length of a list, object or string",
		Provides: "An integer",
		Kind:     "pure",
		Config:   []ConfigField{{Name: "source_fact", Type: "fact", Required: true}},
	})

	mustAction(r, "data.merge", dataMergeAction, ActionInfo{
		Family:   "data",
		Summary:  "Merge several facts into one object, later facts winning",
		Provides: "The merged object",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "sources", Type: "[]fact", Summary: "Facts to merge in order; omitted means every required fact"},
			{Name: "deep", Type: "bool", Default: "false", Summary: "Merge nested objects rather than replacing them"},
		},
	})

	mustAction(r, "data.pick", dataPickAction, ActionInfo{
		Family:   "data",
		Summary:  "Publish only the named paths of a value",
		Provides: "The narrowed object",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "paths", Type: "[]string", Required: true},
		},
	})

	mustAction(r, "data.map", dataMapAction, ActionInfo{
		Family:   "data",
		Summary:  "Evaluate an expression for every element of a list",
		Provides: "A list of results",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "expression", Type: "expression", Required: true, Summary: "Sees the element as `item` and its position as `index`"},
		},
	})

	mustAction(r, "data.filter", dataFilterAction, ActionInfo{
		Family:   "data",
		Summary:  "Keep the list elements for which an expression is true",
		Provides: "The filtered list",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "expression", Type: "expression", Required: true, Summary: "Sees the element as `item`"},
		},
	})

	mustAction(r, "data.sort", dataSortAction, ActionInfo{
		Family:   "data",
		Summary:  "Sort a list by a field or an expression",
		Provides: "The sorted list",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "by", Type: "string", Summary: "Field path within each element"},
			{Name: "expression", Type: "expression", Summary: "Sort key expression, taking precedence over by"},
			{Name: "descending", Type: "bool"},
			{Name: "numeric", Type: "bool", Summary: "Compare as numbers rather than text"},
		},
	})

	mustAction(r, "data.group", dataGroupAction, ActionInfo{
		Family:   "data",
		Summary:  "Group a list into an object keyed by a field or expression",
		Provides: "An object of lists",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "by", Type: "string"},
			{Name: "expression", Type: "expression"},
		},
	})

	mustAction(r, "data.aggregate", dataAggregateAction, ActionInfo{
		Family:   "data",
		Summary:  "Reduce a list to sum, min, max, avg and count of one field",
		Provides: "An object of aggregates",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "value_field", Type: "string", Summary: "Numeric field within each element. Spelled value_field because BCL reserves field."},
		},
	})

	mustAction(r, "data.template", dataTemplateAction, ActionInfo{
		Family:   "data",
		Summary:  "Render a text template over the node's facts",
		Provides: "The rendered string",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "template", Type: "template", Required: true},
		},
	})

	mustAction(r, "data.json_encode", dataJSONEncodeAction, ActionInfo{
		Family:   "data",
		Summary:  "Serialise a value to JSON text",
		Provides: "A JSON string",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "indent", Type: "bool"},
		},
	})

	mustAction(r, "data.json_decode", dataJSONDecodeAction, ActionInfo{
		Family:   "data",
		Summary:  "Parse JSON text into a value",
		Provides: "The decoded value",
		Kind:     "pure",
		Config:   []ConfigField{{Name: "source_fact", Type: "fact", Required: true}},
	})

	mustAction(r, "request.param", requestParamAction, ActionInfo{
		Family:   "data",
		Summary:  "Read a path parameter, query parameter or header from the request",
		Provides: "The parameter value",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "name", Type: "string", Required: true},
			{Name: "from", Type: "string", Default: "path", Summary: "path, query or header"},
			{Name: "required", Type: "bool", Default: "true"},
			{Name: "default", Type: "any"},
		},
	})

	mustAction(r, "validate.required", validateRequiredAction, ActionInfo{
		Family:  "validate",
		Summary: "Fail unless every listed path is present and non-empty",
		Kind:    "pure",
		Config: []ConfigField{
			{Name: "fields", Type: "[]fact", Required: true},
		},
	})

	mustAction(r, "validate.schema", validateSchemaAction, ActionInfo{
		Family:   "validate",
		Summary:  "Validate a value against a schema block, applying declared defaults",
		Provides: "The validated value",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "shape", Type: "string", Required: true, Summary: "Name of a shape block. Spelled shape because BCL reserves schema."},
			{Name: "source_fact", Type: "fact", Default: "input"},
		},
	})

	mustAction(r, "validate.expression", validateExpressionAction, ActionInfo{
		Family:  "validate",
		Summary: "Fail unless an expression is true",
		Kind:    "pure",
		Config: []ConfigField{
			{Name: "expression", Type: "expression", Required: true},
			{Name: "message", Type: "string", Summary: "Caller-facing message when the check fails"},
		},
	})
}

// sourceValue reads the action's subject: the fact named by source_fact, or the
// whole fact map when no source is configured.
func sourceValue(ctx *ActionContext, path string) (any, bool) {
	if path == "" {
		return ctx.Inputs, true
	}
	return resolvePath(ctx.Inputs, path)
}

// requiredList reads a list-shaped value, accepting the []map[string]any that
// database queries produce as well as a plain []any.
func requiredList(value any, path string) ([]any, error) {
	switch typed := value.(type) {
	case []any:
		return typed, nil
	case []map[string]any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = item
		}
		return out, nil
	case nil:
		return nil, nil
	default:
		return nil, invalidInput("%q is not a list (it is %T)", path, value)
	}
}

var dataTransformAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	raw := configMap(spec.Config, "data")
	if len(raw) == 0 {
		return nil, fmt.Errorf("node %q: data.transform needs a config.data block", spec.Name)
	}
	dataSpec, err := decodeDataSpec(raw)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	pipeline, err := compileDataSpec("node "+spec.Name, dataSpec, build.Schemas)
	if err != nil {
		return nil, err
	}
	sourceFact := configString(spec.Config, "source_fact", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := sourceValue(ctx, sourceFact)
		if !found {
			return ActionResult{}, invalidInput("no value at %q to transform", sourceFact)
		}
		shaped, err := pipeline.Apply(value, actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, shaped), nil
	}), nil
})

var dataGetAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	path, err := requiredString(spec.Config, "path")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	required := configBool(spec.Config, "required", false)
	fallback := spec.Config["default"]
	message := configString(spec.Config, "message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := resolvePath(ctx.Inputs, path)
		if !found || value == nil {
			if fallback != nil {
				return singleOutput(spec, fallback), nil
			}
			if required {
				if message != "" {
					return ActionResult{}, invalidInput("%s", message)
				}
				return ActionResult{}, invalidInput("%s is required", path)
			}
			return singleOutput(spec, nil), nil
		}
		return singleOutput(spec, value), nil
	}), nil
})

var dataFirstAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	message := configString(spec.Config, "message", "")
	optional := configBool(spec.Config, "optional", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, sourceFact)
		items, err := requiredList(value, sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		if len(items) == 0 {
			if optional {
				return singleOutput(spec, nil), nil
			}
			return ActionResult{}, notFoundOrMessage(message)
		}
		return singleOutput(spec, items[0]), nil
	}), nil
})

var dataCountAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, sourceFact)
		switch typed := value.(type) {
		case []any:
			return singleOutput(spec, len(typed)), nil
		case []map[string]any:
			return singleOutput(spec, len(typed)), nil
		case map[string]any:
			return singleOutput(spec, len(typed)), nil
		case string:
			return singleOutput(spec, len(typed)), nil
		case nil:
			return singleOutput(spec, 0), nil
		default:
			return ActionResult{}, invalidInput("%q cannot be counted (it is %T)", sourceFact, value)
		}
	}), nil
})

var dataMergeAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sources := configStrings(spec.Config, "sources")
	if len(sources) == 0 {
		sources = spec.Requires
	}
	deep := configBool(spec.Config, "deep", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		merged := map[string]any{}
		for _, source := range sources {
			value, found := resolvePath(ctx.Inputs, source)
			if !found {
				continue
			}
			object, ok := value.(map[string]any)
			if !ok {
				return ActionResult{}, invalidInput("%q is not an object, so it cannot be merged", source)
			}
			mergeInto(merged, object, deep)
		}
		return singleOutput(spec, merged), nil
	}), nil
})

// mergeInto merges src into dst. A deep merge recurses into objects present on
// both sides; a shallow one replaces. Lists always replace — appending would be a
// surprising interpretation of "merge".
func mergeInto(dst, src map[string]any, deep bool) {
	for key, value := range src {
		if deep {
			if existing, ok := dst[key].(map[string]any); ok {
				if nested, ok := value.(map[string]any); ok {
					mergeInto(existing, nested, true)
					continue
				}
			}
		}
		dst[key] = value
	}
}

var dataPickAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	paths := configStrings(spec.Config, "paths")
	if len(paths) == 0 {
		return nil, fmt.Errorf("node %q: data.pick needs config.paths", spec.Name)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, sourceFact)
		object, ok := value.(map[string]any)
		if !ok {
			return ActionResult{}, invalidInput("%q is not an object", sourceFact)
		}
		picked := map[string]any{}
		for _, path := range paths {
			segments := strings.Split(path, ".")
			if item, found := lookupPath(object, segments); found {
				setPath(picked, segments, item)
			}
		}
		return singleOutput(spec, picked), nil
	}), nil
})

// elementExpr compiles the shape data.map, data.filter, data.sort and data.group
// share: an expression evaluated once per element with `item` and `index` bound.
type elementExpr struct {
	sourceFact string
	expr       *Expression
}

func compileElementExpr(spec NodeSpec, required bool) (*elementExpr, error) {
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	expr, err := configExpr(spec.Config, "expression")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if required && expr == nil {
		return nil, fmt.Errorf("node %q: %s needs config.expression", spec.Name, spec.Uses)
	}
	return &elementExpr{sourceFact: sourceFact, expr: expr}, nil
}

// evalPer evaluates the expression for one element. The environment is rebuilt
// per element rather than mutated, because an expression may capture nothing but
// a stale `item` is the kind of bug that only shows up under concurrency.
func (e *elementExpr) evalPer(ctx *ActionContext, base Env, item any, index int) (any, error) {
	env := make(Env, len(base)+2)
	for key, value := range base {
		env[key] = value
	}
	env["item"] = item
	env["index"] = index
	return e.expr.Eval(env)
}

var dataMapAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	compiled, err := compileElementExpr(spec, true)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, compiled.sourceFact)
		items, err := requiredList(value, compiled.sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		base := actionEnv(ctx)
		out := make([]any, 0, len(items))
		for index, item := range items {
			mapped, err := compiled.evalPer(ctx, base, item, index)
			if err != nil {
				return ActionResult{}, err
			}
			out = append(out, mapped)
		}
		return singleOutput(spec, out), nil
	}), nil
})

var dataFilterAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	compiled, err := compileElementExpr(spec, true)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, compiled.sourceFact)
		items, err := requiredList(value, compiled.sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		base := actionEnv(ctx)
		out := make([]any, 0, len(items))
		for index, item := range items {
			keep, err := compiled.evalPer(ctx, base, item, index)
			if err != nil {
				return ActionResult{}, err
			}
			if Truthy(keep) {
				out = append(out, item)
			}
		}
		return singleOutput(spec, out), nil
	}), nil
})

var dataSortAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	compiled, err := compileElementExpr(spec, false)
	if err != nil {
		return nil, err
	}
	by := configString(spec.Config, "by", "")
	if compiled.expr == nil && by == "" {
		return nil, fmt.Errorf("node %q: data.sort needs config.by or config.expression", spec.Name)
	}
	descending := configBool(spec.Config, "descending", false)
	numeric := configBool(spec.Config, "numeric", false)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, compiled.sourceFact)
		items, err := requiredList(value, compiled.sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		base := actionEnv(ctx)
		keys := make([]any, len(items))
		for index, item := range items {
			if compiled.expr != nil {
				if keys[index], err = compiled.evalPer(ctx, base, item, index); err != nil {
					return ActionResult{}, err
				}
				continue
			}
			if object, ok := item.(map[string]any); ok {
				keys[index], _ = lookupPath(object, strings.Split(by, "."))
			}
		}
		order := make([]int, len(items))
		for i := range order {
			order[i] = i
		}
		// A stable sort on an explicit index permutation keeps equal elements in
		// their original order, which makes paginated output deterministic.
		sort.SliceStable(order, func(a, b int) bool {
			less := compareValues(keys[order[a]], keys[order[b]], numeric)
			if descending {
				return !less && compareValues(keys[order[b]], keys[order[a]], numeric)
			}
			return less
		})
		out := make([]any, len(items))
		for position, index := range order {
			out[position] = items[index]
		}
		return singleOutput(spec, out), nil
	}), nil
})

func compareValues(a, b any, numeric bool) bool {
	if numeric {
		left, leftOK := ToFloat(a)
		right, rightOK := ToFloat(b)
		if leftOK && rightOK {
			return left < right
		}
	}
	return Stringify(a) < Stringify(b)
}

var dataGroupAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	compiled, err := compileElementExpr(spec, false)
	if err != nil {
		return nil, err
	}
	by := configString(spec.Config, "by", "")
	if compiled.expr == nil && by == "" {
		return nil, fmt.Errorf("node %q: data.group needs config.by or config.expression", spec.Name)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, compiled.sourceFact)
		items, err := requiredList(value, compiled.sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		base := actionEnv(ctx)
		grouped := map[string]any{}
		for index, item := range items {
			var key any
			if compiled.expr != nil {
				if key, err = compiled.evalPer(ctx, base, item, index); err != nil {
					return ActionResult{}, err
				}
			} else if object, ok := item.(map[string]any); ok {
				key, _ = lookupPath(object, strings.Split(by, "."))
			}
			name := Stringify(key)
			bucket, _ := grouped[name].([]any)
			grouped[name] = append(bucket, item)
		}
		return singleOutput(spec, grouped), nil
	}), nil
})

var dataAggregateAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	field := configString(spec.Config, "value_field", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, sourceFact)
		items, err := requiredList(value, sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		result := map[string]any{"count": len(items)}
		if field == "" || len(items) == 0 {
			return singleOutput(spec, result), nil
		}
		var (
			sum      float64
			min, max float64
			counted  int
		)
		for _, item := range items {
			var raw any = item
			if object, ok := item.(map[string]any); ok {
				raw, _ = lookupPath(object, strings.Split(field, "."))
			}
			number, ok := ToFloat(raw)
			if !ok {
				continue
			}
			if counted == 0 || number < min {
				min = number
			}
			if counted == 0 || number > max {
				max = number
			}
			sum += number
			counted++
		}
		result["numeric_count"] = counted
		if counted > 0 {
			result["sum"], result["min"], result["max"] = sum, min, max
			result["avg"] = sum / float64(counted)
		}
		return singleOutput(spec, result), nil
	}), nil
})

var dataTemplateAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	tmpl, err := configTemplate(spec.Config, "template", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if tmpl == nil {
		return nil, fmt.Errorf("node %q: data.template needs config.template", spec.Name)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		rendered, err := tmpl.Render(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, rendered), nil
	}), nil
})

var dataJSONEncodeAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	indent := configBool(spec.Config, "indent", false)
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, sourceFact)
		var (
			encoded []byte
			err     error
		)
		if indent {
			encoded, err = json.MarshalIndent(value, "", "  ")
		} else {
			encoded, err = json.Marshal(value)
		}
		if err != nil {
			return ActionResult{}, invalidInput("%q cannot be serialised: %v", sourceFact, err)
		}
		return singleOutput(spec, string(encoded)), nil
	}), nil
})

var dataJSONDecodeAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		raw, _ := resolvePath(ctx.Inputs, sourceFact)
		var decoded any
		if err := json.Unmarshal([]byte(Stringify(raw)), &decoded); err != nil {
			return ActionResult{}, invalidInput("%q is not valid JSON: %v", sourceFact, err)
		}
		return singleOutput(spec, decoded), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Request access
// ---------------------------------------------------------------------------

var requestParamAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	name, err := requiredString(spec.Config, "name")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	from := strings.ToLower(configString(spec.Config, "from", "path"))
	if !slices.Contains([]string{"path", "query", "header"}, from) {
		return nil, fmt.Errorf("node %q: from must be path, query or header", spec.Name)
	}
	required := configBool(spec.Config, "required", true)
	fallback := spec.Config["default"]

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := requestValue(ctx, from, name)
		if !found || value == "" {
			if fallback != nil {
				return singleOutput(spec, fallback), nil
			}
			if required {
				return ActionResult{}, invalidInput("the %s parameter %q is required", from, name)
			}
			return singleOutput(spec, nil), nil
		}
		return singleOutput(spec, value), nil
	}), nil
})

var validateRequiredAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	fields := configStrings(spec.Config, "fields")
	if len(fields) == 0 {
		return nil, fmt.Errorf("node %q: validate.required needs config.fields", spec.Name)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		// Every missing field is reported, not just the first: a caller fixing a
		// form wants the whole list rather than one round trip per field.
		var missing []string
		for _, path := range fields {
			value, found := resolvePath(ctx.Inputs, path)
			if !found || value == nil || Stringify(value) == "" {
				missing = append(missing, strings.TrimPrefix(path, "input."))
			}
		}
		if len(missing) > 0 {
			return ActionResult{}, invalidInput("these fields are required: %s", strings.Join(missing, ", "))
		}
		return acknowledgement(spec, true), nil
	}), nil
})

var validateSchemaAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	name, err := requiredString(spec.Config, "shape")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	schema, ok := build.Schemas[name]
	if !ok {
		return nil, fmt.Errorf("node %q: shape %q is not declared", spec.Name, name)
	}
	sourceFact := configString(spec.Config, "source_fact", "input")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, found := resolvePath(ctx.Inputs, sourceFact)
		if !found {
			return ActionResult{}, invalidInput("no value at %q to validate", sourceFact)
		}
		validated, err := schema.Validate(value)
		if err != nil {
			return ActionResult{}, err
		}
		return acknowledgement(spec, validated), nil
	}), nil
})

var validateExpressionAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	expr, err := requiredExpr(spec.Config, "expression")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	message := configString(spec.Config, "message", "the request did not pass validation")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		ok, err := expr.Bool(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		if !ok {
			return ActionResult{}, invalidInput("%s", message)
		}
		return acknowledgement(spec, true), nil
	}), nil
})
