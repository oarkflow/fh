package platform

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/intent"
)

// ErrDataFiltered reports that a DataSpec's filter rejected the payload. On an
// intent boundary the platform converts it to an invalid-input failure; on a
// process edge it means the edge does not traverse, which is what a filter edge
// is for. Keeping it a distinct sentinel is what lets one pipeline serve both.
var ErrDataFiltered = errors.New("ref/platform: payload filtered")

// DataPipeline is a compiled DataSpec. Every expression, template and regular
// expression inside it is compiled once at load time; Apply does no parsing.
//
// The stage order is fixed and documented on DataSpec. It is implemented here
// in exactly that order, and each stage is skipped entirely when its spec is
// empty — a pipeline that only redacts two fields does two map deletes, not
// fourteen passes.
type DataPipeline struct {
	name string

	source     []string
	extract    map[string]*Expression
	extractKey []string
	set        map[string]*Template
	setRaw     map[string]any
	setKey     []string
	defaults   map[string]any

	transforms []compiledTransform
	filters    []compiledFilter

	appends  map[string]*Template
	prepends map[string]*Template

	rename  map[string]string
	coerce  map[string]string
	flatten []string
	pick    []string
	omit    []string
	redact  []string
	mask    []string

	maskKeep int
	maxBytes int64
	maxDepth int
	strict   bool

	schema *CompiledSchema
}

type compiledTransform struct {
	path string
	expr *Expression
	op   string
	arg  string
	when *Expression
}

type compiledFilter struct {
	expr   *Expression
	deny   bool
	reason string
}

// compileDataSpec turns a spec into a pipeline, resolving schema references
// against the application's compiled schemas. A nil or empty spec compiles to
// nil, and every caller treats a nil pipeline as identity.
func compileDataSpec(name string, spec *DataSpec, schemas map[string]*CompiledSchema) (*DataPipeline, error) {
	if spec.Empty() {
		return nil, nil
	}
	pipeline := &DataPipeline{
		name:     name,
		defaults: spec.Defaults,
		rename:   spec.Rename,
		coerce:   lowerValues(spec.Coerce),
		flatten:  spec.Flatten,
		pick:     spec.Pick,
		omit:     spec.Omit,
		redact:   spec.Redact,
		mask:     spec.Mask,
		maskKeep: spec.MaskKeep,
		maxBytes: spec.MaxBytes,
		maxDepth: spec.MaxDepth,
		strict:   spec.Strict,
	}
	if pipeline.maskKeep <= 0 {
		pipeline.maskKeep = 4
	}
	if spec.Source != "" {
		pipeline.source = strings.Split(spec.Source, ".")
	}
	if len(spec.Extract) > 0 {
		pipeline.extract = make(map[string]*Expression, len(spec.Extract))
		for target, source := range spec.Extract {
			expr, err := CompileExpr(source)
			if err != nil {
				return nil, fmt.Errorf("%s extract %q: %w", name, target, err)
			}
			pipeline.extract[target] = expr
			pipeline.extractKey = append(pipeline.extractKey, target)
		}
		// Sorted so a pipeline that extracts both "a" and "a.b" applies them in
		// a stable order rather than whichever the map iteration produced.
		sort.Strings(pipeline.extractKey)
	}
	if len(spec.Set) > 0 {
		pipeline.set = make(map[string]*Template)
		pipeline.setRaw = make(map[string]any)
		for target, value := range spec.Set {
			text, isText := value.(string)
			if !isText {
				pipeline.setRaw[target] = value
				pipeline.setKey = append(pipeline.setKey, target)
				continue
			}
			tmpl, err := CompileTemplate(text)
			if err != nil {
				return nil, fmt.Errorf("%s set %q: %w", name, target, err)
			}
			if tmpl.HasHoles() {
				pipeline.set[target] = tmpl
			} else {
				pipeline.setRaw[target] = text
			}
			pipeline.setKey = append(pipeline.setKey, target)
		}
		sort.Strings(pipeline.setKey)
	}
	var err error
	if pipeline.appends, err = compileTemplateMap(name, "append", spec.Append); err != nil {
		return nil, err
	}
	if pipeline.prepends, err = compileTemplateMap(name, "prepend", spec.Prepend); err != nil {
		return nil, err
	}
	for i, transform := range spec.Transforms {
		compiled := compiledTransform{path: transform.Path, op: strings.ToLower(strings.TrimSpace(transform.Op)), arg: transform.Arg}
		if transform.Path == "" {
			return nil, fmt.Errorf("%s transform[%d]: path is required", name, i)
		}
		if transform.Expr == "" && compiled.op == "" {
			return nil, fmt.Errorf("%s transform[%d] (%s): either expr or op is required", name, i, transform.Path)
		}
		if compiled.expr, err = CompileExpr(transform.Expr); err != nil {
			return nil, fmt.Errorf("%s transform[%d]: %w", name, i, err)
		}
		if compiled.when, err = CompileExpr(transform.OnlyIf); err != nil {
			return nil, fmt.Errorf("%s transform[%d] only_if: %w", name, i, err)
		}
		if compiled.op != "" && !knownTransformOps[compiled.op] {
			return nil, fmt.Errorf("%s transform[%d]: unknown op %q", name, i, compiled.op)
		}
		pipeline.transforms = append(pipeline.transforms, compiled)
	}
	for i, filter := range spec.Filters {
		expr, err := CompileExpr(filter.Expr)
		if err != nil {
			return nil, fmt.Errorf("%s filter[%d]: %w", name, i, err)
		}
		if expr == nil {
			return nil, fmt.Errorf("%s filter[%d]: expr is required", name, i)
		}
		mode := strings.ToLower(strings.TrimSpace(filter.Mode))
		if mode != "" && mode != "allow" && mode != "deny" {
			return nil, fmt.Errorf("%s filter[%d]: mode must be allow or deny", name, i)
		}
		pipeline.filters = append(pipeline.filters, compiledFilter{expr: expr, deny: mode == "deny", reason: filter.Reason})
	}
	if spec.Schema != "" {
		schema, ok := schemas[spec.Schema]
		if !ok {
			return nil, fmt.Errorf("%s references unknown schema %q", name, spec.Schema)
		}
		pipeline.schema = schema
	}
	return pipeline, nil
}

func compileTemplateMap(name, stage string, raw map[string]string) (map[string]*Template, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]*Template, len(raw))
	for target, value := range raw {
		tmpl, err := CompileTemplate(value)
		if err != nil {
			return nil, fmt.Errorf("%s %s %q: %w", name, stage, target, err)
		}
		out[target] = tmpl
	}
	return out, nil
}

// Apply runs the pipeline over value with env as the expression environment.
// A nil pipeline returns the value untouched.
//
// Apply never mutates its input: an intent's facts are shared with other nodes
// that may already have read them, and a pipeline that rewrote them in place
// would make a graph's result depend on node scheduling order.
func (p *DataPipeline) Apply(value any, env Env) (any, error) {
	if p == nil {
		return value, nil
	}
	current := value
	if len(p.source) > 0 {
		narrowed, found := lookupPath(current, p.source)
		if !found && p.strict {
			return nil, p.fail("source path %q is missing", strings.Join(p.source, "."))
		}
		current = narrowed
	}

	object, isObject := cloneValue(current).(map[string]any)
	if !isObject {
		// A non-object payload only supports the stages that make sense for
		// one: filters, limits and a whole-value transform on path ".".
		return p.applyScalar(current, env)
	}

	if env == nil {
		env = Env{}
	}
	// A pipeline's own expressions see the payload under both "result" and at
	// the top level, so "amount" and "result.amount" both work. Authors reach
	// for the short form; the long form disambiguates when a payload key
	// collides with an environment key such as "input".
	local := make(Env, len(env)+len(object)+1)
	maps.Copy(local, env)
	refresh := func() {
		for key, item := range object {
			local[key] = item
		}
		local["result"] = object
	}
	refresh()

	for key, item := range p.defaults {
		if existing, present := object[key]; !present || existing == nil {
			object[key] = cloneValue(item)
		}
	}
	if len(p.defaults) > 0 {
		refresh()
	}

	for _, target := range p.extractKey {
		result, err := p.extract[target].Eval(local)
		if err != nil {
			return nil, err
		}
		if result == nil && p.strict {
			return nil, p.fail("extract %q produced no value", target)
		}
		setPath(object, strings.Split(target, "."), result)
	}
	if len(p.extractKey) > 0 {
		refresh()
	}

	for _, target := range p.setKey {
		if tmpl, ok := p.set[target]; ok {
			rendered, err := tmpl.Render(local)
			if err != nil {
				return nil, err
			}
			setPath(object, strings.Split(target, "."), rendered)
			continue
		}
		setPath(object, strings.Split(target, "."), cloneValue(p.setRaw[target]))
	}
	if len(p.setKey) > 0 {
		refresh()
	}

	for _, transform := range p.transforms {
		ok, err := transform.when.Bool(local)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		path := strings.Split(transform.path, ".")
		existing, found := lookupPath(object, path)
		if !found && p.strict {
			return nil, p.fail("transform path %q is missing", transform.path)
		}
		var next any
		if transform.expr != nil {
			local["value"] = existing
			if next, err = transform.expr.Eval(local); err != nil {
				return nil, err
			}
			delete(local, "value")
		} else {
			next = existing
		}
		if transform.op != "" {
			if next, err = applyTransformOp(transform.op, transform.arg, next); err != nil {
				return nil, p.fail("transform %q: %v", transform.path, err)
			}
		}
		setPath(object, path, next)
		refresh()
	}

	for target, tmpl := range p.appends {
		if err := p.concat(object, local, target, tmpl, false); err != nil {
			return nil, err
		}
	}
	for target, tmpl := range p.prepends {
		if err := p.concat(object, local, target, tmpl, true); err != nil {
			return nil, err
		}
	}

	for from, to := range p.rename {
		fromPath := strings.Split(from, ".")
		value, found := lookupPath(object, fromPath)
		if !found {
			if p.strict {
				return nil, p.fail("rename source %q is missing", from)
			}
			continue
		}
		deletePath(object, fromPath)
		setPath(object, strings.Split(to, "."), value)
	}

	for path, target := range p.coerce {
		segments := strings.Split(path, ".")
		value, found := lookupPath(object, segments)
		if !found {
			if p.strict {
				return nil, p.fail("coerce path %q is missing", path)
			}
			continue
		}
		converted, err := coerceValue(value, target)
		if err != nil {
			return nil, p.fail("coerce %q: %v", path, err)
		}
		setPath(object, segments, converted)
	}

	for _, path := range p.flatten {
		segments := strings.Split(path, ".")
		value, found := lookupPath(object, segments)
		if !found {
			continue
		}
		nested, ok := value.(map[string]any)
		if !ok {
			if p.strict {
				return nil, p.fail("flatten path %q is not an object", path)
			}
			continue
		}
		deletePath(object, segments)
		for key, item := range nested {
			if _, exists := object[key]; exists && p.strict {
				return nil, p.fail("flatten %q would overwrite existing key %q", path, key)
			}
			object[key] = item
		}
	}

	if len(p.pick) > 0 {
		picked := make(map[string]any, len(p.pick))
		for _, path := range p.pick {
			segments := strings.Split(path, ".")
			value, found := lookupPath(object, segments)
			if !found {
				if p.strict {
					return nil, p.fail("pick path %q is missing", path)
				}
				continue
			}
			setPath(picked, segments, value)
		}
		object = picked
	}
	for _, path := range p.omit {
		deletePath(object, strings.Split(path, "."))
	}
	for _, path := range p.redact {
		deletePath(object, strings.Split(path, "."))
	}
	for _, path := range p.mask {
		segments := strings.Split(path, ".")
		if value, found := lookupPath(object, segments); found {
			setPath(object, segments, maskValue(Stringify(value), p.maskKeep))
		}
	}

	refresh()
	if err := p.runFilters(local); err != nil {
		return nil, err
	}
	if err := p.enforceLimits(object); err != nil {
		return nil, err
	}
	if p.schema != nil {
		validated, err := p.schema.Validate(object)
		if err != nil {
			return nil, err
		}
		return validated, nil
	}
	return object, nil
}

func (p *DataPipeline) applyScalar(value any, env Env) (any, error) {
	local := make(Env, len(env)+2)
	maps.Copy(local, env)
	local["result"] = value
	local["value"] = value
	for _, transform := range p.transforms {
		if transform.path != "" && transform.path != "." {
			continue
		}
		ok, err := transform.when.Bool(local)
		if err != nil || !ok {
			if err != nil {
				return nil, err
			}
			continue
		}
		next := value
		if transform.expr != nil {
			if next, err = transform.expr.Eval(local); err != nil {
				return nil, err
			}
		}
		if transform.op != "" {
			if next, err = applyTransformOp(transform.op, transform.arg, next); err != nil {
				return nil, p.fail("transform: %v", err)
			}
		}
		value = next
		local["result"], local["value"] = value, value
	}
	if err := p.runFilters(local); err != nil {
		return nil, err
	}
	if err := p.enforceLimits(value); err != nil {
		return nil, err
	}
	return value, nil
}

func (p *DataPipeline) concat(object map[string]any, env Env, target string, tmpl *Template, prepend bool) error {
	rendered, err := tmpl.Render(env)
	if err != nil {
		return err
	}
	segments := strings.Split(target, ".")
	existing, _ := lookupPath(object, segments)
	switch typed := existing.(type) {
	case nil:
		setPath(object, segments, rendered)
	case []any:
		if prepend {
			setPath(object, segments, append([]any{rendered}, typed...))
		} else {
			setPath(object, segments, append(slices.Clone(typed), rendered))
		}
	default:
		text := Stringify(existing)
		if prepend {
			setPath(object, segments, rendered+text)
		} else {
			setPath(object, segments, text+rendered)
		}
	}
	return nil
}

func (p *DataPipeline) runFilters(env Env) error {
	for _, filter := range p.filters {
		ok, err := filter.expr.Bool(env)
		if err != nil {
			return err
		}
		rejected := ok == filter.deny
		if rejected {
			continue
		}
		reason := filter.reason
		if reason == "" {
			reason = "payload rejected by filter"
		}
		return fmt.Errorf("%w: %s (%s)", ErrDataFiltered, reason, p.name)
	}
	return nil
}

func (p *DataPipeline) enforceLimits(value any) error {
	if p.maxBytes > 0 {
		encoded, err := json.Marshal(value)
		if err != nil {
			return p.fail("payload is not serialisable: %v", err)
		}
		if int64(len(encoded)) > p.maxBytes {
			return p.fail("payload is %d bytes, over the %d byte limit", len(encoded), p.maxBytes)
		}
	}
	if p.maxDepth > 0 && valueDepth(value, 0) > p.maxDepth {
		return p.fail("payload nests deeper than the %d level limit", p.maxDepth)
	}
	return nil
}

func (p *DataPipeline) fail(format string, args ...any) error {
	return intent.Failure{
		Code:     "INVALID_INPUT",
		Category: intent.CategoryInvalidInput,
		Message:  p.name + ": " + fmt.Sprintf(format, args...),
	}
}

// ---------------------------------------------------------------------------
// Transform operations
// ---------------------------------------------------------------------------

var knownTransformOps = map[string]bool{
	"upper": true, "lower": true, "trim": true, "title": true, "slug": true,
	"hash": true, "base64": true, "base64_decode": true, "hex": true,
	"json_encode": true, "json_decode": true, "round": true, "ceil": true,
	"floor": true, "abs": true, "length": true, "first": true, "last": true,
	"unique": true, "sort": true, "reverse": true, "join": true, "split": true,
	"now": true, "timestamp": true, "uuid": true, "default": true,
	"string": true, "int": true, "float": true, "bool": true,
}

func applyTransformOp(op, arg string, value any) (any, error) {
	switch op {
	case "upper":
		return strings.ToUpper(Stringify(value)), nil
	case "lower":
		return strings.ToLower(Stringify(value)), nil
	case "trim":
		if arg != "" {
			return strings.Trim(Stringify(value), arg), nil
		}
		return strings.TrimSpace(Stringify(value)), nil
	case "title":
		return titleCase(Stringify(value)), nil
	case "slug":
		return slugify(Stringify(value)), nil
	case "hash":
		sum := sha256.Sum256([]byte(Stringify(value)))
		return hex.EncodeToString(sum[:]), nil
	case "base64":
		return base64.StdEncoding.EncodeToString([]byte(Stringify(value))), nil
	case "base64_decode":
		decoded, err := base64.StdEncoding.DecodeString(Stringify(value))
		if err != nil {
			return nil, err
		}
		return string(decoded), nil
	case "hex":
		return hex.EncodeToString([]byte(Stringify(value))), nil
	case "json_encode":
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	case "json_decode":
		var decoded any
		if err := json.Unmarshal([]byte(Stringify(value)), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	case "round", "ceil", "floor", "abs":
		return numericOp(op, arg, value)
	case "length":
		switch typed := value.(type) {
		case string:
			return len(typed), nil
		case []any:
			return len(typed), nil
		case map[string]any:
			return len(typed), nil
		default:
			return 0, nil
		}
	case "first", "last":
		items, ok := value.([]any)
		if !ok || len(items) == 0 {
			return nil, nil
		}
		if op == "first" {
			return items[0], nil
		}
		return items[len(items)-1], nil
	case "unique":
		items, ok := value.([]any)
		if !ok {
			return value, nil
		}
		seen := make(map[string]struct{}, len(items))
		out := make([]any, 0, len(items))
		for _, item := range items {
			key := Stringify(item)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, item)
		}
		return out, nil
	case "sort", "reverse":
		items, ok := value.([]any)
		if !ok {
			return value, nil
		}
		out := slices.Clone(items)
		sort.SliceStable(out, func(i, j int) bool { return Stringify(out[i]) < Stringify(out[j]) })
		if op == "reverse" {
			slices.Reverse(out)
		}
		return out, nil
	case "join":
		items, ok := value.([]any)
		if !ok {
			return Stringify(value), nil
		}
		parts := make([]string, len(items))
		for i, item := range items {
			parts[i] = Stringify(item)
		}
		return strings.Join(parts, orDefault(arg, ",")), nil
	case "split":
		parts := strings.Split(Stringify(value), orDefault(arg, ","))
		out := make([]any, len(parts))
		for i, part := range parts {
			out[i] = part
		}
		return out, nil
	case "now":
		return time.Now().UTC().Format(time.RFC3339Nano), nil
	case "timestamp":
		return time.Now().UTC().Unix(), nil
	case "uuid":
		return newRandomID(), nil
	case "default":
		if value == nil || Stringify(value) == "" {
			return arg, nil
		}
		return value, nil
	case "string", "int", "float", "bool":
		return coerceValue(value, op)
	default:
		return nil, fmt.Errorf("unknown op %q", op)
	}
}

func numericOp(op, arg string, value any) (any, error) {
	number, ok := ToFloat(value)
	if !ok {
		return nil, fmt.Errorf("%s needs a number, got %T", op, value)
	}
	switch op {
	case "abs":
		if number < 0 {
			return -number, nil
		}
		return number, nil
	case "ceil":
		if number == float64(int64(number)) {
			return number, nil
		}
		if number > 0 {
			return float64(int64(number) + 1), nil
		}
		return float64(int64(number)), nil
	case "floor":
		if number == float64(int64(number)) {
			return number, nil
		}
		if number > 0 {
			return float64(int64(number)), nil
		}
		return float64(int64(number) - 1), nil
	default: // round
		places := 0
		if arg != "" {
			parsed, err := strconv.Atoi(arg)
			if err != nil {
				return nil, fmt.Errorf("round arg must be a digit count: %w", err)
			}
			places = parsed
		}
		shift := 1.0
		for range places {
			shift *= 10
		}
		scaled := number * shift
		if scaled >= 0 {
			scaled = float64(int64(scaled + 0.5))
		} else {
			scaled = float64(int64(scaled - 0.5))
		}
		return scaled / shift, nil
	}
}

func coerceValue(value any, target string) (any, error) {
	switch target {
	case "string":
		return Stringify(value), nil
	case "int":
		number, ok := ToFloat(value)
		if !ok {
			return nil, fmt.Errorf("cannot read %T as an integer", value)
		}
		return int64(number), nil
	case "float", "number":
		number, ok := ToFloat(value)
		if !ok {
			return nil, fmt.Errorf("cannot read %T as a number", value)
		}
		return number, nil
	case "bool", "boolean":
		return Truthy(value), nil
	case "time":
		switch typed := value.(type) {
		case time.Time:
			return typed, nil
		case string:
			parsed, err := time.Parse(time.RFC3339, typed)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("cannot read %T as a timestamp", value)
		}
	case "duration":
		switch typed := value.(type) {
		case time.Duration:
			return typed, nil
		case string:
			parsed, err := time.ParseDuration(typed)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("cannot read %T as a duration", value)
		}
	case "json":
		var decoded any
		if err := json.Unmarshal([]byte(Stringify(value)), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unknown coercion target %q", target)
	}
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

// lookupPath walks a dotted path through maps and slices. A numeric segment
// indexes a slice, so "items.0.sku" reads the first item's SKU — the shape
// JSON payloads actually have.
func lookupPath(root any, segments []string) (any, bool) {
	current := root
	for _, segment := range segments {
		switch typed := current.(type) {
		case map[string]any:
			next, ok := typed[segment]
			if !ok {
				return nil, false
			}
			current = next
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			current = typed[index]
		case []map[string]any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			current = typed[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// setPath writes through a dotted path, creating intermediate maps. A segment
// whose existing value is not a map is replaced: the explicit instruction to
// write at a.b.c beats whatever a.b happened to hold.
func setPath(root map[string]any, segments []string, value any) {
	if len(segments) == 0 {
		return
	}
	current := root
	for _, segment := range segments[:len(segments)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
	current[segments[len(segments)-1]] = value
}

func deletePath(root map[string]any, segments []string) {
	if len(segments) == 0 {
		return
	}
	current := root
	for _, segment := range segments[:len(segments)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, segments[len(segments)-1])
}

// cloneValue deep-copies the JSON-shaped part of a value. Anything else (a
// time, a struct, a resource handle) is shared by reference, which is correct:
// those are immutable or intentionally shared, and copying them would be both
// expensive and wrong.
func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = cloneValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneValue(item)
		}
		return out
	case []map[string]any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneValue(item)
		}
		return out
	default:
		return value
	}
}

func valueDepth(value any, depth int) int {
	switch typed := value.(type) {
	case map[string]any:
		deepest := depth + 1
		for _, item := range typed {
			if d := valueDepth(item, depth+1); d > deepest {
				deepest = d
			}
		}
		return deepest
	case []any:
		deepest := depth + 1
		for _, item := range typed {
			if d := valueDepth(item, depth+1); d > deepest {
				deepest = d
			}
		}
		return deepest
	default:
		return depth
	}
}

// maskValue hides all but the last keep characters. A value too short to mask
// meaningfully becomes all asterisks rather than leaking most of itself.
func maskValue(text string, keep int) string {
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= keep {
		return strings.Repeat("*", len(runes))
	}
	return strings.Repeat("*", len(runes)-keep) + string(runes[len(runes)-keep:])
}

func lowerValues(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = strings.ToLower(strings.TrimSpace(value))
	}
	return out
}

func titleCase(text string) string {
	parts := strings.Fields(strings.ToLower(text))
	for i, part := range parts {
		runes := []rune(part)
		runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
}

func slugify(text string) string {
	var out strings.Builder
	lastDash := true
	for _, r := range strings.ToLower(text) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				out.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(out.String(), "-")
}

// decodeDataSpec builds a DataSpec from a nested config block.
//
// This exists because a `data { … }` block inside a node's `config { … }` arrives
// as an untyped map rather than through BCL's struct decoding — the config map is
// deliberately open so host-registered actions can define their own shapes. The
// keys are exactly the ones DataSpec declares, so an author writing a data block
// on a node and one on a process edge writes the same thing.
func decodeDataSpec(raw map[string]any) (*DataSpec, error) {
	spec := &DataSpec{
		Source:   configString(raw, "source", ""),
		Extract:  stringMap(raw["extract"]),
		Set:      configMap(raw, "set"),
		Defaults: configMap(raw, "defaults"),
		Pick:     configStrings(raw, "pick"),
		Omit:     configStrings(raw, "omit"),
		Rename:   stringMap(raw["rename"]),
		Coerce:   stringMap(raw["coerce"]),
		Append:   stringMap(raw["append"]),
		Prepend:  stringMap(raw["prepend"]),
		Flatten:  configStrings(raw, "flatten"),
		Redact:   configStrings(raw, "redact"),
		Mask:     configStrings(raw, "mask"),
		Strict:   configBool(raw, "strict", false),
		Schema:   configString(raw, "schema", ""),
	}
	var err error
	if spec.MaskKeep, err = configInt(raw, "mask_keep", 0); err != nil {
		return nil, err
	}
	if spec.MaxBytes, err = configInt64(raw, "max_bytes", 0); err != nil {
		return nil, err
	}
	if spec.MaxDepth, err = configInt(raw, "max_depth", 0); err != nil {
		return nil, err
	}
	for _, block := range configBlocks(raw, "transforms", "transform") {
		spec.Transforms = append(spec.Transforms, DataTransformSpec{
			Path:   configString(block, "path", ""),
			Expr:   configString(block, "expr", ""),
			Op:     configString(block, "op", ""),
			Arg:    configString(block, "arg", ""),
			OnlyIf: configString(block, "only_if", ""),
		})
	}
	for _, block := range configBlocks(raw, "filters", "filter") {
		spec.Filters = append(spec.Filters, DataFilterSpec{
			Expr:   configString(block, "expr", ""),
			Mode:   configString(block, "mode", ""),
			Reason: configString(block, "reason", ""),
		})
	}
	// An empty extract map from stringMap is indistinguishable from an absent
	// one; normalise so Empty() reports accurately.
	if len(spec.Extract) == 0 {
		spec.Extract = nil
	}
	if len(spec.Rename) == 0 {
		spec.Rename = nil
	}
	if len(spec.Coerce) == 0 {
		spec.Coerce = nil
	}
	if len(spec.Append) == 0 {
		spec.Append = nil
	}
	if len(spec.Prepend) == 0 {
		spec.Prepend = nil
	}
	return spec, nil
}
