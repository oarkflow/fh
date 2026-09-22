package platform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/bcl"
)

// One expression language, one variable environment, everywhere.
//
// Edge guards, node skip conditions, authorization conditions, data extracts,
// transforms, filters, threshold values, rate-limit keys and task templates all
// compile through this file. An application author learns the language once and
// it behaves identically at every boundary — which is the difference between a
// configurable system and a collection of subtly different mini-languages.
//
// Every expression is compiled at load time. A syntax error stops the
// deployment from starting rather than surfacing on the one request that
// happened to take that branch.

// Env is the variable environment an expression sees. The key set is fixed and
// documented so an author can rely on it:
//
//	input      the intent's decoded request body
//	facts      every fact available to this node, by local name
//	result     the value under consideration (a node output, an edge payload)
//	principal  the authenticated identity: id, username, email, roles, scopes, claims
//	tenant     the resolved tenant id
//	session    the session values, when the route has a session
//	run        the process run: id, process, status, input, output, started_at
//	step       the current process step: name, attempt, result
//	now        the invocation's logical clock, as RFC3339
//	env        the process environment name from the document
//	const      application constants
//
// A key absent in a given context is absent rather than nil-valued, so Strict
// mode can tell "not applicable here" from "explicitly null".
type Env map[string]any

// Expression is a compiled, reusable expression. It is safe for concurrent use.
type Expression struct {
	raw  string
	prog *bcl.ExpressionProgram
}

// Raw returns the source text, for error messages and catalog output.
func (e *Expression) Raw() string {
	if e == nil {
		return ""
	}
	return e.raw
}

// evalOptions enables the pure helper families an expression may call. Network
// access, command execution and filesystem reads are deliberately not among
// them: an expression is a predicate over data already in hand, and anything
// that reaches outside belongs in a node where it is budgeted and audited.
var evalOptions = &bcl.EvalOptions{
	AllowHash:     true,
	AllowEncoding: true,
	AllowTime:     true,
}

// CompileExpr compiles one expression. An empty string compiles to nil, which
// every caller treats as "no condition", so an optional guard needs no special
// casing at the call site.
func CompileExpr(raw string) (*Expression, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	prog, err := bcl.CompileExpression(raw)
	if err != nil {
		return nil, fmt.Errorf("compile expression %q: %w", raw, err)
	}
	return &Expression{raw: raw, prog: prog}, nil
}

// MustCompileExpr is CompileExpr for expressions built by this package itself,
// where a failure is a bug rather than bad configuration.
func MustCompileExpr(raw string) *Expression {
	expr, err := CompileExpr(raw)
	if err != nil {
		panic(err)
	}
	return expr
}

// Eval evaluates the expression against env.
func (e *Expression) Eval(env Env) (any, error) {
	if e == nil {
		return nil, nil
	}
	opts := *evalOptions
	opts.Variables = env
	value, err := e.prog.Eval(env, &opts)
	if err != nil {
		return nil, fmt.Errorf("evaluate %q: %w", e.raw, err)
	}
	return value, nil
}

// Bool evaluates the expression as a guard. A nil Expression is true, so an
// unset condition never blocks — matching how an omitted guard reads.
func (e *Expression) Bool(env Env) (bool, error) {
	if e == nil {
		return true, nil
	}
	value, err := e.Eval(env)
	if err != nil {
		return false, err
	}
	return Truthy(value), nil
}

// String evaluates the expression and renders the result as text.
func (e *Expression) String(env Env) (string, error) {
	if e == nil {
		return "", nil
	}
	value, err := e.Eval(env)
	if err != nil {
		return "", err
	}
	return Stringify(value), nil
}

// Float evaluates the expression as a number, which threshold edges and
// numeric guards need.
func (e *Expression) Float(env Env) (float64, bool, error) {
	if e == nil {
		return 0, false, nil
	}
	value, err := e.Eval(env)
	if err != nil {
		return 0, false, err
	}
	number, ok := ToFloat(value)
	return number, ok, nil
}

// Truthy is the platform's single definition of truth, applied identically by
// every guard.
//
// Nil, false, zero, the empty string and an empty collection are false; so are
// the strings "false", "0", "no" and "off", because a value that arrived as
// text from a form or a header should behave the way its author meant. Anything
// else is true.
func Truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "", "false", "0", "no", "off", "null":
			return false
		}
		return true
	case int:
		return typed != 0
	case int32:
		return typed != 0
	case int64:
		return typed != 0
	case float32:
		return typed != 0
	case float64:
		return typed != 0
	case []any:
		return len(typed) > 0
	case []string:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	case time.Duration:
		return typed != 0
	default:
		return true
	}
}

// Stringify renders a value as text without the %v formatting artefacts that
// make configuration output hard to read: a float that is a whole number loses
// its trailing zero, and a composite value becomes JSON rather than Go syntax.
func Stringify(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case bool:
		return strconv.FormatBool(typed)
	case int:
		return strconv.Itoa(typed)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float32:
		return formatFloat(float64(typed))
	case float64:
		return formatFloat(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case time.Duration:
		return typed.String()
	case json.Number:
		return typed.String()
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprint(typed)
		}
		return string(encoded)
	}
}

func formatFloat(value float64) string {
	if value == float64(int64(value)) {
		return strconv.FormatInt(int64(value), 10)
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// ToFloat converts a value to a number, reporting whether it could. Numeric
// strings convert, because JSON decoding and HTTP headers routinely deliver
// numbers as text and refusing them would only push the conversion into every
// caller.
func ToFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case json.Number:
		number, err := typed.Float64()
		return number, err == nil
	case string:
		number, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return number, err == nil
	case bool:
		if typed {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

// Template is a compiled string template: literal text interleaved with
// {{ expression }} holes. It backs DataSpec.Set values, task titles and
// instructions, notification bodies and header values.
//
// Templating is separate from expressions on purpose. A template always
// produces a string; an expression produces a typed value. Conflating them is
// how configuration ends up with numbers that are secretly strings.
type Template struct {
	raw   string
	parts []templatePart
}

type templatePart struct {
	literal string
	expr    *Expression
}

// CompileTemplate compiles a template. A string with no holes compiles to a
// constant, which costs nothing to render.
func CompileTemplate(raw string) (*Template, error) {
	tmpl := &Template{raw: raw}
	rest := raw
	for {
		open := strings.Index(rest, "{{")
		if open < 0 {
			break
		}
		close := strings.Index(rest[open:], "}}")
		if close < 0 {
			return nil, fmt.Errorf("template %q: unclosed {{", raw)
		}
		close += open
		if open > 0 {
			tmpl.parts = append(tmpl.parts, templatePart{literal: rest[:open]})
		}
		source := strings.TrimSpace(rest[open+2 : close])
		expr, err := CompileExpr(source)
		if err != nil {
			return nil, fmt.Errorf("template %q: %w", raw, err)
		}
		if expr == nil {
			return nil, fmt.Errorf("template %q: empty {{ }} hole", raw)
		}
		tmpl.parts = append(tmpl.parts, templatePart{expr: expr})
		rest = rest[close+2:]
	}
	if rest != "" {
		tmpl.parts = append(tmpl.parts, templatePart{literal: rest})
	}
	return tmpl, nil
}

// HasHoles reports whether the template interpolates anything. A template
// without holes can be stored as a plain string instead of rendered per call.
func (t *Template) HasHoles() bool {
	if t == nil {
		return false
	}
	for _, part := range t.parts {
		if part.expr != nil {
			return true
		}
	}
	return false
}

// Render evaluates the template against env.
func (t *Template) Render(env Env) (string, error) {
	if t == nil {
		return "", nil
	}
	if !t.HasHoles() {
		return t.raw, nil
	}
	var out strings.Builder
	out.Grow(len(t.raw) + 32)
	for _, part := range t.parts {
		if part.expr == nil {
			out.WriteString(part.literal)
			continue
		}
		value, err := part.expr.Eval(env)
		if err != nil {
			return "", err
		}
		out.WriteString(Stringify(value))
	}
	return out.String(), nil
}

// Raw returns the template source.
func (t *Template) Raw() string {
	if t == nil {
		return ""
	}
	return t.raw
}
