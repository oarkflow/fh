package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/platform/spi"
)

// Shared machinery for action implementations.
//
// Two ideas run through the whole action catalog and are worth stating once
// here rather than repeating in sixty factories:
//
//  1. Everything expensive happens at build time. An action factory resolves its
//     resource, compiles its expressions and templates, validates its config and
//     closes over the result. Run does the work and nothing else — no map
//     lookups for configuration, no expression parsing, no reflection.
//  2. Failures carry a category. A node that fails because the caller sent bad
//     input must not look like a node that failed because a database was down:
//     the first is a 422 and must not be retried, the second is a 503 and should
//     be. Every action reports through intent.Failure so the transport, the
//     retry policy and the process engine all agree.

// actionEnv builds the expression environment for one node invocation.
//
// The facts are exposed twice — flat at the top level and under "facts" — so an
// author can write either `order.total` or `facts.order.total`. The flat form is
// what people reach for; the namespaced form disambiguates when a fact is called
// something like "input" or "now".
func actionEnv(ctx *ActionContext) Env {
	env := make(Env, len(ctx.Inputs)+8)
	maps.Copy(env, ctx.Inputs)
	env["facts"] = ctx.Inputs
	env["config"] = ctx.Config
	env["principal"] = principalMap(ctx.Principal)
	env["tenant"] = ctx.TenantID
	env["now"] = ctx.Now.Format(time.RFC3339Nano)
	if _, shadowed := ctx.Inputs["input"]; !shadowed {
		env["input"] = nil
	}
	if sess, ok := sessionFromContext(ctx.Context); ok {
		env["session"] = sessionMap(sess)
	}
	return env
}

// principalMap renders a principal for expression use. It is a map rather than
// the struct so an expression can reach `principal.claims.department` without the
// expression engine needing to know Go types.
func principalMap(principal Principal) map[string]any {
	return map[string]any{
		"id":        principal.ID,
		"username":  principal.Username,
		"email":     principal.Email,
		"tenant_id": principal.TenantID,
		"roles":     principal.Roles,
		"scopes":    principal.Scopes,
		"claims":    principal.Claims,
	}
}

func sessionMap(sess *session.Session) map[string]any {
	if sess == nil {
		return nil
	}
	values := map[string]any{"id": sess.ID}
	for _, key := range []string{"user_id", "tenant_id", "roles", "username", "email"} {
		if value := sess.Get(key); value != nil {
			values[key] = value
		}
	}
	return values
}

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

// exactlyOneOutput enforces that a node publishing a single value declared a
// single fact. Getting this wrong is the most common authoring slip, and the
// message says what to do about it.
func exactlyOneOutput(spec NodeSpec) error {
	if len(spec.Provides) != 1 {
		return fmt.Errorf("node %q: %s publishes one value, so it needs exactly one name in provides (got %d)",
			spec.Name, spec.Uses, len(spec.Provides))
	}
	return nil
}

// acknowledgement publishes a value only if the node asked for one. Actions whose
// point is the side effect (a cache delete, an audit record) are usable both with
// and without a provided fact, and forcing an author to declare an unused fact
// just to call one would be noise.
func acknowledgement(spec NodeSpec, value any) ActionResult {
	if len(spec.Provides) == 0 {
		return ActionResult{}
	}
	return ActionResult{Outputs: map[string]any{spec.Provides[0]: value}}
}

// singleOutput publishes to the node's one declared fact.
func singleOutput(spec NodeSpec, value any) ActionResult {
	return ActionResult{Outputs: map[string]any{spec.Provides[0]: value}}
}

// ---------------------------------------------------------------------------
// Fact access
// ---------------------------------------------------------------------------

// factString reads a required string from the node's facts by dotted path.
func factString(inputs map[string]any, name string) (string, error) {
	value, found := resolvePath(inputs, name)
	if !found {
		return "", fmt.Errorf("required value %q is missing", name)
	}
	text := Stringify(value)
	if text == "" {
		return "", fmt.Errorf("value %q must not be empty", name)
	}
	return text, nil
}

// factArgs reads a list of dotted paths into positional query arguments. A
// missing path becomes nil, which the database renders as NULL — the same thing
// an omitted optional column means.
func factArgs(inputs map[string]any, names []string) []any {
	result := make([]any, 0, len(names))
	for _, name := range names {
		value, _ := resolvePath(inputs, name)
		result = append(result, normalizeSQLArg(value))
	}
	return result
}

// normalizeSQLArg converts values database/sql drivers refuse to handle. A
// decoded JSON object or array has no natural SQL type, so it goes across as
// JSON text rather than failing at bind time with an unhelpful driver error.
func normalizeSQLArg(value any) any {
	switch typed := value.(type) {
	case nil, bool, string, []byte, int, int32, int64, float32, float64, time.Time:
		return typed
	case map[string]any, []any, []map[string]any:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return Stringify(typed)
		}
		return string(encoded)
	default:
		return Stringify(typed)
	}
}

// resolvePath walks a dotted path through the node's facts, indexing slices with
// numeric segments so "users.0.email" reads the first row's email.
func resolvePath(root map[string]any, path string) (any, bool) {
	var current any = root
	for _, segment := range strings.Split(path, ".") {
		switch value := current.(type) {
		case map[string]any:
			next, ok := value[segment]
			if !ok {
				return nil, false
			}
			current = next
		case []map[string]any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(value) {
				return nil, false
			}
			current = value[index]
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(value) {
				return nil, false
			}
			current = value[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// ---------------------------------------------------------------------------
// Resource resolution
// ---------------------------------------------------------------------------

// requireResource resolves the node's named resource and asserts its type. The
// error names the node, the resource and what was expected, because "type
// assertion failed" on its own tells an author nothing about which line of
// configuration to fix.
func requireResource[T any](build BuildContext, spec NodeSpec, what string) (T, error) {
	var zero T
	if spec.Resource == "" {
		return zero, fmt.Errorf("node %q: %s needs a resource (%s)", spec.Name, spec.Uses, what)
	}
	resolved, ok := build.Resource(spec.Resource)
	if !ok {
		return zero, fmt.Errorf("node %q: resource %q is not declared", spec.Name, spec.Resource)
	}
	typed, ok := resolved.(T)
	if !ok {
		return zero, fmt.Errorf("node %q: resource %q is not %s", spec.Name, spec.Resource, what)
	}
	return typed, nil
}

// requireCache resolves a cache, preferring the context-aware contract when the
// provider offers it so cancellation reaches the backend.
func requireCache(build BuildContext, spec NodeSpec, key string) (cacheHandle, error) {
	var resolved Resource
	var name string
	if key == "" {
		name = spec.Resource
	} else {
		name = configString(spec.Config, key, "")
	}
	if name == "" {
		return cacheHandle{}, fmt.Errorf("node %q: a cache resource is required", spec.Name)
	}
	found, ok := build.Resource(name)
	if !ok {
		return cacheHandle{}, fmt.Errorf("node %q: cache resource %q is not declared", spec.Name, name)
	}
	resolved = found

	handle := cacheHandle{name: name}
	if typed, ok := resolved.(spi.CacheContext); ok {
		handle.withContext = typed
	}
	if typed, ok := resolved.(spi.Cache); ok {
		handle.plain = typed
	}
	if handle.withContext == nil && handle.plain == nil {
		return cacheHandle{}, fmt.Errorf("node %q: resource %q is not a cache", spec.Name, name)
	}
	handle.prefixed, _ = resolved.(spi.CachePrefix)
	return handle, nil
}

// cacheHandle hides the two cache contracts behind one call site.
type cacheHandle struct {
	name        string
	plain       spi.Cache
	withContext spi.CacheContext
	prefixed    spi.CachePrefix
}

func (c cacheHandle) get(ctx context.Context, key string) ([]byte, bool, error) {
	if c.withContext != nil {
		return c.withContext.GetContext(ctx, key)
	}
	return c.plain.Get(key)
}

func (c cacheHandle) set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c.withContext != nil {
		return c.withContext.SetContext(ctx, key, value, ttl)
	}
	return c.plain.Set(key, value, ttl)
}

func (c cacheHandle) delete(ctx context.Context, key string) error {
	if c.withContext != nil {
		return c.withContext.DeleteContext(ctx, key)
	}
	return c.plain.Delete(key)
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

// queryRows runs a query and materialises the rows as maps.
//
// Byte slices become strings: a driver returns TEXT and VARCHAR columns as
// []byte, which would JSON-encode as base64 and surprise every author who
// looked at the response. Whatever else a driver hands back is passed through
// untouched.
func queryRows(ctx context.Context, db *sql.DB, statement string, args []any) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, 8)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			if raw, ok := values[i].([]byte); ok {
				row[column] = string(raw)
				continue
			}
			row[column] = values[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// databaseFailure maps SQLSTATE codes onto platform failure categories.
//
// This matters more than it looks. Without it, a unique-constraint violation on
// a duplicate signup reaches the caller as a 500 "internal error" and gets
// retried; with it, the caller sees a 409 and stops. The codes covered are the
// ones an application actually hits.
func databaseFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return intent.Failure{Code: "TIMEOUT", Category: intent.CategoryTimeout, Message: "the database did not respond in time", Cause: err}
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	type sqlState interface{ SQLState() string }
	var state sqlState
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505": // unique_violation
			return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: "that record already exists", Cause: err}
		case "23503": // foreign_key_violation
			return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: "that record is still referenced by something else", Cause: err}
		case "23502": // not_null_violation
			return intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: "a required field was missing", Cause: err}
		case "23514", "22001", "22P02", "22003": // check, too long, bad syntax, out of range
			return intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: "the database rejected the input", Cause: err}
		case "40001", "40P01": // serialization failure, deadlock
			return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: "a concurrent change conflicted; retry the request", Cause: err}
		case "53300", "57P03": // too many connections, cannot connect now
			return intent.Failure{Code: "UNAVAILABLE", Category: intent.CategoryUnavailable, Message: "the database is not accepting connections", Cause: err}
		case "42501": // insufficient privilege
			return intent.Failure{Code: "UNAVAILABLE", Category: intent.CategoryUnavailable, Message: "the database rejected the operation", Cause: err}
		}
	}
	// Drivers without SQLSTATE (SQLite, MySQL) are matched on message text. It is
	// less precise, but a duplicate key reaching the client as a 409 rather than
	// a 500 is worth an imprecise check.
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "unique constraint"), strings.Contains(text, "duplicate entry"), strings.Contains(text, "duplicate key"):
		return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: "that record already exists", Cause: err}
	case strings.Contains(text, "foreign key constraint"):
		return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: "that record is still referenced by something else", Cause: err}
	case strings.Contains(text, "check constraint"), strings.Contains(text, "not null"):
		return intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: "the database rejected the input", Cause: err}
	case strings.Contains(text, "database is locked"), strings.Contains(text, "deadlock"):
		return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: "a concurrent change conflicted; retry the request", Cause: err}
	}
	return err
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

func invalidInput(format string, args ...any) error {
	return intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: fmt.Sprintf(format, args...)}
}

func conflict(format string, args ...any) error {
	return intent.Failure{Code: "CONFLICT", Category: intent.CategoryConflict, Message: fmt.Sprintf(format, args...)}
}

func unavailable(format string, args ...any) error {
	return intent.Failure{Code: "UNAVAILABLE", Category: intent.CategoryUnavailable, Message: fmt.Sprintf(format, args...)}
}

func permissionDenied(message string) error {
	if message == "" {
		message = "you do not have permission to do that"
	}
	return intent.Failure{Code: "PERMISSION_DENIED", Category: intent.CategoryPermission, Message: message}
}

// isNotFound reports whether an error is the platform's not-found failure, so a
// caller can offer an "optional" mode without string matching.
func isNotFound(err error) bool {
	var failure intent.Failure
	return errors.As(err, &failure) && failure.Category == intent.CategoryNotFound
}

func rateLimited(message string) error {
	if message == "" {
		message = "too many requests"
	}
	return intent.Failure{Code: "RATE_LIMITED", Category: intent.CategoryRateLimit, Message: message}
}
