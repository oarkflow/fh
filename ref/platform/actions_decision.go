package platform

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/platform/spi"
)

// Decision actions.
//
// A decision node is not an ordinary node that happens to return a boolean. It
// participates in REF's deny-dominant decision algebra: the scheduler will not
// run any effect, async-effect or operation node until every decision node has
// recorded an allow, and a single deny anywhere in the plan blocks all of them
// regardless of how many allows were recorded.
//
// That is why authorization belongs here rather than in a validate node. A
// validate node that returns "not allowed" only stops the nodes that depend on
// its fact; a decision node stops every write in the graph.

func registerDecisionActions(r *Registry) {
	mustAction(r, "decision.expression", decisionExpressionAction, ActionInfo{
		Family:  "decision",
		Summary: "Allow or deny based on an expression",
		Kind:    "decision",
		Config: []ConfigField{
			{Name: "expression", Type: "expression", Required: true},
			{Name: "message", Type: "string", Summary: "Caller-facing denial message"},
		},
	})

	mustAction(r, "decision.authz", decisionAuthzAction, ActionInfo{
		Family:       "decision",
		Summary:      "Allow or deny using the platform's authorization rules: roles, permissions, scopes and a condition",
		ResourceKind: "authz",
		Kind:         "decision",
		Config: []ConfigField{
			{Name: "roles", Type: "[]string"},
			{Name: "permissions", Type: "[]string"},
			{Name: "scopes", Type: "[]string"},
			{Name: "deny_roles", Type: "[]string"},
			{Name: "require_all", Type: "bool"},
			{Name: "condition", Type: "expression"},
			{Name: "message", Type: "string"},
		},
	})

	mustAction(r, "decision.tenant", decisionTenantAction, ActionInfo{
		Family:  "decision",
		Summary: "Require a resolved tenant, and optionally that a record belongs to it",
		Kind:    "decision",
		Config: []ConfigField{
			{Name: "record_fact", Type: "fact", Summary: "Record whose tenant must match the request's"},
			{Name: "tenant_field", Type: "string", Default: "tenant_id"},
			{Name: "message", Type: "string"},
		},
	})

	mustAction(r, "decision.table", decisionTableAction, ActionInfo{
		Family:   "decision",
		Summary:  "Evaluate an ordered decision table and publish the first matching outcome",
		Provides: "The matched outcome, with its reason code",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "rules", Type: "[]object", Required: true, Summary: `Ordered: rules [ { name … condition … outcome … reason … score … } … ]`},
			{Name: "default_outcome", Type: "string", Summary: "Used when no rule matches; without it, no match is a failure"},
			{Name: "default_reason", Type: "string"},
			{Name: "mode", Type: "string", Default: "first", Summary: `"first" stops at the first match; "all" collects every match; "highest_score" picks the best`},
			{Name: "deny_outcomes", Type: "[]string", Summary: "Outcomes that additionally record a policy deny"},
		},
	})

	mustAction(r, "decision.rate_limit", decisionRateLimitAction, ActionInfo{
		Family:       "decision",
		Summary:      "Deny when a rate limit is exhausted",
		ResourceKind: "ratelimit",
		Kind:         "decision",
		Config: []ConfigField{
			{Name: "key", Type: "expression", Required: true},
			{Name: "limit", Type: "int", Required: true},
			{Name: "window", Type: "duration", Required: true},
			{Name: "message", Type: "string"},
		},
	})
}

var decisionExpressionAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	expr, err := requiredExpr(spec.Config, "expression")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	message := configString(spec.Config, "message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		allow, err := expr.Bool(actionEnv(ctx))
		if err != nil {
			// An expression that cannot be evaluated denies. Treating an error as
			// "allow" would turn a typo in a guard into an open door.
			return ActionResult{Decision: &Decision{Allow: false, Message: "policy evaluation failed"}}, err
		}
		decision := &Decision{Allow: allow, Message: message}
		if allow {
			return ActionResult{Decision: decision}, nil
		}
		return ActionResult{Decision: decision}, permissionDenied(message)
	}), nil
})

// authzGate is a compiled AuthzSpec: the same evaluation used by a route, an
// intent, a node and a process step.
type authzGate struct {
	roles       []string
	permissions []string
	scopes      []string
	denyRoles   []string
	requireAll  bool
	condition   *Expression
	message     string
	authorizer  spi.Authorizer
}

// compileAuthz builds the gate. An empty gate is rejected: a declared but unfilled
// authz block is a mistake, and the safe reading of a mistake is "deny", which a
// compile error expresses better than a silent runtime denial would.
func compileAuthz(build BuildContext, what string, spec *AuthzSpec) (*authzGate, error) {
	if spec == nil {
		return nil, nil
	}
	if spec.Empty() {
		return nil, fmt.Errorf("%s: this authz block declares no rule. Give it roles, permissions, scopes or a condition, or remove it", what)
	}
	condition, err := CompileExpr(spec.Condition)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	gate := &authzGate{
		roles:       spec.Roles,
		permissions: spec.Permissions,
		scopes:      spec.Scopes,
		denyRoles:   spec.DenyRoles,
		requireAll:  spec.RequireAll,
		condition:   condition,
		message:     spec.Message,
	}
	if len(spec.Permissions) > 0 {
		name := spec.Authorizer
		if name == "" {
			name = soleAuthorizerName(build)
		}
		if name == "" {
			return nil, fmt.Errorf("%s: checking permissions needs an authz resource. Declare one (authz.rbac) or name it with authorizer", what)
		}
		resolved, ok := build.Resource(name)
		if !ok {
			return nil, fmt.Errorf("%s: authorizer %q is not declared", what, name)
		}
		authorizer, ok := resolved.(spi.Authorizer)
		if !ok {
			return nil, fmt.Errorf("%s: resource %q is not an authorizer", what, name)
		}
		gate.authorizer = authorizer
	}
	return gate, nil
}

// soleAuthorizerName finds the application's single authorizer, so an author with
// one authz resource does not have to name it on every gate.
func soleAuthorizerName(build BuildContext) string {
	found := ""
	for name, resource := range build.Resources {
		if _, ok := resource.(spi.Authorizer); !ok {
			continue
		}
		if found != "" {
			// More than one: refuse to guess. Picking arbitrarily between two
			// authorization policies is the worst possible default.
			return ""
		}
		found = name
	}
	return found
}

// evaluate runs the gate in its fixed, deny-dominant order.
func (g *authzGate) evaluate(ctx *ActionContext, env Env) (bool, string, error) {
	if g == nil {
		return true, "", nil
	}
	principal := ctx.Principal
	if principal.ID == "" {
		return false, "authentication required", nil
	}
	for _, role := range g.denyRoles {
		if principal.HasRole(role) {
			return false, g.denial("you are not permitted to do this"), nil
		}
	}
	if len(g.roles) > 0 {
		if g.requireAll {
			for _, role := range g.roles {
				if !principal.HasRole(role) {
					return false, g.denial("you do not have the required role"), nil
				}
			}
		} else if !slices.ContainsFunc(g.roles, principal.HasRole) {
			return false, g.denial("you do not have the required role"), nil
		}
	}
	for _, permission := range g.permissions {
		allowed, err := g.authorizer.HasPermission(ctx.Context, principal, permission)
		if err != nil {
			return false, "authorization check failed", err
		}
		if !allowed {
			return false, g.denial("you do not have the required permission"), nil
		}
	}
	for _, scope := range g.scopes {
		if !principal.HasScope(scope) {
			return false, g.denial("this credential does not carry the required scope"), nil
		}
	}
	if g.condition != nil {
		if env == nil {
			env = actionEnv(ctx)
		}
		ok, err := g.condition.Bool(env)
		if err != nil {
			return false, "authorization check failed", err
		}
		if !ok {
			return false, g.denial("you are not permitted to do this"), nil
		}
	}
	return true, "", nil
}

func (g *authzGate) denial(fallback string) string {
	if g.message != "" {
		return g.message
	}
	return fallback
}

var decisionAuthzAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	authzSpec := &AuthzSpec{
		Roles:       configStrings(spec.Config, "roles"),
		Permissions: configStrings(spec.Config, "permissions"),
		Scopes:      configStrings(spec.Config, "scopes"),
		DenyRoles:   configStrings(spec.Config, "deny_roles"),
		RequireAll:  configBool(spec.Config, "require_all", false),
		Condition:   configString(spec.Config, "condition", ""),
		Message:     configString(spec.Config, "message", ""),
		Authorizer:  spec.Resource,
	}
	gate, err := compileAuthz(build, "node "+spec.Name, authzSpec)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		allowed, message, err := gate.evaluate(ctx, actionEnv(ctx))
		if err != nil {
			return ActionResult{Decision: &Decision{Allow: false, Message: message}}, err
		}
		if allowed {
			return ActionResult{Decision: &Decision{Allow: true}}, nil
		}
		return ActionResult{Decision: &Decision{Allow: false, Message: message}}, permissionDenied(message)
	}), nil
})

var decisionTenantAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	recordFact := configString(spec.Config, "record_fact", "")
	tenantField := configString(spec.Config, "tenant_field", "tenant_id")
	message := configString(spec.Config, "message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		if ctx.TenantID == "" {
			return ActionResult{Decision: &Decision{Allow: false, Message: "no tenant"}},
				permissionDenied("this operation requires a tenant, and none could be determined for the request")
		}
		if recordFact == "" {
			return ActionResult{Decision: &Decision{Allow: true}}, nil
		}
		record, found := resolvePath(ctx.Inputs, recordFact)
		if !found {
			return ActionResult{Decision: &Decision{Allow: false}}, notFound("record", recordFact)
		}
		object, ok := record.(map[string]any)
		if !ok {
			if list, listOK := record.([]map[string]any); listOK && len(list) > 0 {
				object = list[0]
			} else {
				return ActionResult{Decision: &Decision{Allow: false}}, invalidInput("%q is not a record", recordFact)
			}
		}
		if Stringify(object[tenantField]) != ctx.TenantID {
			// Reported as not-found rather than forbidden. Confirming that a record
			// exists in another tenant is itself a cross-tenant disclosure.
			return ActionResult{Decision: &Decision{Allow: false, Message: "not found"}}, notFoundOrMessage(message)
		}
		return ActionResult{Decision: &Decision{Allow: true}}, nil
	}), nil
})

// ---------------------------------------------------------------------------
// Decision tables
// ---------------------------------------------------------------------------

// decisionRule is one row of a decision table.
type decisionRule struct {
	name    string
	when    *Expression
	outcome string
	reason  string
	score   float64
	data    map[string]any
}

var decisionTableAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	blocks := configBlocks(spec.Config, "rules", "rule")
	if len(blocks) == 0 {
		return nil, fmt.Errorf("node %q: decision.table needs a config.rules list, e.g. rules [ { condition \"…\" outcome \"…\" } ]", spec.Name)
	}
	rules := make([]decisionRule, 0, len(blocks))
	for i, block := range blocks {
		when, err := CompileExpr(configString(block, "condition", ""))
		if err != nil {
			return nil, fmt.Errorf("node %q: rule[%d]: %w", spec.Name, i, err)
		}
		outcome := configString(block, "outcome", "")
		if outcome == "" {
			return nil, fmt.Errorf("node %q: rule[%d] needs an outcome", spec.Name, i)
		}
		score, err := configFloat(block, "score", 0)
		if err != nil {
			return nil, fmt.Errorf("node %q: rule[%d]: %w", spec.Name, i, err)
		}
		rules = append(rules, decisionRule{
			name:    configString(block, "name", fmt.Sprintf("rule_%d", i)),
			when:    when,
			outcome: outcome,
			reason:  configString(block, "reason", ""),
			score:   score,
			data:    configMap(block, "data"),
		})
	}
	mode := strings.ToLower(configString(spec.Config, "mode", "first"))
	if !slices.Contains([]string{"first", "all", "highest_score"}, mode) {
		return nil, fmt.Errorf("node %q: mode must be first, all or highest_score", spec.Name)
	}
	defaultOutcome := configString(spec.Config, "default_outcome", "")
	defaultReason := configString(spec.Config, "default_reason", "")
	denyOutcomes := configStrings(spec.Config, "deny_outcomes")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		var matched []decisionRule
		for _, rule := range rules {
			ok, err := rule.when.Bool(env)
			if err != nil {
				return ActionResult{}, fmt.Errorf("rule %q: %w", rule.name, err)
			}
			if !ok {
				continue
			}
			matched = append(matched, rule)
			if mode == "first" {
				break
			}
		}

		if len(matched) == 0 {
			if defaultOutcome == "" {
				return ActionResult{}, invalidInput("no decision rule matched, and this table declares no default_outcome")
			}
			return decisionTableResult(spec, decisionRule{name: "default", outcome: defaultOutcome, reason: defaultReason}, nil, denyOutcomes), nil
		}
		if mode == "highest_score" {
			best := matched[0]
			for _, rule := range matched[1:] {
				if rule.score > best.score {
					best = rule
				}
			}
			return decisionTableResult(spec, best, matched, denyOutcomes), nil
		}
		return decisionTableResult(spec, matched[0], matched, denyOutcomes), nil
	}), nil
})

// decisionTableResult renders the matched rule, recording a policy deny when the
// outcome is one the table marked as denying. That is what lets one table both
// classify a case and block the writes downstream of it.
func decisionTableResult(spec NodeSpec, chosen decisionRule, all []decisionRule, denyOutcomes []string) ActionResult {
	names := make([]string, 0, len(all))
	for _, rule := range all {
		names = append(names, rule.name)
	}
	value := map[string]any{
		"outcome":       chosen.outcome,
		"reason":        chosen.reason,
		"rule":          chosen.name,
		"score":         chosen.score,
		"matched_rules": names,
	}
	for key, item := range chosen.data {
		value[key] = item
	}
	result := ActionResult{Outputs: map[string]any{spec.Provides[0]: value}}
	if slices.Contains(denyOutcomes, chosen.outcome) {
		message := chosen.reason
		if message == "" {
			message = "this request was declined by policy"
		}
		result.Decision = &Decision{Allow: false, Message: message}
	}
	return result
}

// ---------------------------------------------------------------------------
// Rate limiting as a decision
// ---------------------------------------------------------------------------

var decisionRateLimitAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	limiter, err := requireResource[spi.RateLimiter](build, spec, "a rate limiter resource")
	if err != nil {
		return nil, err
	}
	key, err := requiredExpr(spec.Config, "key")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	limit, err := configInt(spec.Config, "limit", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("node %q: decision.rate_limit needs a positive limit", spec.Name)
	}
	window, err := configDuration(spec.Config, "window", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if window <= 0 {
		return nil, fmt.Errorf("node %q: decision.rate_limit needs a positive window", spec.Name)
	}
	message := configString(spec.Config, "message", "")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.String(actionEnv(ctx))
		if err != nil {
			return ActionResult{Decision: &Decision{Allow: false}}, err
		}
		if resolved == "" {
			// An empty key would pool every caller into one bucket, which either
			// throttles everybody or nobody. Both are wrong enough to refuse.
			return ActionResult{Decision: &Decision{Allow: false}}, invalidInput("the rate-limit key evaluated to empty")
		}
		allowed, remaining, resetAt, err := limiter.Allow(ctx.Context, resolved, limit, window)
		if err != nil {
			// A limiter that is unreachable fails closed. A rate limit exists to
			// protect something, and protecting nothing while the limiter is down
			// is when it is needed most.
			return ActionResult{Decision: &Decision{Allow: false}}, unavailable("the rate limiter is unavailable")
		}
		if allowed {
			return ActionResult{
				Decision: &Decision{Allow: true, Obligations: nil},
				Outputs:  limitOutputs(spec, remaining, resetAt),
			}, nil
		}
		return ActionResult{Decision: &Decision{Allow: false, Message: "too many requests"}}, rateLimited(message)
	}), nil
})

func limitOutputs(spec NodeSpec, remaining int, resetAt time.Time) map[string]any {
	if len(spec.Provides) == 0 {
		return nil
	}
	return map[string]any{spec.Provides[0]: map[string]any{
		"remaining": remaining,
		"reset_at":  resetAt,
	}}
}
