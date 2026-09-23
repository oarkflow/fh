package platform

// RouteSpec projects an intent or a process onto HTTP.
//
// Everything declared here is enforced before the intent is dispatched and
// fails closed: an authz block that cannot resolve its authorizer, a rate-limit
// block whose limiter is unreachable, or a tenant block that cannot determine a
// tenant all refuse the request rather than letting it through unguarded.
type RouteSpec struct {
	Name   string `bcl:",id"`
	Method string `bcl:"method,ident"`
	Path   string `bcl:"path"`
	Intent string `bcl:"intent"`
	// Process starts a durable process run instead of dispatching an intent.
	Process string `bcl:"process"`

	// Mode is "sync" (default), "async" (enqueue and return 202), "stream"
	// (server-sent events) or "download" (stream a stored object).
	Mode  string `bcl:"mode,ident"`
	Queue string `bcl:"queue"`

	// Session names a session resource. When set, the route begins a session
	// before dispatch and commits it after, so session nodes have one to use.
	Session string `bcl:"session"`
	// Auth names an authenticator resource. When set, the principal is
	// resolved before dispatch and exposed to every node — so a node does not
	// have to re-derive identity, and an unauthenticated request never reaches
	// a graph that assumed one.
	Auth string `bcl:"auth"`
	// AllowAnonymous permits an unauthenticated request through a route that
	// names an authenticator, for endpoints like login that must be reachable
	// without credentials. It must be explicit: forgetting to set it fails
	// closed, forgetting to unset it does not silently open the route.
	AllowAnonymous bool `bcl:"allow_anonymous"`

	Status       int               `bcl:"status"`
	Headers      map[string]string `bcl:"headers"`
	CacheControl string            `bcl:"cache_control"`

	Description string              `bcl:"description"`
	Tags        []string            `bcl:"tags"`
	Parameters  []HTTPParameterSpec `bcl:"parameter,block"`

	// Timeout bounds the whole request, independent of the intent's own budget.
	Timeout Duration `bcl:"timeout"`
	// MaxBodyBytes caps the request body. Zero uses the server default.
	MaxBodyBytes int64 `bcl:"max_body_bytes"`

	Authz        *AuthzSpec       `bcl:"authz"`
	RateLimit    *RateLimitSpec   `bcl:"rate_limit"`
	Idempotency  *IdempotencySpec `bcl:"idempotency"`
	Tenant       *TenantRouteSpec `bcl:"tenant"`
	Audit        *AuditSpec       `bcl:"route_audit"`
	CORS         *CORSSpec        `bcl:"cors"`
	RequestData  *DataSpec        `bcl:"request_data"`
	ResponseData *DataSpec        `bcl:"response_data"`
}

// HTTPParameterSpec declares a stable HTTP input contract for a route.
type HTTPParameterSpec struct {
	Name        string   `bcl:",id"`
	In          string   `bcl:"in,ident"`
	Description string   `bcl:"description"`
	Required    bool     `bcl:"required"`
	Kind        string   `bcl:"kind,ident"`
	Format      string   `bcl:"format,ident"`
	Enum        []string `bcl:"enum"`
}

// AuthzSpec is one authorization gate, used identically on a route, an intent,
// an intent node and a process step. One shape, one evaluation order, so a
// reviewer reads the same rules everywhere.
//
// Evaluation order is fixed and deny-dominant:
//
//  1. DenyRoles   — holding any listed role denies, immediately
//  2. Roles       — must hold at least one (or all, when RequireAll)
//  3. Permissions — must hold every listed permission
//  4. Condition   — must evaluate true
//
// An empty AuthzSpec denies rather than allows. A gate that was written but
// left unfilled is a mistake, and the safe reading of a mistake is "no".
type AuthzSpec struct {
	Roles       []string `bcl:"roles"`
	Permissions []string `bcl:"permissions"`
	// RequireAll makes Roles conjunctive instead of disjunctive.
	RequireAll bool     `bcl:"require_all"`
	DenyRoles  []string `bcl:"deny_roles"`
	// Scopes requires OAuth-style scopes, checked like Permissions.
	Scopes []string `bcl:"scopes"`
	// Condition is an expression over principal, tenant, input, session and
	// the facts available at this point — the hook for ownership checks such
	// as "principal.id == input.owner_id".
	Condition string `bcl:"condition"`
	// Authorizer names an authz resource. Empty uses the application's single
	// authorizer; naming one matters only in a deployment with several.
	Authorizer string `bcl:"authorizer"`
	// OnDeny is "error" (default, 403), "skip" (node/step only: publish
	// Fallback and continue) or "redirect:<step>" (process steps only).
	OnDeny string `bcl:"on_deny,ident"`
	// Message overrides the denial text shown to the caller. Keep it vague:
	// a precise denial reason is an information leak about what exists.
	Message string `bcl:"message"`
}

// Empty reports whether the gate declares no rule at all, which the compiler
// treats as an error rather than as "allow".
func (a *AuthzSpec) Empty() bool {
	if a == nil {
		return true
	}
	return len(a.Roles) == 0 && len(a.Permissions) == 0 && len(a.DenyRoles) == 0 &&
		len(a.Scopes) == 0 && a.Condition == ""
}

// RateLimitSpec throttles a route or a step.
//
// Key is an expression choosing what to count by; it defaults to the remote IP
// for an anonymous route and the principal ID for an authenticated one, which
// is almost always what an author means and never what they should have to
// spell out.
type RateLimitSpec struct {
	Limiter string   `bcl:"limiter"`
	Limit   int      `bcl:"limit"`
	Window  Duration `bcl:"window"`
	Key     string   `bcl:"key"`
	// Burst allows a short excess above Limit before throttling engages.
	Burst int `bcl:"burst"`
	// Message overrides the 429 body text.
	Message string `bcl:"message"`
}

// IdempotencySpec makes a mutating route safe to retry. The first request for a
// key executes and its response is stored; a repeat within TTL returns the
// stored response without re-running the intent.
//
// This only applies to intents marked idempotent. Replaying a non-idempotent
// intent's stored response would report success for work that was never done
// the second time, so the compiler rejects that combination.
type IdempotencySpec struct {
	// Header names the request header carrying the key. Default
	// "Idempotency-Key".
	Header string   `bcl:"header"`
	TTL    Duration `bcl:"ttl"`
	// Required rejects a request that omits the key, rather than processing it
	// unguarded.
	Required bool `bcl:"required"`
	// Store names a cache resource holding the records. Default is the
	// application's single cache.
	Store string `bcl:"store"`
	// Scope adds an expression to the key, e.g. "principal.id", so two tenants
	// cannot collide on the same client-chosen key.
	Scope string `bcl:"scope"`
}

// TenantRouteSpec resolves the tenant for a request. Resolution tries, in
// order: the principal's own tenant claim, Claim, then Header.
type TenantRouteSpec struct {
	Header string `bcl:"header"`
	Claim  string `bcl:"claim"`
	// Required rejects a request whose tenant cannot be resolved. Leave it on
	// for anything that reads tenant-scoped data: an unresolved tenant on a
	// tenant-scoped query is a cross-tenant read waiting to happen.
	Required bool `bcl:"required"`
	// Default is the tenant used when none resolves and Required is false.
	Default string `bcl:"default"`
}

// AuditSpec records an immutable, hash-chained entry per request: who, what,
// when, the outcome, and a redacted view of the payload.
type AuditSpec struct {
	Enabled *bool `bcl:"enabled"`
	// Action names the audited operation. Defaults to the route name.
	Action string `bcl:"action"`
	// Redact and Mask apply to the recorded payload, using the same semantics
	// as DataSpec. Credentials and tokens are always redacted regardless.
	Redact []string `bcl:"redact"`
	Mask   []string `bcl:"mask"`
	// IncludeRequest and IncludeResponse control how much body is recorded.
	// Both default to false: an audit trail is about decisions, and recording
	// whole payloads by default turns it into a second copy of the database.
	IncludeRequest  bool `bcl:"include_request"`
	IncludeResponse bool `bcl:"include_response"`
	// Resource names a database resource to persist entries to. Empty uses the
	// application's audit resource.
	Resource string `bcl:"resource"`
}

// CORSSpec configures cross-origin access for one route.
//
// AllowOrigins must be explicit. "*" together with AllowCredentials is rejected
// at compile time, because browsers reject it anyway and an author who wrote it
// has misunderstood what they are enabling.
type CORSSpec struct {
	AllowOrigins     []string `bcl:"allow_origins"`
	AllowMethods     []string `bcl:"allow_methods"`
	AllowHeaders     []string `bcl:"allow_headers"`
	ExposeHeaders    []string `bcl:"expose_headers"`
	AllowCredentials bool     `bcl:"allow_credentials"`
	MaxAge           Duration `bcl:"max_age"`
}
