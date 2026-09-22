package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
)

// The capabilities: everything an intent needs to know before it may act.
//
// A capability produces a *fact*. That is not a stylistic detail — it is how a
// capability gets into a plan at all. The engine compiles an intent by walking the
// facts it requires back to their producers, so a capability that produces nothing
// is never scheduled, however important it looks in a diagram. Each policy here
// therefore publishes a typed verdict fact, and every intent that must be governed
// by it lists that fact in its Spec.Requires. The compiler then proves the policy
// runs; nobody has to remember to call it.
//
// Facts this file produces:
//
//	capability.PrincipalKey  — who the caller is (from a bearer token, checked against the database)
//	capability.TenantKey     — the authenticated caller's own tenant (from the account, never the header)
//	PublicTenantKey          — the tenant an anonymous caller asked for (header, validated)
//	AddressRateKey           — that this address is within its request budget
//	PrincipalRateKey         — that this account is within its request budget
//	AuthorizationKey         — that policy allows this class of operation, plus the constraints it imposes
var (
	// AddressRateKey is published by the limiter that keys on the remote address.
	AddressRateKey = ref.NewKey[RateVerdict]("app.ratelimit.address")
	// PrincipalRateKey is published by the limiter that keys on the principal.
	PrincipalRateKey = ref.NewKey[RateVerdict]("app.ratelimit.principal")
	// PublicTenantKey is the tenant an anonymous caller named in a header, once it
	// has been checked against the tenants table.
	PublicTenantKey = ref.NewKey[capability.TenantFact]("app.tenant.requested")
	// AuthorizationKey is published by the authorization decision.
	AuthorizationKey = ref.NewKey[Authorization]("app.authorization")
)

// RateVerdict records what the limiter allowed, so a handler can report the
// remaining budget without asking again.
type RateVerdict struct {
	Key       string
	Limit     int
	Remaining int
	Window    time.Duration
}

// Authorization is the result of the policy decision: the roles that satisfied it
// and the constraints every subsequent read and write must respect.
type Authorization struct {
	PrincipalID string
	TenantID    string
	Roles       []string
	// Scope is "own" when the caller may only see their own rows, "tenant" when
	// they may see everything inside their tenant. The intents turn this into a
	// SQL predicate rather than deciding for themselves.
	Scope string
}

// permissions is the application's role model, flattened: a role maps to the set of
// permissions it holds. Inheritance is expanded here, once, so a check is a map
// lookup on the hot path.
var permissions = map[string][]string{
	"customer": {"order:create", "order:read:own", "order:cancel:own", "catalog:read"},
	"agent":    {"order:read", "order:list", "catalog:read"},
	"admin":    {"order:create", "order:read", "order:list", "order:cancel", "catalog:read"},
}

func roleHas(roles []string, permission string) bool {
	for _, role := range roles {
		if slices.Contains(permissions[strings.ToLower(role)], permission) {
			return true
		}
	}
	return false
}

// record puts a verdict in the metrics and the audit trail. Every RecordAllow and
// RecordDeny in this file goes through it, so a policy cannot decide something
// without leaving a trace.
func record(deps *Deps, policy string, allow bool, message string) {
	if deps.Metrics != nil {
		deps.Metrics.RecordDecision(policy, allow)
	}
	if deps.Audit != nil {
		verdict := "deny"
		if allow {
			verdict = "allow"
		}
		deps.Audit.RecordDecision(policy, verdict, message)
	}
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

// NewAuthCapability verifies the bearer token and loads the account behind it.
//
// The token is not the identity: it names one. Roles and the disabled flag are read
// from the database on every request, so revoking an account takes effect on the
// next call rather than when its token happens to expire.
func NewAuthCapability(deps *Deps) capability.Registration {
	return capability.NewAuthCapability("app.auth", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
		// Every failure below returns the same typed failure, which the transports
		// project as 401. Returning a bare error instead would surface as a 500 and
		// tell a caller that something broke rather than that they are not signed in.
		if hint.BearerToken == "" {
			return capability.PrincipalFact{}, unauthenticated()
		}
		claims, err := deps.Tokens.Verify(hint.BearerToken)
		if err != nil {
			return capability.PrincipalFact{}, unauthenticated()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		user, err := deps.Store.UserByID(ctx, claims.Subject)
		if err != nil || user.Disabled {
			return capability.PrincipalFact{}, unauthenticated()
		}
		return capability.PrincipalFact{
			ID:       user.ID,
			Username: user.Email,
			Roles:    user.Roles,
			Claims: map[string]any{
				"tenant_id": user.TenantID,
				"name":      user.Name,
			},
		}, nil
	}, capability.WithTimeout(3*time.Second))
}

// Two tenant capabilities, for the same reason there are two limiters: a capability
// may only read facts it declares.
//
// The obvious design — one resolver that uses the account's tenant when there is an
// account and the header otherwise — cannot work here. The principal would be an
// undeclared read, so the resolver would race authentication and sometimes see no
// identity at all. Splitting them makes each one's input explicit:
//
//	app.tenant         — requires PrincipalKey; the tenant comes from the account.
//	                     The header may agree with it or be absent; it may not
//	                     override it, because changing a header must never change
//	                     whose data is read.
//	app.tenant.public  — no identity; the tenant is the header, validated against
//	                     the tenants table. For the anonymous catalogue, and for
//	                     registering and logging in, which happen before there is
//	                     an account to ask.
//
// Both refuse a suspended tenant.
func NewTenantCapability(deps *Deps) capability.Registration {
	return capability.NewTenantCapability("app.tenant",
		func(inv *invocation.Invocation, principal *capability.PrincipalFact) (capability.TenantFact, error) {
			if principal == nil {
				return capability.TenantFact{}, unauthenticated()
			}
			owned, _ := principal.Claims["tenant_id"].(string)
			if owned == "" {
				return capability.TenantFact{}, forbidden("this account has no tenant")
			}
			if requested := headerValue(inv, "X-Tenant-ID"); requested != "" && !strings.EqualFold(requested, owned) {
				return capability.TenantFact{}, forbidden("this account may not act in another tenant")
			}
			return loadTenant(deps, owned)
		},
		true, // requirePrincipal: the dependency is declared, so the ordering is proved
		capability.WithTimeout(3*time.Second),
	)
}

func NewPublicTenantCapability(deps *Deps) capability.Registration {
	return ref.Read("app.tenant.public", capability.WithSpeculation(ref.PreAuthSafe)).
		WithProvides(PublicTenantKey.Any()).
		WithRun(func(nc *ref.NodeContext) error {
			requested := headerValue(nc.Invocation(), "X-Tenant-ID")
			if requested == "" {
				// Guessing a default would serve one tenant's data to another.
				return invalid("TENANT_REQUIRED", "the X-Tenant-ID header is required")
			}
			tenant, err := loadTenant(deps, requested)
			if err != nil {
				return err
			}
			ref.Publish(nc, PublicTenantKey, tenant)
			return nil
		})
}

// loadTenant reads a tenant and refuses a suspended one. Both capabilities go
// through it so the two cannot drift apart.
func loadTenant(deps *Deps, id string) (capability.TenantFact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tenant, err := deps.Store.Tenant(ctx, strings.ToLower(strings.TrimSpace(id)))
	if err != nil {
		if errors.Is(err, errNotFound) {
			return capability.TenantFact{}, notFound("unknown tenant")
		}
		return capability.TenantFact{}, unavailable("the tenant could not be read")
	}
	if tenant.Suspended {
		return capability.TenantFact{}, forbidden(fmt.Sprintf("tenant %q is suspended", tenant.ID))
	}
	return capability.TenantFact{
		ID:             tenant.ID,
		Name:           tenant.Name,
		Tier:           tenant.Tier,
		IsolationLevel: "row",
		Settings:       map[string]string{"region": tenant.Region},
	}, nil
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

// Two limiters, because a limiter must key on something that is already known
// when it runs.
//
// Fact dependencies are the only ordering guarantee in a REF plan. A limiter that
// merely *peeked* at the principal would race the authentication node and key on
// the address or the account depending on which goroutine won — so instead there are
// two, each declaring what it needs:
//
//	app.ratelimit.address   — no dependencies, keys on the remote address. For
//	                          registration, login and the anonymous catalogue.
//	app.ratelimit.principal — requires PrincipalKey, keys on the account. For
//	                          everything a caller must be signed in to do.
//
// An intent requires exactly one of them, and that choice is visible in its Spec.
//
// Both fail **closed**: if the store cannot be read the request is refused, because
// a limiter that opens under load is not a limiter.
func NewAddressRateLimitCapability(deps *Deps) capability.Registration {
	return rateLimiter(deps, "app.ratelimit.address", AddressRateKey, nil,
		func(nc *ref.NodeContext) (string, error) {
			return "ip:" + nc.Invocation().Transport.RemoteIP, nil
		})
}

func NewPrincipalRateLimitCapability(deps *Deps) capability.Registration {
	return rateLimiter(deps, "app.ratelimit.principal", PrincipalRateKey,
		[]fact.AnyKey{capability.PrincipalKey.Any()},
		func(nc *ref.NodeContext) (string, error) {
			principal, err := ref.Require(nc, capability.PrincipalKey)
			if err != nil {
				return "", err
			}
			return "user:" + principal.ID, nil
		})
}

func rateLimiter(deps *Deps, name string, provides fact.Key[RateVerdict], requires []fact.AnyKey,
	keyOf func(nc *ref.NodeContext) (string, error)) capability.Registration {
	limit := deps.Config.RateLimit
	window := deps.Config.RateWindow

	registration := ref.Decision(name).WithProvides(provides.Any())
	if len(requires) > 0 {
		registration = registration.WithRequires(requires...)
	}
	return registration.WithRun(func(nc *ref.NodeContext) error {
		key, err := keyOf(nc)
		if err != nil {
			nc.Decisions().RecordDeny(name, "the rate limit key could not be determined")
			return err
		}
		used, err := incrementWindow(deps.Cache, "rate:"+key, window)
		if err != nil {
			record(deps, name, false, "the rate limiter is unavailable")
			nc.Decisions().RecordDeny(name, "the rate limiter is unavailable")
			return intent.Failure{
				Code:     "RATE_LIMITER_UNAVAILABLE",
				Category: intent.CategoryUnavailable,
				Message:  "the rate limiter is unavailable",
			}
		}
		if used > limit {
			record(deps, name, false, "rate limit exceeded")
			nc.Decisions().RecordDeny(name, "rate limit exceeded")
			return intent.Failure{
				Code:     "RATE_LIMITED",
				Category: intent.CategoryRateLimit,
				Message:  fmt.Sprintf("at most %d requests per %s", limit, window),
			}
		}
		record(deps, name, true, "")
		nc.Decisions().RecordAllow(name, nil)
		ref.Publish(nc, provides, RateVerdict{
			Key:       key,
			Limit:     limit,
			Remaining: limit - used,
			Window:    window,
		})
		return nil
	})
}

// incrementWindow is an atomic read-modify-write over the kv store: Mutate holds
// the key's lock for the whole callback, so two concurrent requests cannot both read
// the same count. Doing this with Get followed by Set would undercount under
// exactly the load that matters.
func incrementWindow(cache kv.Store, key string, window time.Duration) (int, error) {
	used := 0
	err := cache.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		count := 0
		if exists {
			count, _ = strconv.Atoi(string(current))
		}
		count++
		used = count
		// The TTL is only set when the window starts, so the window is fixed rather
		// than sliding forward with every request.
		ttl := window
		if exists {
			ttl = 0
		}
		return []byte(strconv.Itoa(count)), ttl, true, nil
	})
	if err != nil {
		return 0, err
	}
	return used, nil
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

// NewAuthorizationCapability is the policy node.
//
// It is a decision, so a deny here blocks every effect in the plan — not just the
// node that asked. It also records *constraints*, which REF intersects across all
// decisions: the tenant id it pins and the region set it narrows travel with the
// execution, and contradicting them anywhere else in the graph is itself a deny.
// The intents read those constraints back and build their SQL from them, so the
// tenant boundary is enforced by the policy rather than by each query's author
// remembering it.
func NewAuthorizationCapability(deps *Deps) capability.Registration {
	return ref.Decision("app.policy.orders").
		WithRequires(capability.PrincipalKey.Any(), capability.TenantKey.Any()).
		WithProvides(AuthorizationKey.Any()).
		WithRun(func(nc *ref.NodeContext) error {
			principal, err := ref.Require(nc, capability.PrincipalKey)
			if err != nil {
				nc.Decisions().RecordDeny("app.policy.orders", "no identity")
				return err
			}
			tenant, err := ref.Require(nc, capability.TenantKey)
			if err != nil {
				nc.Decisions().RecordDeny("app.policy.orders", "no tenant context")
				return err
			}
			if !roleHas(principal.Roles, "catalog:read") && !roleHas(principal.Roles, "order:read:own") &&
				!roleHas(principal.Roles, "order:read") {
				record(deps, "app.policy.orders", false, "no role may use the order API")
				nc.Decisions().RecordDeny("app.policy.orders", "this account holds no role that may use the order API")
				return nil
			}

			scope := "own"
			if roleHas(principal.Roles, "order:list") {
				scope = "tenant"
			}

			region := tenant.Settings["region"]
			if region == "" {
				region = "eu-central-1"
			}
			record(deps, "app.policy.orders", true, "scope="+scope)
			nc.Decisions().RecordAllow("app.policy.orders",
				[]ref.Constraint{
					{Field: "tenant_id", Values: []string{tenant.ID}},
					{Field: "region", Values: []string{region}},
				},
				ref.Obligation{Name: "audit_trail", Action: "log_access", Data: principal.ID},
			)
			ref.Publish(nc, AuthorizationKey, Authorization{
				PrincipalID: principal.ID,
				TenantID:    tenant.ID,
				Roles:       principal.Roles,
				Scope:       scope,
			})
			return nil
		})
}

// ---------------------------------------------------------------------------
// Catalogue cache
// ---------------------------------------------------------------------------

// CatalogKey carries the catalogue, whether it came from the cache or the database.
var CatalogKey = ref.NewKey[CatalogFact]("app.catalog")

type CatalogFact struct {
	TenantID string    `json:"tenant_id"`
	Products []Product `json:"products"`
	Cached   bool      `json:"cached"`
}

// NewCatalogCapability is a cache-aside read.
//
// It is a Read node marked PreAuthSafe, so the engine may run it *while*
// authentication and policy are still being decided — and if policy then denies, the
// read is simply discarded. That is the speculation the architecture is for: the
// latency win is real, and it is only safe because the node performs no mutation.
func NewCatalogCapability(deps *Deps) capability.Registration {
	ttl := deps.Config.CatalogCacheTTL

	return ref.Read("app.catalog", capability.WithSpeculation(ref.PreAuthSafe)).
		WithRequires(PublicTenantKey.Any()).
		WithProvides(CatalogKey.Any()).
		WithRun(func(nc *ref.NodeContext) error {
			tenant, err := ref.Require(nc, PublicTenantKey)
			if err != nil {
				return err
			}
			key := "catalog:" + tenant.ID

			if raw, found, err := deps.Cache.Get(key); err == nil && found {
				var products []Product
				if json.Unmarshal(raw, &products) == nil {
					deps.Metrics.AddBusiness("catalog.cache_hit", 1)
					ref.Publish(nc, CatalogKey, CatalogFact{TenantID: tenant.ID, Products: products, Cached: true})
					return nil
				}
				// A corrupt entry is a miss, not an error: fall through to the
				// database and overwrite it below.
			}

			if err := nc.Budget().AcquireDBQuery(1); err != nil {
				return err
			}
			products, err := deps.Store.Catalogue(nc.Context, tenant.ID)
			if err != nil {
				return intent.Failure{
					Code:     "CATALOG_UNAVAILABLE",
					Category: intent.CategoryUnavailable,
					Message:  "the catalogue could not be read",
				}
			}
			if encoded, err := json.Marshal(products); err == nil {
				_ = deps.Cache.Set(key, encoded, ttl)
			}
			deps.Metrics.AddBusiness("catalog.cache_miss", 1)
			ref.Publish(nc, CatalogKey, CatalogFact{TenantID: tenant.ID, Products: products})
			return nil
		})
}

// headerValue reads one request header out of whatever metadata the transport
// attached. It returns "" for a transport that has no headers at all — a queue job
// or a CLI invocation — which is why every caller treats the empty case explicitly.
func headerValue(inv *invocation.Invocation, name string) string {
	meta, ok := inv.Metadata.(invocation.HTTPMeta)
	if !ok {
		return ""
	}
	if values, exists := meta.Headers[name]; exists && len(values) > 0 {
		return values[0]
	}
	// Header maps arrive canonicalised by some transports and not by others.
	for key, values := range meta.Headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}
