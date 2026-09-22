package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/platform/spi"
	"github.com/oarkflow/fh/ref/process"
)

// The HTTP layer.
//
// Every guard a route declares is enforced here, before dispatch, in a fixed order,
// and every one of them fails closed:
//
//  1. CORS preflight
//  2. Body limit
//  3. Tenant resolution
//  4. Authentication
//  5. Authorization
//  6. Rate limit
//  7. Idempotency
//  8. Dispatch
//  9. Response shaping
//  10. Audit
//
// The order is not arbitrary. Authentication precedes authorization because a gate
// needs an identity; the rate limit follows both so a throttle can be keyed on the
// principal rather than only the address; idempotency is last before dispatch so a
// replayed request that would have been rejected still is.

// compiledRoute is one route with everything resolved at load time.
type compiledRoute struct {
	spec RouteSpec

	session   *session.SessionManager
	auth      spi.Authenticator
	authz     *authzGate
	limiter   spi.RateLimiter
	limitKey  *Expression
	idemStore cacheHandle
	idemScope *Expression
	queue     spi.JobQueue
	engine    *process.Engine

	requestPipeline  *DataPipeline
	responsePipeline *DataPipeline
	auditPipeline    *DataPipeline

	status  int
	mode    string
	params  []string
	timeout time.Duration
	// limitWindow, idemTTL and corsMaxAge are the parsed forms of the spec's
	// duration strings, resolved once at load time.
	limitWindow time.Duration
	idemTTL     time.Duration
	corsMaxAge  time.Duration
}

// requestIdentity is what the route resolved before dispatch, carried into every
// node so an action never re-derives it.
type requestIdentityKey struct{}

type requestIdentity struct {
	principal Principal
	tenant    string
}

func withRequestIdentity(ctx context.Context, principal Principal, tenant string) context.Context {
	return context.WithValue(ctx, requestIdentityKey{}, requestIdentity{principal: principal, tenant: tenant})
}

func requestIdentityFrom(ctx context.Context) (requestIdentity, bool) {
	identity, ok := ctx.Value(requestIdentityKey{}).(requestIdentity)
	return identity, ok
}

// compileRoutes resolves every route's resources and pipelines.
func (p *Platform) compileRoutes(doc Document) error {
	build := p.buildContext()
	for i := range doc.Routes {
		spec := doc.Routes[i]
		what := "route " + spec.Name
		timeout, err := durationField(what, "timeout", spec.Timeout, 0)
		if err != nil {
			return err
		}
		route := compiledRoute{
			spec:    spec,
			status:  spec.Status,
			mode:    strings.ToLower(strings.TrimSpace(spec.Mode)),
			params:  pathParams(spec.Path),
			timeout: timeout,
		}
		if route.status == 0 {
			route.status = 200
		}
		if route.mode == "" {
			route.mode = "sync"
		}

		if spec.Session != "" {
			manager, ok := p.resources[spec.Session].(*session.SessionManager)
			if !ok {
				return fmt.Errorf("ref/platform: %s: resource %q is not a session manager", what, spec.Session)
			}
			route.session = manager
		}
		if spec.Auth != "" {
			authenticator, ok := asAuthenticator(p.resources[spec.Auth])
			if !ok {
				return fmt.Errorf("ref/platform: %s: resource %q is not an authenticator", what, spec.Auth)
			}
			route.auth = authenticator
		}
		if spec.Process != "" {
			engine, ok := p.processes[spec.Process]
			if !ok {
				return fmt.Errorf("ref/platform: %s: process %q has no engine", what, spec.Process)
			}
			route.engine = engine
		}
		if route.mode == "async" {
			queue, ok := p.resources[spec.Queue].(spi.JobQueue)
			if !ok {
				return fmt.Errorf("ref/platform: %s: resource %q is not a queue", what, spec.Queue)
			}
			route.queue = queue
		}

		gate, err := compileAuthz(build, what, spec.Authz)
		if err != nil {
			return err
		}
		route.authz = gate
		if spec.CORS != nil {
			if route.corsMaxAge, err = durationField(what, "cors max_age", spec.CORS.MaxAge, 0); err != nil {
				return err
			}
		}

		if spec.RateLimit != nil {
			limiter, err := p.resolveLimiter(spec.RateLimit.Limiter)
			if err != nil {
				return fmt.Errorf("ref/platform: %s: %w", what, err)
			}
			route.limiter = limiter
			if route.limitWindow, err = durationField(what, "rate_limit window", spec.RateLimit.Window, 0); err != nil {
				return err
			}
			// The default key is the principal when the route authenticates and the
			// remote address when it does not, which is what an author means by "limit
			// per caller" in both cases.
			keySource := spec.RateLimit.Key
			if keySource == "" {
				if spec.Auth != "" || spec.Session != "" {
					keySource = `principal.id != "" ? principal.id : remote_ip`
				} else {
					keySource = "remote_ip"
				}
			}
			if route.limitKey, err = CompileExpr(keySource); err != nil {
				return fmt.Errorf("ref/platform: %s rate_limit key: %w", what, err)
			}
		}
		if spec.Idempotency != nil {
			store, err := p.resolveIdempotencyStore(spec.Idempotency.Store)
			if err != nil {
				return fmt.Errorf("ref/platform: %s: %w", what, err)
			}
			route.idemStore = store
			if route.idemScope, err = CompileExpr(spec.Idempotency.Scope); err != nil {
				return fmt.Errorf("ref/platform: %s idempotency scope: %w", what, err)
			}
			if route.idemTTL, err = durationField(what, "idempotency ttl", spec.Idempotency.TTL, 24*time.Hour); err != nil {
				return err
			}
		}

		if route.requestPipeline, err = compileDataSpec(what+" request_data", spec.RequestData, p.schemas); err != nil {
			return err
		}
		if route.responsePipeline, err = compileDataSpec(what+" response_data", spec.ResponseData, p.schemas); err != nil {
			return err
		}
		if spec.Audit != nil {
			auditSpec := &DataSpec{
				Redact: append(slices.Clone(spec.Audit.Redact), alwaysRedacted...),
				Mask:   spec.Audit.Mask,
			}
			if route.auditPipeline, err = compileDataSpec(what+" audit", auditSpec, p.schemas); err != nil {
				return err
			}
		}
		p.routes = append(p.routes, route)
	}
	return nil
}

// resolveLimiter finds the named limiter, or the application's only one.
func (p *Platform) resolveLimiter(name string) (spi.RateLimiter, error) {
	if name != "" {
		limiter, ok := p.resources[name].(spi.RateLimiter)
		if !ok {
			return nil, fmt.Errorf("resource %q is not a rate limiter", name)
		}
		return limiter, nil
	}
	var found spi.RateLimiter
	for _, resource := range p.resources {
		limiter, ok := resource.(spi.RateLimiter)
		if !ok {
			continue
		}
		if found != nil {
			// Two limiters and no name: refuse to guess which policy applies.
			return nil, errors.New("this application declares several rate limiters, so rate_limit must name one")
		}
		found = limiter
	}
	if found == nil {
		return nil, errors.New("rate limiting needs a limiter resource (ratelimit.store or ratelimit.memory)")
	}
	return found, nil
}

// resolveIdempotencyStore finds the named cache, or the application's only one.
func (p *Platform) resolveIdempotencyStore(name string) (cacheHandle, error) {
	if name != "" {
		resource, ok := p.resources[name]
		if !ok {
			return cacheHandle{}, fmt.Errorf("idempotency store %q is not declared", name)
		}
		return wrapCache(name, resource)
	}
	var (
		found cacheHandle
		count int
	)
	for candidate, resource := range p.resources {
		handle, err := wrapCache(candidate, resource)
		if err != nil {
			continue
		}
		found = handle
		count++
	}
	if count != 1 {
		return cacheHandle{}, errors.New("an idempotency guard needs a cache resource; name one with idempotency.store")
	}
	return found, nil
}

func wrapCache(name string, resource Resource) (cacheHandle, error) {
	handle := cacheHandle{name: name}
	if typed, ok := resource.(spi.CacheContext); ok {
		handle.withContext = typed
	}
	if typed, ok := resource.(spi.Cache); ok {
		handle.plain = typed
	}
	if handle.plain == nil && handle.withContext == nil {
		return cacheHandle{}, fmt.Errorf("resource %q is not a cache", name)
	}
	handle.prefixed, _ = resource.(spi.CachePrefix)
	return handle, nil
}

// ---------------------------------------------------------------------------
// Mounting
// ---------------------------------------------------------------------------

// Mount registers every configured HTTP route. Mount must run before Listen.
func (p *Platform) Mount(app *fh.App) error {
	if app == nil {
		return fmt.Errorf("ref/platform: nil fh app")
	}
	for i := range p.routes {
		route := p.routes[i]
		app.Add(route.spec.Method, route.spec.Path, func(c fh.Ctx) error {
			return p.serve(c, route)
		}).Name(route.spec.Name)
	}
	if err := p.mountTriggers(app); err != nil {
		return err
	}
	return nil
}

// serve runs one request through the guards and the dispatch.
func (p *Platform) serve(c fh.Ctx, route compiledRoute) error {
	if route.spec.CORS != nil {
		if handled := applyCORS(c, route.spec.CORS, route.corsMaxAge); handled {
			return nil
		}
	}
	if route.spec.MaxBodyBytes > 0 && int64(len(c.Body())) > route.spec.MaxBodyBytes {
		return projectFailure(c, intent.Failure{Code: "PAYLOAD_TOO_LARGE", Category: intent.CategoryInvalidInput,
			Message: fmt.Sprintf("the request body is larger than the %d byte limit", route.spec.MaxBodyBytes)})
	}

	ctx := c.Context()
	if route.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, route.timeout)
		defer cancel()
	}
	ctx = context.WithValue(ctx, httpContextKey{}, c)

	// A session is begun before authentication so a session-backed authenticator
	// has one to read, and committed after the response so a login's rotation
	// reaches the client.
	sessionID := ""
	if route.session != nil {
		sess, complete, err := route.session.Begin(c)
		if err != nil {
			return projectFailure(c, err)
		}
		c.OnBeforeResponse(complete)
		ctx = context.WithValue(ctx, sessionContextKey{}, sess)
		sessionID = sess.ID
	}
	c.SetContext(ctx)

	principal, err := p.authenticate(ctx, c, route, sessionID)
	if err != nil {
		return projectFailure(c, err)
	}
	tenant, err := resolveTenant(c, route, principal)
	if err != nil {
		return projectFailure(c, err)
	}
	ctx = withRequestIdentity(ctx, principal, tenant)
	c.SetContext(ctx)

	env := requestEnv(c, principal, tenant)

	if route.authz != nil {
		actionCtx := &ActionContext{Context: ctx, Principal: principal, TenantID: tenant, Now: time.Now().UTC()}
		allowed, message, err := route.authz.evaluate(actionCtx, env)
		if err != nil {
			return projectFailure(c, unavailable("the authorization check failed"))
		}
		if !allowed {
			return projectFailure(c, permissionDenied(message))
		}
	}

	if route.limiter != nil {
		if err := p.enforceRateLimit(ctx, c, route, env); err != nil {
			return projectFailure(c, err)
		}
	}

	// An idempotency guard both short-circuits a replay and records the response, so
	// its key is computed once here and reused after dispatch.
	idemKey := ""
	if route.spec.Idempotency != nil {
		key, replayed, err := p.checkIdempotency(ctx, c, route, env)
		switch {
		case err != nil:
			return projectFailure(c, err)
		case replayed:
			return nil
		}
		idemKey = key
	}

	body := c.Body()
	if route.requestPipeline != nil {
		shaped, err := p.shapeRequest(route, body, env)
		if err != nil {
			return projectFailure(c, err)
		}
		body = shaped
	}

	switch {
	case route.mode == "async":
		return p.serveAsync(c, route, body, principal, tenant, sessionID)
	case route.spec.Process != "":
		return p.serveProcess(ctx, c, route, body, principal, tenant, idemKey)
	case route.mode == "stream":
		return p.serveStream(ctx, c, route, body, principal, tenant, sessionID)
	default:
		return p.serveSync(ctx, c, route, body, principal, tenant, sessionID, idemKey, env)
	}
}

// authenticate resolves the caller, failing closed unless the route opted into
// anonymous access.
func (p *Platform) authenticate(ctx context.Context, c fh.Ctx, route compiledRoute, sessionID string) (Principal, error) {
	if route.auth == nil {
		// No authenticator: a session's own subject is the identity, if there is one.
		if sess, ok := sessionFromContext(ctx); ok {
			if subject := sess.Get("user_id"); subject != nil && Stringify(subject) != "" {
				principal := Principal{ID: Stringify(subject), Username: Stringify(sess.Get("username"))}
				principal.TenantID = Stringify(sess.Get("tenant_id"))
				principal.Roles = stringSlice(sess.Get("roles"))
				return principal, nil
			}
		}
		return Principal{}, nil
	}

	creds := Credentials{
		BearerToken: extractBearer(c.Get("Authorization")),
		APIKey:      c.Get("X-API-Key"),
		SessionID:   sessionID,
		RemoteIP:    c.IP(),
		TLS:         c.Protocol() == "https",
	}
	if username, password, ok := basicCredentials(c.Get("Authorization")); ok {
		creds.Username, creds.Password = username, password
	}

	principal, err := route.auth.Authenticate(ctx, creds)
	if err != nil {
		if route.spec.AllowAnonymous {
			// The route said anonymous access is acceptable. That is an explicit
			// decision, not a fallback the platform chose.
			return Principal{}, nil
		}
		return Principal{}, errUnauthenticated
	}
	return principal, nil
}

// resolveTenant determines the request's tenant: the principal's own claim first,
// then a configured claim or header.
func resolveTenant(c fh.Ctx, route compiledRoute, principal Principal) (string, error) {
	spec := route.spec.Tenant
	if spec == nil {
		return principal.TenantID, nil
	}
	if principal.TenantID != "" {
		return principal.TenantID, nil
	}
	if spec.Claim != "" && principal.Claims != nil {
		if value, found := lookupPath(principal.Claims, strings.Split(spec.Claim, ".")); found {
			if text := Stringify(value); text != "" {
				return text, nil
			}
		}
	}
	if spec.Header != "" {
		if value := c.Get(spec.Header); value != "" {
			return value, nil
		}
	}
	if spec.Default != "" {
		return spec.Default, nil
	}
	if spec.Required {
		// An unresolved tenant on a tenant-scoped route is a cross-tenant read
		// waiting to happen, so it is refused rather than defaulted.
		return "", permissionDenied("this request needs a tenant, and none could be determined")
	}
	return "", nil
}

// requestEnv is the expression environment the route-level guards see.
func requestEnv(c fh.Ctx, principal Principal, tenant string) Env {
	return Env{
		"principal": principalMap(principal),
		"tenant":    tenant,
		"remote_ip": c.IP(),
		"method":    c.Method(),
		"path":      c.Path(),
		"now":       time.Now().UTC().Format(time.RFC3339),
	}
}

// enforceRateLimit consumes a token and sets the standard headers.
func (p *Platform) enforceRateLimit(ctx context.Context, c fh.Ctx, route compiledRoute, env Env) error {
	key, err := route.limitKey.String(env)
	if err != nil {
		return unavailable("the rate-limit key could not be evaluated")
	}
	if key == "" {
		// An empty key would pool every caller into one bucket, throttling everybody
		// or nobody. Neither is the configured intent.
		return unavailable("the rate-limit key evaluated to empty")
	}
	limit := route.spec.RateLimit.Limit + route.spec.RateLimit.Burst
	allowed, remaining, resetAt, err := route.limiter.Allow(ctx, "route:"+route.spec.Name+":"+key, limit, route.limitWindow)
	if err != nil {
		// Fail closed: a rate limit exists to protect something, and this is when it
		// matters most.
		return unavailable("the rate limiter is unavailable")
	}
	c.Set("X-RateLimit-Limit", strconv.Itoa(limit))
	c.Set("X-RateLimit-Remaining", strconv.Itoa(max(remaining, 0)))
	if !resetAt.IsZero() {
		c.Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
	}
	if !allowed {
		if !resetAt.IsZero() {
			c.Set("Retry-After", strconv.Itoa(max(int(time.Until(resetAt).Seconds()), 1)))
		}
		return rateLimited(route.spec.RateLimit.Message)
	}
	return nil
}

// idempotencyResponse is what a guarded route stores for a replay.
type idempotencyResponse struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// checkIdempotency short-circuits a replay, returning the key to record under.
func (p *Platform) checkIdempotency(ctx context.Context, c fh.Ctx, route compiledRoute, env Env) (string, bool, error) {
	spec := route.spec.Idempotency
	header := spec.Header
	if header == "" {
		header = "Idempotency-Key"
	}
	presented := c.Get(header)
	if presented == "" {
		if spec.Required {
			return "", false, invalidInput("this request needs an %s header", header)
		}
		return "", false, nil
	}
	if len(presented) > 200 {
		return "", false, invalidInput("the %s header is too long", header)
	}

	// The scope is mixed into the key so two tenants cannot collide on the same
	// client-chosen value.
	scope := ""
	if route.idemScope != nil {
		resolved, err := route.idemScope.String(env)
		if err != nil {
			return "", false, unavailable("the idempotency scope could not be evaluated")
		}
		scope = resolved
	}
	key := "idem:" + route.spec.Name + ":" + scope + ":" + presented

	raw, found, err := route.idemStore.get(ctx, key)
	if err != nil {
		// "I cannot tell whether this already ran" must not become "run it again"
		// for a guarded mutation.
		return "", false, unavailable("the idempotency store is unavailable, so this request cannot be safely processed")
	}
	if found && len(raw) > 0 {
		var stored idempotencyResponse
		if json.Unmarshal(raw, &stored) == nil && stored.Status > 0 {
			c.Set("Idempotent-Replay", "true")
			return key, true, c.Status(stored.Status).Send(stored.Body)
		}
		// A record we cannot read means the original is still in flight or the entry
		// is corrupt. Reporting a conflict is safer than running the mutation again.
		return key, true, projectFailure(c, conflict("a request with this idempotency key is already being processed"))
	}

	// Claim the key before doing the work, so a concurrent duplicate sees the claim.
	claim, _ := json.Marshal(idempotencyResponse{})
	if err := route.idemStore.set(ctx, key, claim, route.idemTTL); err != nil {
		return "", false, unavailable("the idempotency key could not be claimed")
	}
	return key, false, nil
}

// recordIdempotency stores a successful response for replay.
func (p *Platform) recordIdempotency(ctx context.Context, route compiledRoute, key string, status int, body []byte) {
	if key == "" {
		return
	}
	encoded, err := json.Marshal(idempotencyResponse{Status: status, Body: body})
	if err != nil {
		return
	}
	// A failure here means a future replay re-runs the work. That is a worse outcome
	// than a duplicate record, but a better one than failing a request that already
	// succeeded.
	_ = route.idemStore.set(ctx, key, encoded, route.idemTTL)
}

// shapeRequest applies the route's request pipeline to the raw body.
func (p *Platform) shapeRequest(route compiledRoute, body []byte, env Env) ([]byte, error) {
	var value any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &value); err != nil {
			return nil, invalidInput("the request body is not valid JSON")
		}
	}
	shaped, err := route.requestPipeline.Apply(value, env)
	if err != nil {
		if errors.Is(err, ErrDataFiltered) {
			return nil, invalidInput("the request was rejected by this route's filter")
		}
		return nil, err
	}
	return json.Marshal(shaped)
}

// ---------------------------------------------------------------------------
// Dispatch modes
// ---------------------------------------------------------------------------

// buildInvocation assembles the REF invocation for a request.
//
// It deliberately does not take the resolved principal: the invocation carries a
// *hint* — the raw credentials — and the identity the route already resolved
// travels in the request context instead, which is where actionContext reads it
// from. Passing both would give a node two sources for one answer.
func (p *Platform) buildInvocation(c fh.Ctx, route compiledRoute, body []byte, sessionID string) *invocation.Invocation {
	return &invocation.Invocation{
		ID:     invocation.ID(orRequestID(c.Get("X-Request-ID"))),
		Intent: invocation.IntentID(route.spec.Intent),
		Input:  invocation.NewInput(body, orDefault(c.Get("Content-Type"), "application/json")),
		Principal: invocation.NewPrincipalHint(
			extractBearer(c.Get("Authorization")), c.Get("X-API-Key"), nil, sessionID),
		Metadata: invocation.NewHTTPMeta(
			c.Method(), c.Path(), route.spec.Path, c.Hostname(),
			c.GetReqHeaders(), queryValues(c.OriginalURL()), routeParams(route.spec.Path, c.Path()),
		),
		Transport: invocation.Transport{Protocol: "http", RemoteIP: c.IP(), TLS: c.Protocol() == "https"},
		Received:  time.Now(),
	}
}

func (p *Platform) serveSync(ctx context.Context, c fh.Ctx, route compiledRoute, body []byte, principal Principal, tenant, sessionID, idemKey string, env Env) error {
	inv := p.buildInvocation(c, route, body, sessionID)
	result, err := p.Engine.Dispatch(ctx, inv)
	if err != nil {
		p.audit(ctx, route, principal, tenant, body, nil, err)
		return projectFailure(c, err)
	}

	value := result.Value
	if route.responsePipeline != nil {
		shaped, shapeErr := route.responsePipeline.Apply(value, env)
		if shapeErr != nil && !errors.Is(shapeErr, ErrDataFiltered) {
			return projectFailure(c, shapeErr)
		}
		if shapeErr == nil {
			value = shaped
		}
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return projectFailure(c, fmt.Errorf("the response could not be serialised: %w", err))
	}
	p.applyResponseHeaders(c, route)
	p.recordIdempotency(ctx, route, idemKey, route.status, encoded)
	p.audit(ctx, route, principal, tenant, body, encoded, nil)
	return c.Status(route.status).Send(encoded)
}

func (p *Platform) serveAsync(c fh.Ctx, route compiledRoute, body []byte, principal Principal, tenant, sessionID string) error {
	jobType := "platform.intent." + route.spec.Intent
	if route.spec.Process != "" {
		jobType = "platform.process." + route.spec.Process
	}
	headers := map[string]string{
		"bearer_token": extractBearer(c.Get("Authorization")),
		"api_key":      c.Get("X-API-Key"),
		"session_id":   sessionID,
		"principal_id": principal.ID,
		"tenant_id":    tenant,
	}
	jobID, err := route.queue.Enqueue(jobType, json.RawMessage(body), headers)
	if err != nil {
		return projectFailure(c, unavailable("the request could not be queued"))
	}
	p.applyResponseHeaders(c, route)
	return c.Status(202).JSON(map[string]any{"job_id": jobID, "status": "queued"})
}

func (p *Platform) serveProcess(ctx context.Context, c fh.Ctx, route compiledRoute, body []byte, principal Principal, tenant, idemKey string) error {
	var input any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return projectFailure(c, invalidInput("the request body is not valid JSON"))
		}
	}
	definition, _ := route.engine.Definition(route.spec.Process)
	options := process.StartOptions{
		TenantID:      tenant,
		PrincipalID:   principal.ID,
		CorrelationID: c.Get("X-Correlation-ID"),
	}
	// The process's own idempotency path deduplicates run creation, which is what
	// makes a retried POST return the original run rather than starting a second.
	if definition != nil && definition.Idempotency != "" {
		if object, ok := input.(map[string]any); ok {
			if value, found := lookupPath(object, strings.Split(definition.Idempotency, ".")); found {
				options.IdempotencyKey = Stringify(value)
			}
		}
	}
	if options.IdempotencyKey == "" {
		options.IdempotencyKey = c.Get("Idempotency-Key")
	}

	run, err := route.engine.Start(ctx, route.spec.Process, input, options)
	if err != nil {
		p.audit(ctx, route, principal, tenant, body, nil, err)
		return projectFailure(c, processFailure(err))
	}
	payload := runView(run)
	encoded, _ := json.Marshal(payload)
	p.applyResponseHeaders(c, route)
	p.recordIdempotency(ctx, route, idemKey, route.status, encoded)
	p.audit(ctx, route, principal, tenant, body, encoded, nil)
	return c.Status(route.status).Send(encoded)
}

// serveStream projects an intent's result as a single server-sent event.
//
// It is deliberately modest: REF's stream node kind exists, but an intent that
// genuinely streams incremental results needs the engine to hand them over
// incrementally, which is a larger change than this route mode. What this gives
// you is an SSE-shaped endpoint for a client that wants one, which is honest about
// producing one event.
func (p *Platform) serveStream(ctx context.Context, c fh.Ctx, route compiledRoute, body []byte, principal Principal, tenant, sessionID string) error {
	inv := p.buildInvocation(c, route, body, sessionID)
	result, err := p.Engine.Dispatch(ctx, inv)
	if err != nil {
		return projectFailure(c, err)
	}
	encoded, err := json.Marshal(result.Value)
	if err != nil {
		return projectFailure(c, err)
	}
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	return c.Status(200).SendString("event: result\ndata: " + string(encoded) + "\n\n")
}

func (p *Platform) applyResponseHeaders(c fh.Ctx, route compiledRoute) {
	for name, value := range route.spec.Headers {
		c.Set(name, value)
	}
	if route.spec.CacheControl != "" {
		c.Set("Cache-Control", route.spec.CacheControl)
	}
}

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

// applyCORS sets the headers and answers a preflight. It reports whether the
// request was fully handled.
func applyCORS(c fh.Ctx, spec *CORSSpec, maxAge time.Duration) bool {
	origin := c.Get("Origin")
	if origin == "" {
		return false
	}
	allowed := slices.Contains(spec.AllowOrigins, "*") || slices.Contains(spec.AllowOrigins, origin)
	if !allowed {
		// An unlisted origin gets no CORS headers, which is what makes the browser
		// block it. Answering the request without them is correct.
		return false
	}
	// The concrete origin is echoed rather than "*" whenever credentials are in
	// play, because a wildcard plus credentials is rejected by every browser.
	if spec.AllowCredentials {
		c.Set("Access-Control-Allow-Origin", origin)
		c.Set("Access-Control-Allow-Credentials", "true")
		c.Set("Vary", "Origin")
	} else if slices.Contains(spec.AllowOrigins, "*") {
		c.Set("Access-Control-Allow-Origin", "*")
	} else {
		c.Set("Access-Control-Allow-Origin", origin)
		c.Set("Vary", "Origin")
	}
	if len(spec.ExposeHeaders) > 0 {
		c.Set("Access-Control-Expose-Headers", strings.Join(spec.ExposeHeaders, ", "))
	}

	if !strings.EqualFold(c.Method(), "OPTIONS") {
		return false
	}
	if len(spec.AllowMethods) > 0 {
		c.Set("Access-Control-Allow-Methods", strings.Join(spec.AllowMethods, ", "))
	}
	if len(spec.AllowHeaders) > 0 {
		c.Set("Access-Control-Allow-Headers", strings.Join(spec.AllowHeaders, ", "))
	}
	if maxAge > 0 {
		c.Set("Access-Control-Max-Age", strconv.Itoa(int(maxAge.Seconds())))
	}
	_ = c.Status(204).SendString("")
	return true
}

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

// audit records a route-level entry when the route asked for one.
//
// It never fails a request: an audit trail that can take the application down is an
// availability risk rather than a control. A write failure is logged by the
// underlying action and the request proceeds.
func (p *Platform) audit(ctx context.Context, route compiledRoute, principal Principal, tenant string, request, response []byte, cause error) {
	spec := route.spec.Audit
	if spec == nil || (spec.Enabled != nil && !*spec.Enabled) {
		return
	}
	action := spec.Action
	if action == "" {
		action = route.spec.Name
	}
	outcome := "success"
	detail := map[string]any{}
	if cause != nil {
		outcome = "failure"
		detail["error"] = cause.Error()
	}
	if spec.IncludeRequest && len(request) > 0 {
		var value any
		if json.Unmarshal(request, &value) == nil {
			if route.auditPipeline != nil {
				if shaped, err := route.auditPipeline.Apply(value, nil); err == nil {
					value = shaped
				} else {
					// A payload that could not be redacted is not recorded at all:
					// recording it unredacted would be worse than recording nothing.
					value = "[UNREDACTABLE]"
				}
			}
			detail["request"] = value
		}
	}
	if spec.IncludeResponse && len(response) > 0 {
		var value any
		if json.Unmarshal(response, &value) == nil {
			if route.auditPipeline != nil {
				if shaped, err := route.auditPipeline.Apply(value, nil); err == nil {
					value = shaped
				} else {
					value = "[UNREDACTABLE]"
				}
			}
			detail["response"] = value
		}
	}
	p.recordAudit(ctx, spec.Resource, auditRequest{
		Action:    action,
		Subject:   route.spec.Path,
		Outcome:   outcome,
		Principal: principal,
		Tenant:    tenant,
		Detail:    detail,
	})
}

// auditRequest is one route-level audit entry.
type auditRequest struct {
	Action    string
	Subject   string
	Outcome   string
	Principal Principal
	Tenant    string
	Detail    map[string]any
}

// recordAudit appends to the application's audit chain.
func (p *Platform) recordAudit(ctx context.Context, resourceName string, request auditRequest) {
	db, ok := p.auditDatabase(resourceName)
	if !ok {
		return
	}
	log := sharedAuditLog(db, "platform_audit")
	if err := log.migrate(ctx); err != nil {
		return
	}
	entry := auditEntry{
		ID:         newPrefixedID("aud"),
		RecordedAt: time.Now().UTC(),
		ActorID:    request.Principal.ID,
		ActorName:  request.Principal.Username,
		TenantID:   request.Tenant,
		Action:     request.Action,
		Subject:    request.Subject,
		Outcome:    request.Outcome,
		Stream:     orDefault(request.Tenant, "default"),
	}
	if len(request.Detail) > 0 {
		if encoded, err := json.Marshal(request.Detail); err == nil {
			entry.Detail = string(encoded)
		}
	}
	_, _ = log.append(ctx, entry)
}

// auditDatabase finds the database an audit entry goes to.
func (p *Platform) auditDatabase(name string) (*Database, bool) {
	if name != "" {
		db, ok := p.resources[name].(*Database)
		return db, ok
	}
	var (
		found *Database
		count int
	)
	for _, resource := range p.resources {
		if db, ok := resource.(*Database); ok {
			found, count = db, count+1
		}
	}
	// With several databases and no name, there is no correct guess — and an audit
	// entry in the wrong database is worse than one that was skipped and reported by
	// the compiler's own validation.
	return found, count == 1
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func extractBearer(value string) string {
	const prefix = "Bearer "
	if len(value) > len(prefix) && equalFoldASCII(value[:len(prefix)], prefix) {
		return value[len(prefix):]
	}
	return ""
}

func queryValues(rawURL string) map[string][]string {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return nil
	}
	return parsed.Query()
}

// pathParams lists the named parameters in a route pattern.
func pathParams(pattern string) []string {
	var params []string
	for _, part := range strings.Split(strings.Trim(pattern, "/"), "/") {
		if strings.HasPrefix(part, ":") && len(part) > 1 {
			params = append(params, part[1:])
		}
	}
	return params
}

// routeParams extracts a request's path parameters against its pattern.
func routeParams(pattern, path string) map[string]string {
	patternParts := strings.Split(strings.Trim(pattern, "/"), "/")
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	if len(patternParts) != len(pathParts) {
		return nil
	}
	params := make(map[string]string)
	for i, part := range patternParts {
		if strings.HasPrefix(part, ":") && len(part) > 1 {
			params[part[1:]] = pathParts[i]
		}
	}
	return params
}

func orRequestID(value string) string {
	if value != "" {
		return value
	}
	return newPrefixedID("req")
}

// runView is the client-facing shape of a process run. It deliberately excludes
// the cursor and the step states: those are operator data, and a caller placing an
// order does not need the graph's internals.
func runView(run *process.Run) map[string]any {
	view := map[string]any{
		"run_id":  run.ID,
		"process": run.Process,
		"status":  string(run.Status),
	}
	if len(run.Output) > 0 {
		var output any
		if json.Unmarshal(run.Output, &output) == nil {
			view["output"] = output
		}
	}
	if run.Error != "" {
		view["error"] = run.Error
	}
	if run.Waiting != nil {
		view["waiting"] = map[string]any{
			"reason": run.Waiting.Reason,
			"step":   run.Waiting.Step,
			"detail": run.Waiting.Detail,
			"until":  run.Waiting.Until,
		}
	}
	return view
}

// projectFailure maps a failure onto an HTTP response.
//
// The body carries a code and a message and nothing else. A stack trace, a SQL
// error or an upstream response body in a 500 is an information leak, and the
// detail belongs in the server's own logs.
func projectFailure(c fh.Ctx, err error) error {
	status, code, message := 500, "INTERNAL_ERROR", "internal error"
	var failure intent.Failure
	if errors.As(err, &failure) {
		code, message = failure.Code, failure.Message
		switch failure.Category {
		case intent.CategoryInvalidInput:
			status = 422
		case intent.CategoryNotFound:
			status = 404
		case intent.CategoryConflict:
			status = 409
		case intent.CategoryPermission:
			status = 403
		case intent.CategoryAuth:
			status = 401
		case intent.CategoryRateLimit:
			status = 429
		case intent.CategoryUnavailable:
			status = 503
		case intent.CategoryTimeout:
			status = 504
		default:
			status = 500
		}
	}
	if status == 401 {
		c.Set("WWW-Authenticate", `Bearer realm="api"`)
	}
	return c.Status(status).JSON(map[string]any{"error": map[string]any{"code": code, "message": message}})
}
