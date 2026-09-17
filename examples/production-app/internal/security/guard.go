// Package security wires github.com/oarkflow/tcpguard into the application
// for runtime anomaly detection and business-logic abuse protection
// (credential stuffing, endpoint scanning, note-creation velocity, and so
// on). It is deliberately separate from RBAC/ABAC (see internal/auth and
// policy.authz): authz answers "is this subject allowed to do this?" for a
// specific request, while tcpguard answers "does this traffic pattern look
// like an attack or a runaway business process?" across many requests.
package security

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/tcpguard"
)

// Guard wraps a *tcpguard.Guard with the fh middleware and event-emission
// helpers the rest of the application calls.
type Guard struct {
	engine         *tcpguard.Guard
	responsePolicy tcpguard.ResponseMessagePolicy
	logger         LogFunc
}

// LogFunc receives every tcpguard decision (allowed or enforced) as plain
// values, so callers (main.go) can wire it into their own structured logger
// without importing tcpguard themselves just to log.
type LogFunc func(event, requestID string, effect, severity string, riskScore float64)

type Options struct {
	// PolicyDir is a directory of *.bcl policy files (see security/policy).
	PolicyDir  string
	Production bool
	// Log receives every decision (allowed or enforced) for structured
	// logging/metrics/SIEM export. May be nil.
	Log LogFunc
}

func New(ctx context.Context, opts Options) (*Guard, error) {
	bundle, err := tcpguard.LoadTCPGuardBundleDir(ctx, opts.PolicyDir)
	if err != nil {
		return nil, fmt.Errorf("load tcpguard policy: %w", err)
	}
	env := tcpguard.EnvironmentDevelopment
	if opts.Production {
		env = tcpguard.EnvironmentProduction
	}
	policy := tcpguard.DefaultResponseMessagePolicy(env)
	renderer := tcpguard.PublicDecisionResponseRenderer(policy)

	engine, err := tcpguard.New(
		tcpguard.WithBundle(bundle),
		tcpguard.WithResponseMessagePolicy(policy),
		tcpguard.WithResponseRenderer(renderer),
		tcpguard.WithStore(tcpguard.NewMemoryStore()),
		tcpguard.WithMetrics(tcpguard.NewMemoryMetrics()),
		// GeoIP enrichment downloads and indexes a multi-million-row IP
		// database on first use if enabled - fine for a policy pack that
		// gates on country, but a needless multi-second stall on the
		// request that happens to trigger it (observed: an auth request
		// blocked ~17s and hit its own timeout) for one that does not. This
		// template's default policy pack has no geo rules; turn GeoIP back
		// on only alongside a rule that actually uses network.country*.
		tcpguard.WithContextBuilder(tcpguard.HTTPContextBuilder{TrustedProxyHeaders: false, DisableGeoIP: true}),
	)
	if err != nil {
		return nil, fmt.Errorf("create tcpguard: %w", err)
	}
	return &Guard{engine: engine, responsePolicy: policy, logger: opts.Log}, nil
}

// Middleware evaluates every request tcpguard's policy pack applies to and
// either continues the fh chain or writes the configured decision response
// and stops it. skip lets the caller exempt static assets and framework-only
// endpoints (health checks, metrics) from evaluation.
func (g *Guard) Middleware(skip func(fh.Ctx) bool) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if skip != nil && skip(c) {
			return c.Next()
		}
		req, err := toHTTPRequest(c)
		if err != nil {
			return err
		}
		result, err := g.engine.EvaluateHTTPRequest(req)
		if err != nil {
			return err
		}
		g.setDecisionHeaders(c, result)
		g.log("tcpguard.http.decision", result.Context, result.Decision)
		if !result.Enforced {
			return c.Next()
		}
		for key, value := range result.Response.Headers {
			c.Set(key, value)
		}
		return c.Status(result.Response.Status).JSON(result.Response.Body)
	}
}

// EmitAuthEvent reports a login outcome so the abuse detector's
// credential-stuffing / password-spray / account-enumeration facts stay
// accurate. eventType is "auth.login_failed" or "auth.login_success". Returns
// true when the caller should stop and respond with a generic
// too-many-attempts error instead of the normal auth failure/success
// response - the brute-force threshold can be crossed by the very attempt
// that is being reported, so the caller must check this before replying.
func (g *Guard) EmitAuthEvent(c fh.Ctx, eventType, userID string) (blocked bool, err error) {
	req, err := toHTTPRequest(c)
	if err != nil {
		return false, err
	}
	sec, err := (tcpguard.HTTPContextBuilder{DisableGeoIP: true, IdentityExtractor: func(_ *http.Request, s *tcpguard.Context) {
		s.Identity.ID = userID
	}}).BuildHTTP(c.Context(), req)
	if err != nil {
		return false, err
	}
	decision := g.engine.Evaluate(c.Context(), tcpguard.Event{Type: eventType, RequestID: sec.Request.ID}, sec)
	g.log("tcpguard."+eventType, sec, decision)
	return decision.Effect == tcpguard.DecisionBlock || decision.Effect == tcpguard.DecisionDeny, nil
}

// EmitBusinessEvent reports a business action (e.g. "note.create") tagged
// with an approximate monetary/volume amount, feeding the abuse detector's
// function-invocation and payment-velocity facts. Returns true when the
// caller should stop and respond with a generic "too many requests"/"blocked"
// error instead of performing the action.
func (g *Guard) EmitBusinessEvent(c fh.Ctx, action string, amount float64) (blocked bool, err error) {
	req, err := toHTTPRequest(c)
	if err != nil {
		return false, err
	}
	sec, err := (tcpguard.HTTPContextBuilder{DisableGeoIP: true, BusinessExtractor: func(_ *http.Request, s *tcpguard.Context) {
		s.Business.Action = action
		s.Business.Amount = amount
	}}).BuildHTTP(c.Context(), req)
	if err != nil {
		return false, err
	}
	decision := g.engine.Evaluate(c.Context(), tcpguard.Event{Type: "function.invoked", RequestID: sec.Request.ID}, sec)
	g.log("tcpguard.business."+action, sec, decision)
	return decision.Effect == tcpguard.DecisionBlock || decision.Effect == tcpguard.DecisionDeny, nil
}

func (g *Guard) setDecisionHeaders(c fh.Ctx, result tcpguard.HTTPRequestResult) {
	c.Set("X-TCPGuard-Decision", string(result.Decision.Effect))
	if result.Decision.Severity != "" {
		c.Set("X-TCPGuard-Severity", string(result.Decision.Severity))
	}
	if result.Context != nil && result.Context.Request.ID != "" {
		c.Set("X-TCPGuard-Trace", result.Context.Request.ID)
	}
}

func (g *Guard) log(event string, sec *tcpguard.Context, decision tcpguard.Decision) {
	if g.logger == nil {
		return
	}
	requestID := ""
	if sec != nil {
		requestID = sec.Request.ID
	}
	g.logger(event, requestID, string(decision.Effect), string(decision.Severity), decision.Risk.Score)
}

// toHTTPRequest adapts an fh.Ctx into the framework-neutral *http.Request
// tcpguard's public API expects. fh implements its own HTTP/1.1+HTTP/2
// stack (it does not wrap net/http), so there is no request to reuse here -
// this builds a synthetic one from the fields tcpguard's context builder
// reads (method, URL, headers, body, remote address).
func toHTTPRequest(c fh.Ctx) (*http.Request, error) {
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), c.OriginalURL(), bytes.NewReader(c.BodyRaw()))
	if err != nil {
		return nil, fmt.Errorf("build tcpguard request: %w", err)
	}
	req.Host = c.Hostname()
	req.RemoteAddr = c.IP() + ":0"
	for key, values := range c.GetReqHeaders() {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return req, nil
}
