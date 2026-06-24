// Package tcpguard adapts github.com/oarkflow/tcpguard to github.com/oarkflow/fh.
//
// This package intentionally does not implement guard/rule/decision logic itself.
// It is a thin FH middleware adapter over the upstream TCPGuard engine.
package tcpguard

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/oarkflow/fh"
	guard "github.com/oarkflow/tcpguard"
)

// HeaderMode controls how much decision metadata the adapter writes to HTTP headers.
type HeaderMode string

const (
	// HeaderModeStandard preserves the historical adapter behavior and writes
	// decision, severity, trace, risk (when enabled), and public message headers.
	HeaderModeStandard HeaderMode = "standard"
	// HeaderModeCompact writes only the fields useful for support/debugging.
	// Allowed requests get X-TCPGuard-Decision only; enforced decisions also get
	// severity, trace, and a safe public message.
	HeaderModeCompact HeaderMode = "compact"
	// HeaderModeNone disables TCPGuard response metadata headers entirely.
	HeaderModeNone HeaderMode = "none"
)

type Config struct {
	// Guard is the TCPGuard instance used to evaluate requests. Required.
	Guard *guard.Guard

	// Skip bypasses TCPGuard for framework-only endpoints such as local health,
	// metrics, or a datasource callback. Returning true calls c.Next().
	Skip func(fh.Ctx) bool

	// OnDecision is called after each successful evaluation. Use it for logs,
	// metrics, traces, or SOC event fan-out that should live at adapter level.
	OnDecision func(fh.Ctx, guard.HTTPRequestResult)

	// OnError customizes evaluation/build errors. If nil, the error is returned
	// to fh's normal error handling.
	OnError func(fh.Ctx, error) error

	// HeaderPrefix controls adapter response metadata header names. Empty uses
	// X-TCPGuard.
	HeaderPrefix string

	// ResponsePolicy controls the safe X-TCPGuard-Message header for allowed
	// and denied decisions. Empty uses environment-detected safe defaults.
	ResponsePolicy guard.ResponseMessagePolicy

	// HeaderMode controls adapter-level response metadata. Empty uses
	// HeaderModeStandard for backward compatibility.
	HeaderMode HeaderMode
}

// Middleware adapts a TCPGuard Guard to fh middleware.
func Middleware(g *guard.Guard) fh.HandlerFunc {
	return MiddlewareWithConfig(Config{Guard: g})
}

// MiddlewareWithConfig adapts TCPGuard to fh with enterprise integration hooks.
//
// The adapter evaluates every request before the next fh handler runs. When the
// decision is not enforceable for the current guard mode/effect it calls
// c.Next(). When the decision is enforced, the adapter renders the configured
// TCPGuard response and stops the fh chain.
func MiddlewareWithConfig(cfg Config) fh.HandlerFunc {
	prefix := cfg.HeaderPrefix
	if prefix == "" {
		prefix = "X-TCPGuard"
	}
	responsePolicy := cfg.ResponsePolicy
	return func(c fh.Ctx) error {
		if cfg.Skip != nil && cfg.Skip(c) {
			return c.Next()
		}
		if cfg.Guard == nil {
			return fmt.Errorf("tcpguard fh adapter: nil guard")
		}

		req, err := http.NewRequestWithContext(c.Context(), c.Method(), c.OriginalURL(), bytes.NewReader(c.BodyRaw()))
		if err != nil {
			return handleError(cfg, c, err)
		}
		req.Host = c.Hostname()
		req.RemoteAddr = c.IP()
		for key, values := range c.GetReqHeaders() {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}

		result, err := cfg.Guard.EvaluateHTTPRequest(req)
		if err != nil {
			return handleError(cfg, c, err)
		}
		setDecisionHeaders(c, prefix, result, responsePolicy, cfg.HeaderMode)
		if cfg.OnDecision != nil {
			cfg.OnDecision(c, result)
		}
		if !result.Enforced {
			return c.Next()
		}
		for key, value := range result.Response.Headers {
			c.Set(key, value)
		}
		return c.Status(result.Response.Status).JSON(result.Response.Body)
	}
}

// New is kept for backward compatibility with older examples. New code should
// use Middleware or MiddlewareWithConfig for consistency with other adapters.
func New(g *guard.Guard) fh.HandlerFunc { return Middleware(g) }

func handleError(cfg Config, c fh.Ctx, err error) error {
	if cfg.OnError != nil {
		return cfg.OnError(c, err)
	}
	return err
}

func setDecisionHeaders(c fh.Ctx, prefix string, result guard.HTTPRequestResult, policy guard.ResponseMessagePolicy, mode HeaderMode) {
	if mode == "" {
		mode = HeaderModeStandard
	}
	if mode == HeaderModeNone {
		return
	}
	c.Set(prefix+"-Decision", string(result.Decision.Effect))

	if mode == HeaderModeCompact && !result.Enforced {
		return
	}
	if policy.IncludeRiskScore {
		c.Set(prefix+"-Risk", fmt.Sprintf("%.0f", result.Decision.Risk.Score))
	}
	if result.Decision.Severity != "" {
		c.Set(prefix+"-Severity", string(result.Decision.Severity))
	}
	if result.Context != nil && result.Context.Request.ID != "" {
		c.Set(prefix+"-Trace", result.Context.Request.ID)
	}
	if msg := guard.PublicDecisionMessage(result.Context, result.Decision, policy); msg != "" {
		c.Set(prefix+"-Message", msg)
	}
}
