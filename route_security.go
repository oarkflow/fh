package fh

import "time"

type RouteSecurityConfig struct {
	AuthRequired        bool          `json:"auth_required,omitempty"`
	MFARequired         bool          `json:"mfa_required,omitempty"`
	Scopes              []string      `json:"scopes,omitempty"`
	Roles               []string      `json:"roles,omitempty"`
	AuditRequired       bool          `json:"audit_required,omitempty"`
	IdempotencyRequired bool          `json:"idempotency_required,omitempty"`
	SignedRequired      bool          `json:"signed_required,omitempty"`
	ReplayProtected     bool          `json:"replay_protected,omitempty"`
	BodyLimit           int64         `json:"body_limit,omitempty"`
	Timeout             time.Duration `json:"timeout,omitempty"`
	RateLimitProfile    string        `json:"rate_limit_profile,omitempty"`
	DataClass           string        `json:"data_class,omitempty"`
}

// RouteSecurity attaches route-local security metadata and enforces common
// principal/scope/role checks. Register it as route middleware.
func RouteSecurity(cfg RouteSecurityConfig) HandlerFunc {
	return func(c Ctx) error {
		c.Locals("fh.route_security", cfg)
		if cfg.AuthRequired {
			if _, ok := PrincipalFrom(c); !ok {
				EmitSecurityEvent(c, "auth.required_missing", nil)
				return NewHTTPError(StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
			}
		}
		if len(cfg.Scopes) > 0 {
			values, ok, err := extractStrings(c, PrincipalScopesExtractor())
			if err != nil {
				return err
			}
			if !ok {
				return NewHTTPError(StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
			}
			for _, want := range cfg.Scopes {
				if !hasString(values, want) {
					EmitSecurityEvent(c, "authz.scope_denied", map[string]any{"value": want})
					return NewHTTPError(StatusForbidden, "SCOPE_DENIED", "required scope is missing")
				}
			}
		}
		if len(cfg.Roles) > 0 {
			values, ok, err := extractStrings(c, PrincipalRolesExtractor())
			if err != nil {
				return err
			}
			if !ok {
				return NewHTTPError(StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
			}
			for _, want := range cfg.Roles {
				if !hasString(values, want) {
					EmitSecurityEvent(c, "authz.role_denied", map[string]any{"value": want})
					return NewHTTPError(StatusForbidden, "ROLE_DENIED", "required role is missing")
				}
			}
		}
		return c.Next()
	}
}

func DataClass(sensitivity string, categories ...string) HandlerFunc {
	return func(c Ctx) error {
		p := DataPolicy{Sensitivity: sensitivity, RedactLogs: true}
		c.Locals("fh.data_policy", p)
		return c.Next()
	}
}

// WithRouteSecurity annotates the latest route with compliance metadata.
func (a *App) WithRouteSecurity(cfg RouteSecurityConfig) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	if a.lastRoute.method == "" {
		panic("fh: no route available to annotate")
	}
	a.updateRouteInfo(a.lastRoute.method, a.lastRoute.path, func(r *RouteInfo) {
		r.Security = cfg
		if cfg.DataClass != "" {
			r.Data.Sensitivity = cfg.DataClass
		}
	})
	return a
}

func (a *App) WithDataPolicy(p DataPolicy) *App {
	a.buildMu.Lock()
	defer a.buildMu.Unlock()
	a.assertMutable()
	if a.lastRoute.method == "" {
		panic("fh: no route available to annotate")
	}
	a.updateRouteInfo(a.lastRoute.method, a.lastRoute.path, func(r *RouteInfo) { r.Data = p })
	return a
}
