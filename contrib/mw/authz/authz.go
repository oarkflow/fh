package middleware

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/authz/stores"
	"github.com/oarkflow/fh"
)

var ErrMissingEngine = errors.New("authz middleware: missing engine")

func LoadEngineFromAuthzFile(path string) (*authz.Engine, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg, err := authz.NewDSLParser().Parse(data)
	if err != nil {
		return nil, err
	}

	roleMembers := stores.NewMemoryRoleMembershipStore()

	engine := authz.NewEngine(
		stores.NewMemoryPolicyStore(),
		stores.NewMemoryRoleStore(),
		stores.NewMemoryACLStore(),
		stores.NewMemoryAuditStore(),
		authz.WithRoleMembershipStore(roleMembers),
	)

	if err := engine.ApplyConfig(context.Background(), cfg); err != nil {
		return nil, err
	}

	return engine, nil
}

type FHConfig struct {
	Engine *authz.Engine

	Subject     fh.Extractor[*authz.Subject]
	Action      fh.Extractor[authz.Action]
	Resource    fh.Extractor[*authz.Resource]
	Environment fh.Extractor[*authz.Environment]

	OnDenied          func(c fh.Ctx, decision *authz.Decision) error
	OnUnauthenticated func(c fh.Ctx, err error) error
	OnError           func(c fh.Ctx, err error) error

	Next      func(c fh.Ctx) bool
	SkipPaths []string

	Context       func(c fh.Ctx) context.Context
	DecisionLocal string
}

func SubjectFromHeaders() fh.Extractor[*authz.Subject] {
	return func(c fh.Ctx) (*authz.Subject, bool, error) {
		return FHDefaultSubjectExtractor(c), true, nil
	}
}

func SubjectFromHeadersOrPrincipal() fh.Extractor[*authz.Subject] {
	return func(c fh.Ctx) (*authz.Subject, bool, error) {
		if p, ok := fh.PrincipalFrom(c); ok {
			return principalSubject(p), true, nil
		}
		return FHDefaultSubjectExtractor(c), true, nil
	}
}

func SubjectFromJWTClaims(localKey string) fh.Extractor[*authz.Subject] {
	if localKey == "" {
		localKey = "jwt_claims"
	}
	return func(c fh.Ctx) (*authz.Subject, bool, error) {
		claims, ok := c.Locals(localKey).(map[string]any)
		if !ok || claims == nil {
			return nil, false, nil
		}
		id := claimString(claims, "sub")
		if id == "" {
			id = claimString(claims, "subject")
		}
		if id == "" {
			return nil, false, nil
		}
		return &authz.Subject{
			ID:       id,
			Type:     claimStringDefault(claims, "type", "user"),
			TenantID: firstClaimString(claims, "tenant_id", "tenant", "tid"),
			Roles:    claimStrings(claims, "roles", "role", "groups"),
			Attrs: map[string]any{
				"claims":      claims,
				"scopes":      claimStrings(claims, "scope", "scp", "scopes"),
				"permissions": claimStrings(claims, "permissions", "perms"),
				"auth_method": "jwt",
			},
		}, true, nil
	}
}

func SubjectFromPrincipal() fh.Extractor[*authz.Subject] {
	return func(c fh.Ctx) (*authz.Subject, bool, error) {
		p, ok := fh.PrincipalFrom(c)
		if !ok {
			return nil, false, nil
		}
		return principalSubject(p), true, nil
	}
}

func ActionFromMethod() fh.Extractor[authz.Action] {
	return func(c fh.Ctx) (authz.Action, bool, error) {
		if c == nil {
			return "", false, nil
		}
		action := authz.Action(c.Method())
		return action, action != "", nil
	}
}

func StaticAction(action authz.Action) fh.Extractor[authz.Action] {
	return func(fh.Ctx) (authz.Action, bool, error) {
		return action, action != "", nil
	}
}

func ResourceFromRoute(resourceType, idParam string) fh.Extractor[*authz.Resource] {
	return func(c fh.Ctx) (*authz.Resource, bool, error) {
		res := FHDefaultResourceExtractor(c)
		if resourceType != "" {
			res.Type = resourceType
		}
		if idParam != "" {
			if id := c.Param(idParam); id != "" {
				res.ID = id
			}
		}
		return res, true, nil
	}
}

func EnvironmentFromRequest() fh.Extractor[*authz.Environment] {
	return func(c fh.Ctx) (*authz.Environment, bool, error) {
		return FHDefaultEnvironmentExtractor(c), true, nil
	}
}

func FHDefaultSubjectExtractor(c fh.Ctx) *authz.Subject {
	return &authz.Subject{
		ID:       c.Get("X-Subject-ID"),
		TenantID: c.Get("X-Tenant-ID"),
		Roles:    splitTrim(c.Get("X-Roles"), ","),
	}
}

func FHDefaultResourceExtractor(c fh.Ctx) *authz.Resource {
	tenant := c.Get("X-Tenant-ID")

	return &authz.Resource{
		ID:       c.Method() + ":" + c.Path(),
		Type:     "route",
		TenantID: tenant,
	}
}

func FHDefaultEnvironmentExtractor(c fh.Ctx) *authz.Environment {
	return &authz.Environment{
		Time:     time.Now(),
		TenantID: c.Get("X-Tenant-ID"),
	}
}

func FHDefaultUnauthenticatedHandler(c fh.Ctx, err error) error {
	return c.Status(http.StatusUnauthorized).JSON(map[string]any{
		"error":   "unauthorized",
		"message": "authentication is required",
	})
}

func FHDefaultDeniedHandler(c fh.Ctx, decision *authz.Decision) error {
	return c.Status(http.StatusForbidden).JSON(map[string]any{
		"error":   "forbidden",
		"message": "access denied",
	})
}

func FHDefaultErrorHandler(c fh.Ctx, err error) error {
	return c.Status(http.StatusInternalServerError).JSON(map[string]any{
		"error":   "internal_error",
		"message": "authorization check failed",
	})
}

func FHDefaultConfig(engine *authz.Engine) FHConfig {
	return FHConfig{
		Engine:            engine,
		Subject:           SubjectFromHeaders(),
		Action:            ActionFromMethod(),
		Resource:          ResourceFromRoute("", ""),
		Environment:       EnvironmentFromRequest(),
		OnDenied:          FHDefaultDeniedHandler,
		OnUnauthenticated: FHDefaultUnauthenticatedHandler,
		OnError:           FHDefaultErrorHandler,
		DecisionLocal:     "authz_decision",
		Context: func(c fh.Ctx) context.Context {
			return context.Background()
		},
	}
}

func FH(engine *authz.Engine) fh.HandlerFunc {
	return FHWithConfig(FHDefaultConfig(engine))
}

func FHWithConfig(cfg FHConfig) fh.HandlerFunc {
	if cfg.Subject == nil {
		cfg.Subject = SubjectFromHeaders()
	}
	if cfg.Action == nil {
		cfg.Action = ActionFromMethod()
	}
	if cfg.Resource == nil {
		cfg.Resource = ResourceFromRoute("", "")
	}
	if cfg.Environment == nil {
		cfg.Environment = EnvironmentFromRequest()
	}
	if cfg.OnDenied == nil {
		cfg.OnDenied = FHDefaultDeniedHandler
	}
	if cfg.OnUnauthenticated == nil {
		cfg.OnUnauthenticated = FHDefaultUnauthenticatedHandler
	}
	if cfg.OnError == nil {
		cfg.OnError = FHDefaultErrorHandler
	}
	if cfg.Context == nil {
		cfg.Context = func(c fh.Ctx) context.Context {
			return context.Background()
		}
	}
	if cfg.DecisionLocal == "" {
		cfg.DecisionLocal = "authz_decision"
	}

	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}

		if shouldSkipPath(c.Path(), cfg.SkipPaths) {
			return c.Next()
		}

		if cfg.Engine == nil {
			return cfg.OnError(c, ErrMissingEngine)
		}

		subject, ok, err := cfg.Subject(c)
		if err != nil {
			return cfg.OnError(c, err)
		}
		if !ok || subject == nil || strings.TrimSpace(subject.ID) == "" {
			return cfg.OnUnauthenticated(c, errors.New("authz middleware: subject extractor returned empty"))
		}

		action, ok, err := cfg.Action(c)
		if err != nil {
			return cfg.OnError(c, err)
		}
		if !ok || action == "" {
			action = authz.Action(c.Method())
		}

		resource, ok, err := cfg.Resource(c)
		if err != nil {
			return cfg.OnError(c, err)
		}
		if !ok || resource == nil {
			return cfg.OnError(c, errors.New("authz middleware: resource extractor returned empty"))
		}

		env, _, err := cfg.Environment(c)
		if err != nil {
			return cfg.OnError(c, err)
		}
		if env == nil {
			env = &authz.Environment{Time: time.Now()}
		}

		if resource.TenantID == "" {
			resource.TenantID = subject.TenantID
		}
		if env.TenantID == "" {
			env.TenantID = subject.TenantID
		}
		if resource.Type == "" {
			resource.Type = "route"
		}
		if resource.ID == "" {
			resource.ID = c.Method() + ":" + c.Path()
		}

		decision, err := cfg.Engine.Authorize(
			cfg.Context(c),
			subject,
			action,
			resource,
			env,
		)
		if err != nil {
			return cfg.OnError(c, err)
		}

		c.Locals(cfg.DecisionLocal, decision)

		if !decision.Allowed {
			return cfg.OnDenied(c, decision)
		}

		return c.Next()
	}
}

func FHDecision(c fh.Ctx) *authz.Decision {
	if decision, ok := c.Locals("authz_decision").(*authz.Decision); ok {
		return decision
	}
	return nil
}

func FHParamResourceExtractor(paramMap map[string]string) fh.Extractor[*authz.Resource] {
	return func(c fh.Ctx) (*authz.Resource, bool, error) {
		tenant := c.Get("X-Tenant-ID")

		res := &authz.Resource{
			ID:       c.Method() + ":" + c.Path(),
			Type:     "route",
			TenantID: tenant,
			Attrs:    make(map[string]any),
		}

		for urlParam, resField := range paramMap {
			val := c.Param(urlParam)
			if val == "" {
				continue
			}

			switch resField {
			case "id":
				res.ID = val
			case "type":
				res.Type = val
			case "owner":
				res.OwnerID = val
			case "tenant":
				res.TenantID = val
			default:
				res.Attrs[resField] = val
			}
		}

		return res, true, nil
	}
}

func FHResourceFromPath() fh.Extractor[*authz.Resource] {
	return func(c fh.Ctx) (*authz.Resource, bool, error) {
		tenant := c.Get("X-Tenant-ID")
		path := strings.Trim(c.Path(), "/")
		parts := strings.SplitN(path, "/", 3)

		res := &authz.Resource{
			ID:       c.Method() + ":" + c.Path(),
			Type:     "route",
			TenantID: tenant,
		}

		if len(parts) >= 1 && parts[0] != "" {
			res.Type = parts[0]
		}
		if len(parts) >= 2 && parts[1] != "" {
			res.ID = parts[1]
		}

		return res, true, nil
	}
}

func FHRouteOwnerResource() fh.Extractor[*authz.Resource] {
	return func(c fh.Ctx) (*authz.Resource, bool, error) {
		tenant := c.Get("X-Tenant-ID")

		res := &authz.Resource{
			ID:       c.Method() + ":" + c.Path(),
			Type:     "route",
			TenantID: tenant,
		}

		parts := strings.Split(strings.Trim(c.Path(), "/"), "/")
		if len(parts) >= 2 && parts[0] == "users" {
			res.OwnerID = parts[len(parts)-1]
		}

		return res, true, nil
	}
}

func shouldSkipPath(path string, skipPaths []string) bool {
	for _, p := range skipPaths {
		if p == "" {
			continue
		}

		if p == path {
			return true
		}

		if strings.HasSuffix(p, "*") {
			prefix := strings.TrimSuffix(p, "*")
			if strings.HasPrefix(path, prefix) {
				return true
			}
		}
	}

	return false
}

func splitTrim(s, sep string) []string {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}

	return out
}

func principalSubject(p fh.Principal) *authz.Subject {
	return &authz.Subject{
		ID:       p.ID,
		Type:     p.Type,
		TenantID: p.TenantID,
		Roles:    p.Roles,
		Attrs: map[string]any{
			"subject":     p.Subject,
			"scopes":      p.Scopes,
			"permissions": p.Permissions,
			"claims":      p.Claims,
			"auth_method": p.AuthMethod,
		},
	}
}

func claimString(claims map[string]any, key string) string {
	if claims == nil || key == "" {
		return ""
	}
	v, _ := claims[key].(string)
	return strings.TrimSpace(v)
}

func claimStringDefault(claims map[string]any, key, def string) string {
	if v := claimString(claims, key); v != "" {
		return v
	}
	return def
}

func firstClaimString(claims map[string]any, keys ...string) string {
	for _, key := range keys {
		if v := claimString(claims, key); v != "" {
			return v
		}
	}
	return ""
}

func claimStrings(claims map[string]any, keys ...string) []string {
	for _, key := range keys {
		if claims == nil || key == "" {
			continue
		}
		switch v := claims[key].(type) {
		case string:
			return splitTrim(strings.ReplaceAll(v, " ", ","), ",")
		case []string:
			out := make([]string, 0, len(v))
			for _, s := range v {
				s = strings.TrimSpace(s)
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		case []any:
			out := make([]string, 0, len(v))
			for _, it := range v {
				if s, _ := it.(string); strings.TrimSpace(s) != "" {
					out = append(out, strings.TrimSpace(s))
				}
			}
			return out
		}
	}
	return nil
}
