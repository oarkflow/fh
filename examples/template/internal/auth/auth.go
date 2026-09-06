package auth

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/authz/pkg/stores"
	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
)

var ErrUnauthenticated = errors.New("authentication required")
var ErrForbidden = errors.New("permission denied")

type Principal struct {
	Subject string   `json:"subject"`
	Roles   []string `json:"roles,omitempty"`
	Tenant  string   `json:"tenant,omitempty"`
}
type Authenticator interface {
	Authenticate(context.Context, fh.Ctx) (Principal, error)
}
type Authorizer interface {
	Authorize(context.Context, Principal, string, any) error
}

// Credentials is a minimal bootstrap identity adapter. The plaintext password
// is hashed once with authz's Argon2id implementation and never retained.
type Credentials struct {
	username string
	hash     string
}

func NewCredentials(username, password string) (*Credentials, error) {
	if strings.TrimSpace(username) == "" || password == "" {
		return nil, nil
	}
	hash, err := authz.HashPassword(password)
	if err != nil {
		return nil, err
	}
	return &Credentials{username: username, hash: hash}, nil
}

func (v *Credentials) Verify(username, password string) (Principal, error) {
	if v == nil || username != v.username || authz.CheckPassword(v.hash, password) != nil {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: username, Tenant: "default", Roles: []string{"developer"}}, nil
}

type DevAuthenticator struct{}

func (DevAuthenticator) Authenticate(_ context.Context, c fh.Ctx) (Principal, error) {
	ip := net.ParseIP(c.IP())
	if ip == nil || !ip.IsLoopback() {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: "local-development", Tenant: "default", Roles: []string{"developer"}}, nil
}

// BearerAuthenticator validates HMAC bearer tokens issued by authz.TokenConfig.
type BearerAuthenticator struct{ Tokens *authz.TokenConfig }

func (a BearerAuthenticator) Authenticate(_ context.Context, c fh.Ctx) (Principal, error) {
	header := strings.TrimSpace(firstHeader(c.GetReqHeaders(), "Authorization"))
	if !strings.HasPrefix(strings.ToLower(header), "bearer ") || a.Tokens == nil {
		return Principal{}, ErrUnauthenticated
	}
	claims, err := a.Tokens.ValidateToken(strings.TrimSpace(header[len("Bearer "):]))
	if err != nil || claims.TokenType != "access" || claims.UserID == "" {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: claims.UserID, Tenant: claims.TenantID, Roles: claims.Roles}, nil
}

// SessionAuthenticator accepts a server-side session populated by a verified login handler.
type SessionAuthenticator struct{ Manager *session.SessionManager }

func (a SessionAuthenticator) Authenticate(_ context.Context, c fh.Ctx) (Principal, error) {
	s, ok := c.Locals("session").(*session.Session)
	if !ok {
		if a.Manager == nil {
			return Principal{}, ErrUnauthenticated
		}
		var err error
		s, err = a.Manager.Load(c)
		if err != nil {
			return Principal{}, ErrUnauthenticated
		}
	}
	id, ok := s.Get("user_id").(string)
	if !ok || id == "" {
		return Principal{}, ErrUnauthenticated
	}
	roles := stringsFromSession(s.Get("roles"))
	tenantID, _ := s.Get("tenant_id").(string)
	return Principal{Subject: id, Tenant: tenantID, Roles: roles}, nil
}

func stringsFromSession(value any) []string {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		result := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}

type FirstAuthenticator struct{ Authenticators []Authenticator }

func (a FirstAuthenticator) Authenticate(ctx context.Context, c fh.Ctx) (Principal, error) {
	for _, candidate := range a.Authenticators {
		if candidate == nil {
			continue
		}
		if principal, err := candidate.Authenticate(ctx, c); err == nil {
			return principal, nil
		}
	}
	return Principal{}, ErrUnauthenticated
}

type RoleAuthorizer struct{ Engine *authz.Engine }

func (z RoleAuthorizer) Authorize(ctx context.Context, p Principal, permission string, _ any) error {
	if z.Engine == nil {
		for _, candidate := range p.Roles {
			if candidate == permission {
				return nil
			}
		}
		return ErrForbidden
	}
	resourceID := strings.TrimPrefix(permission, "route:")
	decision, err := z.Engine.Authorize(ctx, &authz.Subject{ID: p.Subject, TenantID: tenant(p.Tenant), Roles: p.Roles}, authz.Action("access"), &authz.Resource{ID: resourceID, Type: "route", TenantID: tenant(p.Tenant)}, &authz.Environment{TenantID: tenant(p.Tenant)})
	if err != nil || decision == nil || !decision.Allowed {
		return ErrForbidden
	}
	return nil
}

func tenant(v string) string {
	if v == "" {
		return "default"
	}
	return v
}
func firstHeader(headers map[string][]string, name string) string {
	for key, values := range headers {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

// NewRBAC creates a deny-by-default authz engine with an example developer role.
func NewRBAC() *authz.Engine {
	roles := stores.NewMemoryRoleStore()
	role := authz.NewRoleBuilder().ID("developer").Name("Developer").Tenant("default").Permission("access", "route:protected").Build()
	_ = roles.CreateRole(context.Background(), role)
	return authz.NewEngine(stores.NewMemoryPolicyStore(), roles, stores.NewMemoryACLStore(), stores.NewMemoryAuditStore())
}

func Middleware(a Authenticator, z Authorizer, permission string) fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if a == nil {
			return fh.NewHTTPError(fh.StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
		}
		p, err := a.Authenticate(c.Context(), c)
		if err != nil {
			return fh.NewHTTPError(fh.StatusUnauthorized, "AUTH_REQUIRED", "authentication is required")
		}
		c.Locals("principal", p)
		if z != nil {
			if err := z.Authorize(c.Context(), p, permission, nil); err != nil {
				return fh.NewHTTPError(fh.StatusForbidden, "FORBIDDEN", "permission denied")
			}
		}
		return c.Next()
	}
}
