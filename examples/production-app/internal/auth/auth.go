package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/fh"
	authzmw "github.com/oarkflow/fh-contrib/mw/authz"
	"github.com/oarkflow/fh/mw/session"

	appdb "github.com/oarkflow/fh/examples/production-app/internal/database"
)

const userKey = "authenticated_user_id"

// RoleStore is the minimum the auth package needs from the database, kept
// narrow so this package stays testable without a real database.
type RoleStore interface {
	EnsureUser(ctx context.Context, id, name, role string) (appdb.User, error)
}

// Login authenticates the credential and loads the caller's RBAC role from
// the database (defaultRole seeds it on first login) rather than hardcoding
// it, so promoting a user to admin/editor/viewer is a data change, not a
// redeploy.
func Login(c fh.Ctx, manager *session.SessionManager, store RoleStore, username, password, expectedUser, expectedPassword, defaultRole string) error {
	if !secretEqual(username, expectedUser) || !secretEqual(password, expectedPassword) {
		return fh.NewHTTPError(fh.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid credentials")
	}
	user, err := store.EnsureUser(c.Context(), username, username, defaultRole)
	if err != nil {
		return fh.NewHTTPError(fh.StatusServiceUnavailable, "ROLE_LOOKUP_FAILED", "could not resolve account role")
	}
	web := session.Get(c)
	web.Set(userKey, username)
	web.Set("roles", []string{user.Role})
	return manager.Regenerate(c, web)
}

func Principal(c fh.Ctx) (fh.Principal, bool) {
	web := session.Get(c)
	user, _ := web.Get(userKey).(string)
	if user == "" {
		return fh.Principal{}, false
	}
	roles := rolesFromSession(web.Get("roles"))
	if len(roles) == 0 {
		roles = []string{"viewer"}
	}
	p := fh.Principal{ID: user, Subject: user, Type: "user", Roles: roles, AuthMethod: "session"}
	fh.SetPrincipal(c, p)
	return p, true
}

func Require(c fh.Ctx) error {
	if _, ok := Principal(c); !ok {
		return fh.NewHTTPError(fh.StatusUnauthorized, "LOGIN_REQUIRED", "login required")
	}
	return c.Next()
}

func PolicyMiddleware(engine *authz.Engine) fh.HandlerFunc {
	return authzmw.FHWithConfig(authzmw.FHConfig{
		Engine: engine, Subject: authzmw.SubjectFromPrincipal(), Action: authzmw.ActionFromMethod(),
		Resource: authzmw.ResourceFromRoute("api", ""), Environment: authzmw.EnvironmentFromRequest(),
		OnDenied: authzmw.FHDefaultDeniedHandler, OnUnauthenticated: authzmw.FHDefaultUnauthenticatedHandler, OnError: authzmw.FHDefaultErrorHandler,
	})
}

// AuthorizeOwned runs a fine-grained, object-level ABAC check for a resource
// whose owner is only known after it has been fetched from the database (a
// specific note, for instance). Route-level PolicyMiddleware only ever sees
// the route pattern, not the row, so this check runs inside the handler once
// the row (and therefore its owner) is known. See policy.authz's
// "notes-owner-rw" / "notes-admin-all" policies.
func AuthorizeOwned(c fh.Ctx, engine *authz.Engine, principal fh.Principal, action authz.Action, resourceType, resourceID, ownerID string) error {
	subject := &authz.Subject{ID: principal.ID, Type: principal.Type, Roles: principal.Roles}
	resource := &authz.Resource{ID: resourceID, Type: resourceType, OwnerID: ownerID}
	decision, err := engine.Authorize(c.Context(), subject, action, resource, &authz.Environment{})
	if err != nil {
		return fh.NewHTTPError(fh.StatusInternalServerError, "AUTHORIZATION_ERROR", "authorization check failed")
	}
	if !decision.Allowed {
		return fh.NewHTTPError(fh.StatusForbidden, "FORBIDDEN", "access denied")
	}
	return nil
}

func DecodeLogin(c fh.Ctx) (string, string, error) {
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(c.BodyCopy(), &input); err != nil || input.Username == "" || input.Password == "" {
		return "", "", fh.NewHTTPError(fh.StatusBadRequest, "INVALID_LOGIN", "username and password are required")
	}
	return input.Username, input.Password, nil
}

// rolesFromSession normalizes the "roles" session value. A freshly set
// session holds a real []string, but the session store round-trips through
// JSON on save/load (see mw/session), which turns it into []any - without
// this, every request after the first would silently fall back to the
// least-privileged role.
func rolesFromSession(value any) []string {
	switch v := value.(type) {
	case []string:
		return v
	case []any:
		roles := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				roles = append(roles, s)
			}
		}
		return roles
	default:
		return nil
	}
}

func secretEqual(actual, expected string) bool {
	a := sha256.Sum256([]byte(actual))
	b := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}
