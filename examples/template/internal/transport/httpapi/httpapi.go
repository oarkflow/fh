package httpapi

import (
	"errors"

	"github.com/oarkflow/authz"
	"github.com/oarkflow/fh"
	appauth "github.com/oarkflow/fh/examples/template/internal/auth"
	"github.com/oarkflow/fh/examples/template/internal/service"
	"github.com/oarkflow/fh/mw/session"
)

type CreateRequest struct {
	Name string `json:"name" query:"name"`
}
type CreateResponse struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type LoginResponse struct {
	Principal appauth.Principal `json:"principal"`
	Tokens    *authz.TokenPair  `json:"tokens"`
}

func Register(app *fh.App, svc *service.Service, credentials *appauth.Credentials, tokens *authz.TokenConfig, sessions *session.SessionManager, sessionMiddleware, authCSRF, authenticated, protected fh.HandlerFunc) {
	api := app.Group("/v1")
	authMutations := api.Group("/auth", sessionMiddleware, authCSRF)
	authMutations.Get("/csrf", func(c fh.Ctx) error {
		return c.JSON(fh.Map{"csrf_token": c.Locals("csrf_token")})
	})
	authMutations.PostTyped("/login", func(c fh.Ctx, req LoginRequest) (LoginResponse, error) {
		if credentials == nil {
			return LoginResponse{}, fh.NewHTTPError(fh.StatusServiceUnavailable, "LOGIN_DISABLED", "configure APP_BOOTSTRAP_PASSWORD or APP_BOOTSTRAP_PASSWORD_FILE")
		}
		principal, err := credentials.Verify(req.Username, req.Password)
		if err != nil {
			return LoginResponse{}, fh.NewHTTPError(fh.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid credentials")
		}
		webSession := session.Get(c)
		webSession.Set("user_id", principal.Subject)
		webSession.Set("tenant_id", principal.Tenant)
		webSession.Set("roles", principal.Roles)
		if err := sessions.Regenerate(c, webSession); err != nil {
			return LoginResponse{}, err
		}
		pair, err := tokens.GenerateTokenPair(&authz.TokenClaims{UserID: principal.Subject, TenantID: principal.Tenant, Roles: principal.Roles})
		if err != nil {
			return LoginResponse{}, err
		}
		return LoginResponse{Principal: principal, Tokens: pair}, nil
	})
	api.Get("/auth/me", authenticated, func(c fh.Ctx) error { return c.JSON(c.Locals("principal")) })
	authMutations.Post("/logout", authenticated, func(c fh.Ctx) error {
		if err := sessions.Destroy(c, session.Get(c)); err != nil {
			return err
		}
		return c.JSON(fh.Map{"ok": true})
	})
	api.Get("/example", func(c fh.Ctx) error {
		items, err := svc.List(c.Context())
		if err != nil {
			return err
		}
		return c.JSON(items)
	})
	api.PostTyped("/example", func(c fh.Ctx, req CreateRequest) (CreateResponse, error) {
		item, err := svc.Add(c.Context(), req.Name)
		if errors.Is(err, service.ErrInvalidName) {
			return CreateResponse{}, fh.NewHTTPError(422, "VALIDATION_FAILED", "name is required")
		}
		return CreateResponse{ID: item.ID, Name: item.Name}, err
	})
	api.Get("/protected", protected, func(c fh.Ctx) error { return c.JSON(fh.Map{"ok": true, "message": "authenticated route"}) })
}
