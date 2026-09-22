package main

import (
	"errors"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
)

// Identity intents.
//
// These are the two intents that cannot require a principal, because they are how
// one is obtained. They still go through the address-keyed rate limiter, which is
// what stops an unauthenticated endpoint from being a free credential-stuffing
// oracle.

// ---------------------------------------------------------------------------
// auth.register
// ---------------------------------------------------------------------------

type RegisterInput struct {
	Email    string `json:"email"`
	Name     string `json:"name"`
	Password string `json:"password"`
}

type RegisterOutput struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	TenantID string `json:"tenant_id"`
	Token    string `json:"token"`
	Expires  string `json:"expires_at"`
}

type RegisterIntent struct{ deps *Deps }

func (RegisterIntent) Name() intent.Name { return "auth.register" }

func (RegisterIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "Create an account in a tenant and issue its first bearer token",
		// The public tenant, not the account's: registration happens before there
		// is an account to ask, but never outside a tenant.
		Requires:      []fact.AnyKey{PublicTenantKey.Any(), AddressRateKey.Any()},
		Timeout:       5 * time.Second,
		MaxDBQueries:  3,
		MaxExternalIO: 0,
		MaxEffects:    2,
	}
}

func (i RegisterIntent) Run(nc *ref.NodeContext, in RegisterInput) (ref.Outcome[RegisterOutput], error) {
	tenant, err := ref.Require(nc, PublicTenantKey)
	if err != nil {
		return ref.Outcome[RegisterOutput]{}, err
	}
	in.Email = strings.TrimSpace(strings.ToLower(in.Email))
	if in.Email == "" || !strings.Contains(in.Email, "@") {
		return ref.Outcome[RegisterOutput]{}, invalid("INVALID_EMAIL", "a valid email address is required")
	}
	if strings.TrimSpace(in.Name) == "" {
		return ref.Outcome[RegisterOutput]{}, invalid("INVALID_NAME", "a name is required")
	}
	hash, err := hashPassword(in.Password)
	if err != nil {
		return ref.Outcome[RegisterOutput]{}, invalid("INVALID_PASSWORD", err.Error())
	}

	if err := nc.Budget().AcquireDBQuery(2); err != nil {
		return ref.Outcome[RegisterOutput]{}, err
	}
	// The account is created here rather than in an effect because the caller needs
	// its id to receive a token, and a token for an account that might not commit is
	// worse than a write outside the effect plan. The unique index on
	// (tenant_id, email) is what makes the duplicate case a conflict rather than a
	// second account.
	user, err := i.deps.Store.CreateUser(nc.Context, User{
		TenantID:     tenant.ID,
		Email:        in.Email,
		Name:         in.Name,
		PasswordHash: hash,
		Roles:        []string{"customer"},
	})
	if err != nil {
		if isUniqueViolation(err) {
			return ref.Outcome[RegisterOutput]{}, intent.Failure{
				Code:     "EMAIL_TAKEN",
				Category: intent.CategoryConflict,
				Message:  "that email address is already registered",
			}
		}
		return ref.Outcome[RegisterOutput]{}, unavailable("the account could not be created")
	}

	token, expires, err := i.deps.Tokens.Issue(user.ID, user.TenantID, user.Roles)
	if err != nil {
		return ref.Outcome[RegisterOutput]{}, unavailable("the token could not be issued")
	}

	welcome := Notification{
		Channel: "email",
		To:      user.Email,
		Subject: "Welcome to " + tenant.Name,
		Body:    "Your account is ready.",
		Data:    map[string]any{"user_id": user.ID, "tenant_id": tenant.ID},
	}
	handle := &deliveryHandle{}
	executionID := string(nc.Invocation().ID)

	return ref.Outcome[RegisterOutput]{
		Value: RegisterOutput{
			UserID:   user.ID,
			Email:    user.Email,
			TenantID: user.TenantID,
			Token:    token,
			Expires:  expires.Format(time.RFC3339),
		},
		Effects: ref.EffectPlan{
			LocalTx: []ref.Effect{
				&WriteOutboxEffect{Source: i.deps.Effects, ExecutionID: executionID, Notification: welcome, Handle: handle},
			},
			Durable: []ref.Effect{
				&DeliverNotificationEffect{Outbox: i.deps.Outbox, Notification: welcome, Handle: handle},
			},
			FireAndForget: []ref.Effect{
				&EmitMetricEffect{Metrics: i.deps.Metrics, Metric: "accounts.registered", Value: 1},
			},
		},
		Meta: ref.OutcomeMeta{CacheControl: "no-store"},
	}, nil
}

// ---------------------------------------------------------------------------
// auth.login
// ---------------------------------------------------------------------------

type LoginInput struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginOutput struct {
	Token    string   `json:"token"`
	Expires  string   `json:"expires_at"`
	UserID   string   `json:"user_id"`
	TenantID string   `json:"tenant_id"`
	Roles    []string `json:"roles"`
}

type LoginIntent struct{ deps *Deps }

func (LoginIntent) Name() intent.Name { return "auth.login" }

func (LoginIntent) Spec() intent.Spec {
	return intent.Spec{
		Description:  "Exchange an email and password for a bearer token",
		Requires:     []fact.AnyKey{PublicTenantKey.Any(), AddressRateKey.Any()},
		Timeout:      5 * time.Second,
		MaxDBQueries: 2,
		MaxEffects:   1,
	}
}

func (i LoginIntent) Run(nc *ref.NodeContext, in LoginInput) (ref.Outcome[LoginOutput], error) {
	tenant, err := ref.Require(nc, PublicTenantKey)
	if err != nil {
		return ref.Outcome[LoginOutput]{}, err
	}
	if err := nc.Budget().AcquireDBQuery(1); err != nil {
		return ref.Outcome[LoginOutput]{}, err
	}

	user, err := i.deps.Store.UserByEmail(nc.Context, tenant.ID, strings.ToLower(strings.TrimSpace(in.Email)))
	if err != nil && !errors.Is(err, errNotFound) {
		return ref.Outcome[LoginOutput]{}, unavailable("the account could not be read")
	}
	// Both branches do the same bcrypt work and return the same failure. An unknown
	// address and a wrong password are indistinguishable from the outside, in both
	// the response and the time it takes.
	if verifyErr := verifyPassword(user.PasswordHash, in.Password); verifyErr != nil || user.Disabled || user.ID == "" {
		return ref.Outcome[LoginOutput]{}, intent.Failure{
			Code:     "INVALID_CREDENTIALS",
			Category: intent.CategoryAuth,
			Message:  "invalid credentials",
		}
	}

	token, expires, err := i.deps.Tokens.Issue(user.ID, user.TenantID, user.Roles)
	if err != nil {
		return ref.Outcome[LoginOutput]{}, unavailable("the token could not be issued")
	}
	return ref.Outcome[LoginOutput]{
		Value: LoginOutput{
			Token:    token,
			Expires:  expires.Format(time.RFC3339),
			UserID:   user.ID,
			TenantID: user.TenantID,
			Roles:    user.Roles,
		},
		Effects: ref.EffectPlan{
			FireAndForget: []ref.Effect{
				&EmitMetricEffect{Metrics: i.deps.Metrics, Metric: "logins.succeeded", Value: 1},
			},
		},
		Meta: ref.OutcomeMeta{CacheControl: "no-store"},
	}, nil
}

// ---------------------------------------------------------------------------
// Failure helpers
// ---------------------------------------------------------------------------

func invalid(code, message string) intent.Failure {
	return intent.Failure{Code: code, Category: intent.CategoryInvalidInput, Message: message}
}

func notFound(message string) intent.Failure {
	return intent.Failure{Code: "NOT_FOUND", Category: intent.CategoryNotFound, Message: message}
}

// unauthenticated is the single answer to every identity failure: no token, an
// expired token, a forged token, a deleted account, a disabled one. The caller
// learns only that they are not signed in.
func unauthenticated() intent.Failure {
	return intent.Failure{
		Code:     "UNAUTHENTICATED",
		Category: intent.CategoryAuth,
		Message:  "invalid or missing credentials",
	}
}

func forbidden(message string) intent.Failure {
	return intent.Failure{Code: "FORBIDDEN", Category: intent.CategoryPermission, Message: message}
}

// unavailable is what a caller is told when a dependency failed. The underlying
// error is logged, never returned: a database error message is an internal detail
// and frequently a disclosure.
func unavailable(message string) intent.Failure {
	return intent.Failure{Code: "UNAVAILABLE", Category: intent.CategoryUnavailable, Message: message}
}

// isUniqueViolation recognises a duplicate-key error without importing the driver's
// error type, so the example keeps working if the driver changes.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "duplicate key") ||
		strings.Contains(text, "unique constraint") ||
		strings.Contains(text, "23505")
}
