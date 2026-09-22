package platform

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/platform/spi"
	"golang.org/x/crypto/bcrypt"
)

// Authentication actions: the pieces a login, logout, password change and
// password reset flow are assembled from.
//
// Three decisions here are worth defending because they are the difference
// between a demo and something you can put on the internet:
//
//   - A failed login is one failure, whatever went wrong. Unknown user, wrong
//     password and a malformed row all produce INVALID_CREDENTIALS, and the
//     wrong-user path still performs a bcrypt comparison so it takes the same
//     time. Otherwise the endpoint enumerates accounts.
//   - Logging in regenerates the session id. Without that, an attacker who
//     planted a session id before login still holds a valid session after it —
//     session fixation.
//   - A reset token is stored only as a hash. The plaintext goes to the user's
//     inbox and nowhere else, so a database leak does not hand over the ability
//     to reset every account.

// httpContextKey carries the fh request context to actions that need the
// response (session regeneration, cookie destruction).
type httpContextKey struct{}

func registerAuthActions(r *Registry) {
	mustAction(r, "auth.authenticate", authenticateAction, ActionInfo{
		Family:       "auth",
		Summary:      "Resolve the caller's identity from the request's credentials",
		ResourceKind: "auth",
		Provides:     "The principal",
		Kind:         "decision",
	})

	mustAction(r, "auth.password_hash", passwordHashAction, ActionInfo{
		Family:   "auth",
		Summary:  "Hash a password with bcrypt, enforcing a minimum length",
		Provides: "The bcrypt hash",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "value_fact", Type: "fact", Required: true},
			{Name: "cost", Type: "int", Default: "12"},
			{Name: "min_length", Type: "int", Default: "12"},
		},
	})

	mustAction(r, "auth.password_verify", passwordVerifyAction, ActionInfo{
		Family:  "auth",
		Summary: "Verify a password against a stored bcrypt hash",
		Kind:    "pure",
		Config: []ConfigField{
			{Name: "password_fact", Type: "fact", Required: true},
			{Name: "hash_fact", Type: "fact", Required: true},
		},
	})

	mustAction(r, "auth.login", loginAction, ActionInfo{
		Family:       "auth",
		Summary:      "Verify credentials against a user record and start a fresh session",
		ResourceKind: "session",
		Provides:     "The principal",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "user_fact", Type: "fact", Default: "user", Summary: "The user record, or a one-row result containing it"},
			{Name: "password_fact", Type: "fact", Default: "input.password"},
			{Name: "password_field", Type: "string", Default: "password_hash"},
			{Name: "id_field", Type: "string", Default: "id"},
			{Name: "username_field", Type: "string", Default: "email"},
			{Name: "email_field", Type: "string", Default: "email"},
			{Name: "tenant_field", Type: "string", Summary: "Column carrying the user's tenant"},
			{Name: "roles_field", Type: "string", Summary: "Column carrying the user's roles"},
			{Name: "disabled_field", Type: "string", Summary: "When truthy on the record, the login is refused"},
		},
	})

	mustAction(r, "auth.require_session", requireSessionAction, ActionInfo{
		Family:       "auth",
		Summary:      "Require an authenticated session and publish its subject",
		ResourceKind: "session",
		Provides:     "The session subject, usually the user id",
		Kind:         "decision",
		Config: []ConfigField{
			{Name: "key", Type: "string", Default: "user_id"},
		},
	})

	mustAction(r, "auth.logout", logoutAction, ActionInfo{
		Family:       "auth",
		Summary:      "Destroy the current session server-side and clear its cookie",
		ResourceKind: "session",
		Kind:         "effect",
	})

	mustAction(r, "auth.reset_token", resetTokenAction, ActionInfo{
		Family:   "auth",
		Summary:  "Mint a single-use reset token, publishing the plaintext and its hash separately",
		Provides: "An object with token, hash and expires_at",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "ttl", Type: "duration", Default: "15m"},
		},
	})

	mustAction(r, "auth.token_hash", tokenHashAction, ActionInfo{
		Family:   "auth",
		Summary:  "Hash a presented token so it can be compared against a stored hash",
		Provides: "The hex-encoded hash",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "value_fact", Type: "fact", Required: true},
		},
	})

	mustAction(r, "auth.jwt_issue", jwtIssueAction, ActionInfo{
		Family:       "auth",
		Summary:      "Mint a JWT for a principal",
		ResourceKind: "auth",
		Provides:     "An object with token and expires_at",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "principal_fact", Type: "fact", Required: true},
			{Name: "ttl", Type: "duration", Summary: "Defaults to the resource's own ttl"},
			{Name: "claims", Type: "map", Summary: "Extra claims; registered claims cannot be overridden"},
		},
	})

	mustAction(r, "auth.api_key_issue", apiKeyIssueAction, ActionInfo{
		Family:   "auth",
		Summary:  "Mint an API key, publishing the plaintext and its hash separately",
		Provides: "An object with key, hash and prefix",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "prefix", Type: "string", Default: "ak", Summary: "Human-readable prefix so a leaked key is identifiable"},
			{Name: "bytes", Type: "int", Default: "32"},
		},
	})

	mustAction(r, "auth.totp_verify", totpVerifyAction, ActionInfo{
		Family:  "auth",
		Summary: "Verify a time-based one-time code against a shared secret",
		Kind:    "pure",
		Config: []ConfigField{
			{Name: "secret_fact", Type: "fact", Required: true, Summary: "Base32 shared secret"},
			{Name: "code_fact", Type: "fact", Required: true},
			{Name: "digits", Type: "int", Default: "6"},
			{Name: "period", Type: "duration", Default: "30s"},
			{Name: "skew", Type: "int", Default: "1", Summary: "Adjacent time steps also accepted"},
		},
	})
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

var authenticateAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	resolved, ok := build.Resource(spec.Resource)
	if !ok {
		return nil, fmt.Errorf("node %q: resource %q is not declared", spec.Name, spec.Resource)
	}
	authenticator, ok := asAuthenticator(resolved)
	if !ok {
		return nil, fmt.Errorf("node %q: resource %q is not an authenticator", spec.Name, spec.Resource)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		// A route that already resolved the principal does not re-verify the
		// credential: re-running an OIDC verification per node would multiply the
		// work for no additional assurance.
		if ctx.Principal.ID != "" {
			return ActionResult{
				Outputs:  map[string]any{spec.Provides[0]: ctx.Principal},
				Decision: &Decision{Allow: true},
			}, nil
		}
		principal, err := authenticator.Authenticate(ctx.Context, credentialsFrom(ctx.Invocation))
		if err != nil {
			return ActionResult{Decision: &Decision{Allow: false, Message: "authentication failed"}}, errUnauthenticated
		}
		return ActionResult{
			Outputs:  map[string]any{spec.Provides[0]: principal},
			Decision: &Decision{Allow: true},
		}, nil
	}), nil
})

// ---------------------------------------------------------------------------
// Passwords
// ---------------------------------------------------------------------------

var passwordHashAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	valueFact, err := requiredString(spec.Config, "value_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	cost, err := configInt(spec.Config, "cost", 12)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		return nil, fmt.Errorf("node %q: bcrypt cost must be between %d and %d", spec.Name, bcrypt.MinCost, bcrypt.MaxCost)
	}
	minLength, err := configInt(spec.Config, "min_length", 12)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		password, err := factString(ctx.Inputs, valueFact)
		if err != nil {
			return ActionResult{}, intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: "a password is required"}
		}
		if len(password) < minLength {
			return ActionResult{}, intent.Failure{
				Code:     "WEAK_PASSWORD",
				Category: intent.CategoryInvalidInput,
				Message:  fmt.Sprintf("the password must contain at least %d characters", minLength),
			}
		}
		if len(password) > 72 {
			// bcrypt silently truncates at 72 bytes. A user who set a 90-character
			// passphrase would be able to log in with the first 72, which is not
			// what they were promised — so it is refused rather than truncated.
			return ActionResult{}, intent.Failure{
				Code:     "INVALID_INPUT",
				Category: intent.CategoryInvalidInput,
				Message:  "the password must be at most 72 characters",
			}
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, string(hash)), nil
	}), nil
})

var passwordVerifyAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	passwordFact, err := requiredString(spec.Config, "password_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	hashFact, err := requiredString(spec.Config, "hash_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		password, passwordErr := factString(ctx.Inputs, passwordFact)
		hash, hashErr := factString(ctx.Inputs, hashFact)
		if passwordErr != nil || hashErr != nil {
			_ = bcrypt.CompareHashAndPassword(dummyBcryptHash, []byte(password))
			return ActionResult{}, errInvalidCredentials
		}
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
			return ActionResult{}, errInvalidCredentials
		}
		return acknowledgement(spec, true), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Login and sessions
// ---------------------------------------------------------------------------

// loginConfig names the columns a user record carries, so one action serves any
// schema without the author having to reshape their table.
type loginConfig struct {
	userFact      string
	passwordFact  string
	passwordField string
	idField       string
	usernameField string
	emailField    string
	tenantField   string
	rolesField    string
	disabledField string
}

var loginAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	manager, err := requireResource[*session.SessionManager](build, spec, "a session resource")
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	cfg := loginConfig{
		userFact:      configString(spec.Config, "user_fact", "user"),
		passwordFact:  configString(spec.Config, "password_fact", "input.password"),
		passwordField: configString(spec.Config, "password_field", "password_hash"),
		idField:       configString(spec.Config, "id_field", "id"),
		usernameField: configString(spec.Config, "username_field", "email"),
		emailField:    configString(spec.Config, "email_field", "email"),
		tenantField:   configString(spec.Config, "tenant_field", ""),
		rolesField:    configString(spec.Config, "roles_field", ""),
		disabledField: configString(spec.Config, "disabled_field", ""),
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		record := singleRecord(ctx.Inputs[cfg.userFact])
		password, passwordErr := factString(ctx.Inputs, cfg.passwordFact)

		// The comparison runs even when there is no user, against a fixed dummy
		// hash, so a request for a nonexistent account costs the same as one for
		// a real account. Timing is the enumeration channel here.
		hash, _ := record[cfg.passwordField].(string)
		if record == nil || passwordErr != nil || hash == "" {
			_ = bcrypt.CompareHashAndPassword(dummyBcryptHash, []byte(password))
			return ActionResult{}, errInvalidCredentials
		}
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
			return ActionResult{}, errInvalidCredentials
		}
		if cfg.disabledField != "" && Truthy(record[cfg.disabledField]) {
			// Reported as a permission failure, not as invalid credentials: the
			// caller proved who they are, and telling them the account is disabled
			// is both true and actionable.
			return ActionResult{}, permissionDenied("this account is disabled")
		}

		sess, found := sessionFromContext(ctx.Context)
		httpCtx, httpFound := ctx.Context.Value(httpContextKey{}).(fh.Ctx)
		if !found || !httpFound {
			return ActionResult{}, intent.Failure{
				Code:     "SESSION_UNAVAILABLE",
				Category: intent.CategoryUnavailable,
				Message:  "this route has no session; add a session resource to the route",
			}
		}
		// Regenerate before writing anything into the session: this is what
		// closes session fixation.
		if err := manager.Regenerate(httpCtx, sess); err != nil {
			return ActionResult{}, err
		}

		principal := Principal{Claims: map[string]any{}}
		principal.ID = Stringify(record[cfg.idField])
		principal.Username = Stringify(record[cfg.usernameField])
		principal.Email = Stringify(record[cfg.emailField])
		if cfg.tenantField != "" {
			principal.TenantID = Stringify(record[cfg.tenantField])
		}
		if cfg.rolesField != "" {
			principal.Roles = stringSlice(record[cfg.rolesField])
		}
		for key, value := range record {
			// The password hash never enters the principal, which is exposed to
			// every subsequent node and serialised into the session.
			if key == cfg.passwordField {
				continue
			}
			principal.Claims[key] = value
		}

		sess.Set("user_id", principal.ID)
		sess.Set("username", principal.Username)
		if principal.TenantID != "" {
			sess.Set("tenant_id", principal.TenantID)
		}
		if len(principal.Roles) > 0 {
			sess.Set("roles", principal.Roles)
		}
		return singleOutput(spec, principal), nil
	}), nil
})

// singleRecord normalises the shapes a user lookup can produce: a bare object, a
// one-row query result, or a list containing one row.
func singleRecord(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	case []map[string]any:
		if len(typed) == 1 {
			return typed[0]
		}
	case []any:
		if len(typed) == 1 {
			if object, ok := typed[0].(map[string]any); ok {
				return object
			}
		}
	}
	return nil
}

var requireSessionAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	if _, err := requireResource[*session.SessionManager](build, spec, "a session resource"); err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	key := configString(spec.Config, "key", "user_id")

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, ok := sessionFromContext(ctx.Context)
		if !ok {
			return ActionResult{Decision: &Decision{Allow: false, Message: "authentication required"}},
				intent.Failure{Code: "SESSION_UNAVAILABLE", Category: intent.CategoryUnavailable, Message: "this route has no session"}
		}
		subject := sess.Get(key)
		if subject == nil || Stringify(subject) == "" {
			return ActionResult{Decision: &Decision{Allow: false, Message: "authentication required"}}, errUnauthenticated
		}
		return ActionResult{
			Outputs:  map[string]any{spec.Provides[0]: subject},
			Decision: &Decision{Allow: true},
		}, nil
	}), nil
})

var logoutAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	manager, err := requireResource[*session.SessionManager](build, spec, "a session resource")
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		sess, found := sessionFromContext(ctx.Context)
		httpCtx, httpFound := ctx.Context.Value(httpContextKey{}).(fh.Ctx)
		if !found || !httpFound {
			// Logging out of a session that is not there is not an error: the
			// caller's goal — not being signed in — is already met.
			return acknowledgement(spec, true), nil
		}
		if err := manager.Destroy(httpCtx, sess); err != nil {
			return ActionResult{}, err
		}
		return acknowledgement(spec, true), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

var resetTokenAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	ttl, err := configDuration(spec.Config, "ttl", 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("node %q: a reset token needs a positive ttl", spec.Name)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		token := newToken(32)
		digest := sha256.Sum256([]byte(token))
		// The plaintext is published for delivery; only the hash is meant to be
		// stored. Keeping them as separate fields makes the intended usage
		// visible in the BCL: store token.hash, send token.token.
		return singleOutput(spec, map[string]any{
			"token":      token,
			"hash":       hex.EncodeToString(digest[:]),
			"expires_at": ctx.Now.Add(ttl),
		}), nil
	}), nil
})

var tokenHashAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	valueFact, err := requiredString(spec.Config, "value_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, err := factString(ctx.Inputs, valueFact)
		if err != nil {
			return ActionResult{}, intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: "a token is required"}
		}
		digest := sha256.Sum256([]byte(value))
		return singleOutput(spec, hex.EncodeToString(digest[:])), nil
	}), nil
})

var jwtIssueAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	issuer, err := requireResource[*jwtAuth](build, spec, "an auth.jwt resource")
	if err != nil {
		return nil, err
	}
	if issuer.signing == nil {
		return nil, fmt.Errorf("node %q: auth resource %q has no signing key, so it cannot issue tokens", spec.Name, spec.Resource)
	}
	principalFact, err := requiredString(spec.Config, "principal_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	ttl, err := configDuration(spec.Config, "ttl", 0)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	extra := configMap(spec.Config, "claims")
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		raw, found := resolvePath(ctx.Inputs, principalFact)
		if !found {
			return ActionResult{}, invalidInput("no principal at %q", principalFact)
		}
		principal, err := principalFromValue(raw)
		if err != nil {
			return ActionResult{}, err
		}
		token, expires, err := issuer.Issue(principal, ttl, extra)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, map[string]any{"token": token, "expires_at": expires}), nil
	}), nil
})

// principalFromValue accepts either a real Principal or the object shape a
// database row or a previous node produced.
func principalFromValue(value any) (Principal, error) {
	switch typed := value.(type) {
	case Principal:
		return typed, nil
	case map[string]any:
		principal := Principal{
			ID:       Stringify(typed["id"]),
			Username: Stringify(typed["username"]),
			Email:    Stringify(typed["email"]),
			TenantID: Stringify(typed["tenant_id"]),
			Roles:    stringSlice(typed["roles"]),
			Scopes:   stringSlice(typed["scopes"]),
		}
		if claims, ok := typed["claims"].(map[string]any); ok {
			principal.Claims = claims
		}
		if principal.ID == "" {
			return Principal{}, invalidInput("the principal has no id")
		}
		return principal, nil
	default:
		return Principal{}, invalidInput("cannot read %T as a principal", value)
	}
}

var apiKeyIssueAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	prefix := configString(spec.Config, "prefix", "ak")
	size, err := configInt(spec.Config, "bytes", 32)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if size < 16 {
		return nil, fmt.Errorf("node %q: an API key needs at least 16 bytes of entropy", spec.Name)
	}
	return ActionFunc(func(*ActionContext) (ActionResult, error) {
		secret := newToken(size)
		key := prefix + "_" + secret
		// The prefix is published separately so an application can store it
		// alongside the hash and show the user which key is which without
		// keeping the secret.
		return singleOutput(spec, map[string]any{
			"key":    key,
			"hash":   hashKey(key),
			"prefix": prefix,
			"hint":   key[:min(len(key), len(prefix)+5)],
		}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// TOTP
// ---------------------------------------------------------------------------

var totpVerifyAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	secretFact, err := requiredString(spec.Config, "secret_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	codeFact, err := requiredString(spec.Config, "code_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	digits, err := configInt(spec.Config, "digits", 6)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if digits < 6 || digits > 8 {
		return nil, fmt.Errorf("node %q: totp digits must be 6, 7 or 8", spec.Name)
	}
	period, err := configDuration(spec.Config, "period", 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	skew, err := configInt(spec.Config, "skew", 1)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		secret, err := factString(ctx.Inputs, secretFact)
		if err != nil {
			return ActionResult{}, errInvalidCredentials
		}
		code, err := factString(ctx.Inputs, codeFact)
		if err != nil {
			return ActionResult{}, errInvalidCredentials
		}
		key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(strings.ReplaceAll(secret, " ", "")))
		if err != nil {
			return ActionResult{}, errInvalidCredentials
		}
		step := ctx.Now.Unix() / int64(period.Seconds())
		for offset := -skew; offset <= skew; offset++ {
			expected := totpCode(key, step+int64(offset), digits)
			if subtle.ConstantTimeCompare([]byte(expected), []byte(code)) == 1 {
				return acknowledgement(spec, true), nil
			}
		}
		return ActionResult{}, errInvalidCredentials
	}), nil
})

// totpCode computes RFC 6238 HOTP over a time step. SHA-1 is what every
// authenticator app implements; this is interoperability, not a security choice,
// and HMAC-SHA1 remains sound for this construction.
func totpCode(key []byte, step int64, digits int) string {
	var counter [8]byte
	binary.BigEndian.PutUint64(counter[:], uint64(step))
	mac := hmac.New(sha1.New, key)
	mac.Write(counter[:])
	sum := mac.Sum(nil)
	offset := sum[len(sum)-1] & 0x0f
	truncated := binary.BigEndian.Uint32(sum[offset:offset+4]) & 0x7fffffff
	modulus := uint32(1)
	for range digits {
		modulus *= 10
	}
	return fmt.Sprintf("%0*d", digits, truncated%modulus)
}

// errInvalidCredentials is the single failure every credential check returns.
var errInvalidCredentials = intent.Failure{
	Code:     "INVALID_CREDENTIALS",
	Category: intent.CategoryAuth,
	Message:  "invalid credentials",
}

var _ spi.Authenticator = (*chainAuth)(nil)
