package platform

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/pkg/storage/kv"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/platform/spi"
	"golang.org/x/crypto/bcrypt"
)

// Authentication and authorization providers.
//
// Two rules hold across every provider here, and they are the reason this file
// is worth reading rather than skimming:
//
//  1. An absent credential is a failure, never an anonymous success. A route
//     that wants to be reachable without credentials says so with
//     allow_anonymous; a provider never decides that on its behalf.
//  2. Failures are indistinguishable to the caller. Wrong password, unknown
//     user, expired token and malformed token all produce the same
//     UNAUTHENTICATED result, because telling them apart is an enumeration
//     oracle.

func registerAuthResources(r *Registry) {
	mustResource(r, "auth.api_key", ResourceFactoryFunc(openAPIKeyAuth), ResourceKindInfo{
		Family:   "auth",
		Summary:  "Static API keys for service-to-service calls. Compared in constant time.",
		Provides: []string{"Authenticator"},
		Config: []ConfigField{
			{Name: "key", Type: "string", Summary: "A single key. Use keys for several."},
			{Name: "principal_id", Type: "string", Summary: "Identity the single key maps to"},
			{Name: "roles", Type: "[]string"},
			{Name: "scopes", Type: "[]string"},
			{Name: "keys", Type: "[]object", Summary: `keys [ { name … value … principal_id … roles […] } … ]`},
			{Name: "header", Type: "string", Default: "X-API-Key"},
		},
	})

	mustResource(r, "auth.basic", ResourceFactoryFunc(openBasicAuth), ResourceKindInfo{
		Family:   "auth",
		Summary:  "HTTP basic credentials verified against bcrypt hashes. For operator and machine access, not end users.",
		Provides: []string{"Authenticator"},
		Config: []ConfigField{
			{Name: "users", Type: "[]object", Required: true, Summary: `users [ { name … password_hash … roles […] } … ]`},
			{Name: "realm", Type: "string", Default: "restricted"},
		},
	})

	mustResource(r, "auth.jwt", ResourceFactoryFunc(openJWTAuth), ResourceKindInfo{
		Family:   "auth",
		Summary:  "Verifies bearer JWTs against a configured secret or public key. The algorithm comes from the key, never the token.",
		Provides: []string{"Authenticator", "JWTIssuer"},
		Config: []ConfigField{
			{Name: "algorithm", Type: "string", Default: "HS256", Summary: "HS256/384/512, RS256/384/512 or ES256/384/512"},
			{Name: "secret", Type: "string", Summary: "HMAC secret, at least 32 bytes"},
			{Name: "public_key", Type: "string", Summary: "PEM verification key, or a path when public_key_file is set"},
			{Name: "public_key_file", Type: "string"},
			{Name: "private_key_file", Type: "string", Summary: "Needed only to issue tokens"},
			{Name: "issuer", Type: "string", Summary: "Required iss claim"},
			{Name: "audience", Type: "string", Summary: "Required aud claim"},
			{Name: "skew", Type: "duration", Default: "1m"},
			{Name: "ttl", Type: "duration", Default: "1h", Summary: "Lifetime of tokens this resource issues"},
			{Name: "allow_missing_expiry", Type: "bool", Default: "false"},
			{Name: "subject_claim", Type: "string", Default: "sub"},
			{Name: "roles_claim", Type: "string", Default: "roles"},
			{Name: "scopes_claim", Type: "string", Default: "scope"},
			{Name: "tenant_claim", Type: "string", Default: "tenant_id"},
			{Name: "email_claim", Type: "string", Default: "email"},
			{Name: "username_claim", Type: "string", Default: "preferred_username"},
		},
	})

	mustResource(r, "auth.oidc", ResourceFactoryFunc(openOIDCAuth), ResourceKindInfo{
		Family:   "auth",
		Summary:  "Verifies bearer tokens against an OIDC provider's JWKS, discovered and cached with a refresh on unknown key ids.",
		Provides: []string{"Authenticator"},
		Config: []ConfigField{
			{Name: "issuer", Type: "string", Required: true, Summary: "Issuer URL; discovery appends /.well-known/openid-configuration"},
			{Name: "jwks_url", Type: "string", Summary: "Skip discovery and use this JWKS endpoint"},
			{Name: "audience", Type: "string", Required: true},
			{Name: "refresh_interval", Type: "duration", Default: "1h"},
			{Name: "timeout", Type: "duration", Default: "10s"},
			{Name: "skew", Type: "duration", Default: "1m"},
			{Name: "roles_claim", Type: "string", Default: "roles"},
			{Name: "scopes_claim", Type: "string", Default: "scope"},
			{Name: "tenant_claim", Type: "string", Default: "tenant_id"},
			{Name: "email_claim", Type: "string", Default: "email"},
			{Name: "username_claim", Type: "string", Default: "preferred_username"},
		},
	})

	mustResource(r, "auth.session", ResourceFactoryFunc(openSessionAuth), ResourceKindInfo{
		Family:   "auth",
		Summary:  "Resolves the caller from a signed session cookie, so a browser and an API client can share one identity model through auth.chain.",
		Provides: []string{"Authenticator"},
		Config: []ConfigField{
			{Name: "session", Type: "resource", Required: true, Summary: "The session resource whose store holds the session"},
			{Name: "subject_claim", Type: "string", Default: "user_id", Summary: "Session key holding the principal id"},
			{Name: "roles_claim", Type: "string", Default: "roles"},
			{Name: "tenant_claim", Type: "string", Default: "tenant_id"},
			{Name: "scopes_claim", Type: "string", Default: "scopes"},
			{Name: "username_claim", Type: "string", Default: "username"},
			{Name: "email_claim", Type: "string", Default: "email"},
		},
	})

	mustResource(r, "auth.chain", ResourceFactoryFunc(openChainAuth), ResourceKindInfo{
		Family:   "auth",
		Summary:  "Tries several authenticators in order and uses the first that succeeds — session cookie for browsers, bearer token for APIs.",
		Provides: []string{"Authenticator"},
		Config: []ConfigField{
			{Name: "authenticators", Type: "[]resource", Required: true, Summary: "Names of auth resources, tried in order"},
		},
	})

	mustResource(r, "authz.rbac", ResourceFactoryFunc(openRBAC), ResourceKindInfo{
		Family:   "authz",
		Summary:  "Role-based authorization over the document's role blocks, with inheritance flattened once at load time.",
		Provides: []string{"Authorizer"},
		Config: []ConfigField{
			{Name: "superuser_roles", Type: "[]string", Summary: "Roles that hold every permission"},
			{Name: "default_roles", Type: "[]string", Summary: "Roles granted to every authenticated principal"},
		},
	})
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

// apiKeyAuth maps opaque keys to principals.
//
// Lookup is by SHA-256 of the presented key rather than by the key itself, and
// the comparison is constant-time. Hashing first means the map is keyed by a
// fixed-width digest — a plain map lookup on the raw secret is a timing signal
// about key prefixes, small but free to avoid.
type apiKeyAuth struct {
	byDigest map[string]Principal
}

func openAPIKeyAuth(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("auth.api_key", spec.Config,
		"key", "principal_id", "roles", "scopes", "keys", "header"); err != nil {
		return nil, nil, err
	}
	auth := &apiKeyAuth{byDigest: map[string]Principal{}}

	add := func(value string, principal Principal) error {
		if len(value) < 16 {
			return fmt.Errorf("resource %q: an API key must contain at least 16 characters", spec.Name)
		}
		if principal.ID == "" {
			return fmt.Errorf("resource %q: every API key needs a principal_id", spec.Name)
		}
		auth.byDigest[hashKey(value)] = principal
		return nil
	}

	if single := configString(spec.Config, "key", ""); single != "" {
		principalID, err := requiredString(spec.Config, "principal_id")
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		}
		if err := add(single, Principal{
			ID:     principalID,
			Roles:  configStrings(spec.Config, "roles"),
			Scopes: configStrings(spec.Config, "scopes"),
		}); err != nil {
			return nil, nil, err
		}
	}
	for _, block := range configBlocks(spec.Config, "keys", "key_block") {
		value := configString(block, "value", "")
		if value == "" {
			return nil, nil, fmt.Errorf("resource %q: every key block needs a value", spec.Name)
		}
		if err := add(value, Principal{
			ID:       configString(block, "principal_id", configString(block, "name", "")),
			Username: configString(block, "username", ""),
			TenantID: configString(block, "tenant_id", ""),
			Roles:    configStrings(block, "roles"),
			Scopes:   configStrings(block, "scopes"),
		}); err != nil {
			return nil, nil, err
		}
	}
	if len(auth.byDigest) == 0 {
		return nil, nil, fmt.Errorf("resource %q: auth.api_key needs at least one key", spec.Name)
	}
	return auth, nil, nil
}

// Authenticate implements spi.Authenticator.
func (a *apiKeyAuth) Authenticate(_ context.Context, creds Credentials) (Principal, error) {
	// A bearer token is accepted as an API key too: plenty of clients send a
	// static key in an Authorization header, and refusing that only produces a
	// confusing 401 for a correct key.
	presented := creds.APIKey
	if presented == "" {
		presented = creds.BearerToken
	}
	if presented == "" {
		return Principal{}, errUnauthenticated
	}
	digest := hashKey(presented)
	principal, found := a.byDigest[digest]
	if !found {
		return Principal{}, errUnauthenticated
	}
	// The digest was already matched by the map; this second comparison keeps
	// the code honest about constant-time intent if the lookup ever changes.
	if subtle.ConstantTimeCompare([]byte(digest), []byte(hashKey(presented))) != 1 {
		return Principal{}, errUnauthenticated
	}
	return principal, nil
}

func hashKey(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// HTTP basic
// ---------------------------------------------------------------------------

type basicAuth struct {
	users map[string]basicUser
	realm string
}

type basicUser struct {
	hash      []byte
	principal Principal
}

func openBasicAuth(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("auth.basic", spec.Config, "users", "realm"); err != nil {
		return nil, nil, err
	}
	auth := &basicAuth{users: map[string]basicUser{}, realm: configString(spec.Config, "realm", "restricted")}
	for _, block := range configBlocks(spec.Config, "users", "user") {
		name := configString(block, "name", configString(block, "username", ""))
		if name == "" {
			return nil, nil, fmt.Errorf("resource %q: every user block needs a name", spec.Name)
		}
		hash := configString(block, "password_hash", "")
		if hash == "" {
			return nil, nil, fmt.Errorf("resource %q: user %q needs a bcrypt password_hash — plain passwords are not accepted in configuration", spec.Name, name)
		}
		if _, err := bcrypt.Cost([]byte(hash)); err != nil {
			return nil, nil, fmt.Errorf("resource %q: user %q password_hash is not a bcrypt hash: %w", spec.Name, name, err)
		}
		auth.users[name] = basicUser{
			hash: []byte(hash),
			principal: Principal{
				ID:       configString(block, "principal_id", name),
				Username: name,
				Email:    configString(block, "email", ""),
				TenantID: configString(block, "tenant_id", ""),
				Roles:    configStrings(block, "roles"),
				Scopes:   configStrings(block, "scopes"),
			},
		}
	}
	if len(auth.users) == 0 {
		return nil, nil, fmt.Errorf("resource %q: auth.basic needs at least one user", spec.Name)
	}
	return auth, nil, nil
}

// Authenticate implements spi.Authenticator.
func (a *basicAuth) Authenticate(_ context.Context, creds Credentials) (Principal, error) {
	if creds.Username == "" || creds.Password == "" {
		return Principal{}, errUnauthenticated
	}
	user, found := a.users[creds.Username]
	if !found {
		// Hash anyway against a dummy cost so an unknown username takes about as
		// long as a known one. Without this, response timing enumerates users.
		_ = bcrypt.CompareHashAndPassword(dummyBcryptHash, []byte(creds.Password))
		return Principal{}, errUnauthenticated
	}
	if err := bcrypt.CompareHashAndPassword(user.hash, []byte(creds.Password)); err != nil {
		return Principal{}, errUnauthenticated
	}
	return user.principal, nil
}

// dummyBcryptHash is a valid cost-10 hash of a value nothing will ever present.
// It exists purely so an unknown-username path performs the same work as a known
// one.
var dummyBcryptHash = []byte("$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy")

// ---------------------------------------------------------------------------
// JWT
// ---------------------------------------------------------------------------

// claimMapping names the claims a token's fields are read from, so one provider
// serves Auth0, Keycloak, Entra and a hand-rolled issuer without code changes.
type claimMapping struct {
	subject  string
	username string
	email    string
	roles    string
	scopes   string
	tenant   string
}

func claimMappingFrom(config map[string]any) claimMapping {
	return claimMapping{
		subject:  configString(config, "subject_claim", "sub"),
		username: configString(config, "username_claim", "preferred_username"),
		email:    configString(config, "email_claim", "email"),
		roles:    configString(config, "roles_claim", "roles"),
		scopes:   configString(config, "scopes_claim", "scope"),
		tenant:   configString(config, "tenant_claim", "tenant_id"),
	}
}

// principalFrom projects a verified claim set onto a Principal.
func (m claimMapping) principalFrom(claims JWTClaims, body map[string]any) Principal {
	principal := Principal{
		ID:     claimString(body, m.subject, claims.Subject),
		Claims: body,
	}
	principal.Username = claimString(body, m.username, "")
	principal.Email = claimString(body, m.email, "")
	principal.TenantID = claimString(body, m.tenant, "")
	principal.Roles = claimStrings(body, m.roles)
	principal.Scopes = claimStrings(body, m.scopes)
	// Keycloak nests realm roles; reading them costs nothing and saves every
	// Keycloak deployment a custom mapper.
	if len(principal.Roles) == 0 {
		if realm, ok := body["realm_access"].(map[string]any); ok {
			principal.Roles = claimStrings(realm, "roles")
		}
	}
	return principal
}

func claimString(body map[string]any, path, fallback string) string {
	if path == "" {
		return fallback
	}
	value, found := lookupPath(body, strings.Split(path, "."))
	if !found {
		return fallback
	}
	if text := Stringify(value); text != "" {
		return text
	}
	return fallback
}

// claimStrings reads a claim that may be a list or a space-delimited string —
// "roles" is conventionally the former and "scope" the latter, and a provider
// should not have to care which it got.
func claimStrings(body map[string]any, path string) []string {
	if path == "" {
		return nil
	}
	value, found := lookupPath(body, strings.Split(path, "."))
	if !found {
		return nil
	}
	switch typed := value.(type) {
	case string:
		return strings.Fields(strings.ReplaceAll(typed, ",", " "))
	default:
		return stringSlice(value)
	}
}

type jwtAuth struct {
	keys    map[string]*JWTKey
	signing *JWTKey
	verify  JWTVerifyOptions
	mapping claimMapping
	ttl     time.Duration
	issuer  string
}

func openJWTAuth(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("auth.jwt", spec.Config,
		"algorithm", "secret", "public_key", "public_key_file", "private_key", "private_key_file",
		"issuer", "audience", "skew", "ttl", "allow_missing_expiry",
		"subject_claim", "roles_claim", "scopes_claim", "tenant_claim", "email_claim", "username_claim"); err != nil {
		return nil, nil, err
	}
	algorithm, err := parseJWTAlgorithm(configString(spec.Config, "algorithm", "HS256"))
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	skew, err := configDuration(spec.Config, "skew", time.Minute)
	if err != nil {
		return nil, nil, err
	}
	ttl, err := configDuration(spec.Config, "ttl", time.Hour)
	if err != nil {
		return nil, nil, err
	}

	auth := &jwtAuth{
		keys:    map[string]*JWTKey{},
		mapping: claimMappingFrom(spec.Config),
		ttl:     ttl,
		issuer:  configString(spec.Config, "issuer", ""),
		verify: JWTVerifyOptions{
			Issuer:             configString(spec.Config, "issuer", ""),
			Audience:           configString(spec.Config, "audience", ""),
			Skew:               skew,
			AllowMissingExpiry: configBool(spec.Config, "allow_missing_expiry", false),
		},
	}

	switch {
	case strings.HasPrefix(string(algorithm), "HS"):
		secret, err := requiredString(spec.Config, "secret")
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %s needs config.secret: %w", spec.Name, algorithm, err)
		}
		key, err := NewHMACKey(algorithm, []byte(secret))
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		}
		auth.keys[""] = key
		auth.signing = key
	default:
		material, err := readKeyMaterial(spec.Config, "public_key")
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		}
		if len(material) == 0 {
			return nil, nil, fmt.Errorf("resource %q: %s needs config.public_key or config.public_key_file", spec.Name, algorithm)
		}
		key, err := ParsePublicKeyPEM(algorithm, material)
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		}
		auth.keys[""] = key
		if private, err := readKeyMaterial(spec.Config, "private_key"); err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		} else if len(private) > 0 {
			signing, err := ParsePrivateKeyPEM(algorithm, private)
			if err != nil {
				return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
			}
			signing.Algorithm = algorithm
			auth.signing = signing
		}
	}
	return auth, nil, nil
}

// readKeyMaterial reads key bytes from either an inline PEM value or a file.
func readKeyMaterial(config map[string]any, key string) ([]byte, error) {
	if inline := configString(config, key, ""); inline != "" {
		return []byte(strings.ReplaceAll(inline, `\n`, "\n")), nil
	}
	path := configString(config, key+"_file", "")
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key+"_file", err)
	}
	return data, nil
}

// Authenticate implements spi.Authenticator.
func (a *jwtAuth) Authenticate(_ context.Context, creds Credentials) (Principal, error) {
	if creds.BearerToken == "" {
		return Principal{}, errUnauthenticated
	}
	claims, body, err := VerifyJWT(creds.BearerToken, a.keys, a.verify)
	if err != nil {
		return Principal{}, errUnauthenticated
	}
	principal := a.mapping.principalFrom(claims, body)
	if principal.ID == "" {
		return Principal{}, errUnauthenticated
	}
	return principal, nil
}

// Issue mints a token for a principal. It is what the auth.jwt_issue action
// calls after a login intent has verified credentials.
func (a *jwtAuth) Issue(principal Principal, ttl time.Duration, extra map[string]any) (string, time.Time, error) {
	if a.signing == nil || !a.signing.CanSign() {
		return "", time.Time{}, errors.New("this auth.jwt resource has no signing key: set secret, or private_key_file for an asymmetric algorithm")
	}
	if ttl <= 0 {
		ttl = a.ttl
	}
	now := time.Now().UTC()
	expires := now.Add(ttl)
	claims := JWTClaims{
		Issuer:    a.issuer,
		Subject:   principal.ID,
		ExpiresAt: expires.Unix(),
		IssuedAt:  now.Unix(),
		NotBefore: now.Unix(),
		ID:        newRandomID(),
	}
	if a.verify.Audience != "" {
		claims.Audience = []string{a.verify.Audience}
	}
	body := map[string]any{}
	for name, value := range extra {
		body[name] = value
	}
	if principal.Username != "" {
		body[a.mapping.username] = principal.Username
	}
	if principal.Email != "" {
		body[a.mapping.email] = principal.Email
	}
	if principal.TenantID != "" {
		body[a.mapping.tenant] = principal.TenantID
	}
	if len(principal.Roles) > 0 {
		body[a.mapping.roles] = principal.Roles
	}
	if len(principal.Scopes) > 0 {
		body[a.mapping.scopes] = strings.Join(principal.Scopes, " ")
	}
	token, err := SignJWT(a.signing, claims, body)
	return token, expires, err
}

// ---------------------------------------------------------------------------
// OIDC
// ---------------------------------------------------------------------------

// oidcAuth verifies tokens against a provider's published JWKS.
//
// The key set is cached and refreshed on an interval, and additionally refreshed
// on demand when a token presents an unknown key id — but at most once per
// minute. Both halves matter: without on-demand refresh a provider's key
// rotation causes an outage until the next interval; without the rate limit, a
// stream of tokens with bogus key ids becomes a request amplifier against the
// provider.
type oidcAuth struct {
	client      *http.Client
	jwksURL     string
	verify      JWTVerifyOptions
	mapping     claimMapping
	refresh     time.Duration
	mu          sync.RWMutex
	keys        map[string]*JWTKey
	fetchedAt   time.Time
	lastAttempt time.Time
}

func openOIDCAuth(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("auth.oidc", spec.Config,
		"issuer", "jwks_url", "audience", "refresh_interval", "timeout", "skew",
		"roles_claim", "scopes_claim", "tenant_claim", "email_claim", "username_claim", "subject_claim"); err != nil {
		return nil, nil, err
	}
	issuer, err := requiredString(spec.Config, "issuer")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	audience, err := requiredString(spec.Config, "audience")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: auth.oidc requires config.audience — a token minted for another application must not authenticate here: %w", spec.Name, err)
	}
	timeout, err := configDuration(spec.Config, "timeout", 10*time.Second)
	if err != nil {
		return nil, nil, err
	}
	refresh, err := configDuration(spec.Config, "refresh_interval", time.Hour)
	if err != nil {
		return nil, nil, err
	}
	skew, err := configDuration(spec.Config, "skew", time.Minute)
	if err != nil {
		return nil, nil, err
	}

	auth := &oidcAuth{
		client:  &http.Client{Timeout: timeout},
		jwksURL: configString(spec.Config, "jwks_url", ""),
		refresh: refresh,
		mapping: claimMappingFrom(spec.Config),
		verify: JWTVerifyOptions{
			Issuer:   issuer,
			Audience: audience,
			Skew:     skew,
		},
	}
	if auth.jwksURL == "" {
		discovered, err := auth.discover(ctx, issuer)
		if err != nil {
			return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
		}
		auth.jwksURL = discovered
	}
	if _, err := url.ParseRequestURI(auth.jwksURL); err != nil {
		return nil, nil, fmt.Errorf("resource %q: jwks_url is not a valid URL: %w", spec.Name, err)
	}
	// Fetch once at load so a misconfigured issuer fails the deployment rather
	// than every request.
	if err := auth.fetch(ctx); err != nil {
		return nil, nil, fmt.Errorf("resource %q: fetch JWKS: %w", spec.Name, err)
	}
	return auth, nil, nil
}

func (a *oidcAuth) discover(ctx context.Context, issuer string) (string, error) {
	endpoint := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("OIDC discovery: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OIDC discovery returned %s", response.Status)
	}
	var document struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return "", fmt.Errorf("OIDC discovery: %w", err)
	}
	if document.JWKSURI == "" {
		return "", errors.New("OIDC discovery document has no jwks_uri")
	}
	if document.Issuer != "" && document.Issuer != strings.TrimSuffix(issuer, "/") && document.Issuer != issuer {
		// A discovery document whose issuer disagrees with the configured one is
		// either a misconfiguration or a redirect to somewhere else. Neither is
		// safe to trust for key material.
		return "", fmt.Errorf("OIDC discovery issuer %q does not match configured issuer %q", document.Issuer, issuer)
	}
	return document.JWKSURI, nil
}

func (a *oidcAuth) fetch(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, a.jwksURL, nil)
	if err != nil {
		return err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	keys, err := parseJWKS(body)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.keys = keys
	a.fetchedAt = time.Now()
	a.mu.Unlock()
	return nil
}

func (a *oidcAuth) currentKeys(ctx context.Context) map[string]*JWTKey {
	a.mu.RLock()
	keys, fetchedAt := a.keys, a.fetchedAt
	a.mu.RUnlock()
	if time.Since(fetchedAt) < a.refresh {
		return keys
	}
	if err := a.fetch(ctx); err != nil {
		// A refresh failure keeps serving the cached keys. They are almost
		// certainly still valid, and rejecting every request because the
		// provider's endpoint blipped would turn their outage into ours.
		return keys
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.keys
}

// Authenticate implements spi.Authenticator.
func (a *oidcAuth) Authenticate(ctx context.Context, creds Credentials) (Principal, error) {
	if creds.BearerToken == "" {
		return Principal{}, errUnauthenticated
	}
	keys := a.currentKeys(ctx)
	claims, body, err := VerifyJWT(creds.BearerToken, keys, a.verify)
	if err != nil {
		if a.refreshForUnknownKey(ctx, creds.BearerToken, keys) {
			keys = a.currentKeys(ctx)
			claims, body, err = VerifyJWT(creds.BearerToken, keys, a.verify)
		}
		if err != nil {
			return Principal{}, errUnauthenticated
		}
	}
	principal := a.mapping.principalFrom(claims, body)
	if principal.ID == "" {
		return Principal{}, errUnauthenticated
	}
	return principal, nil
}

// refreshForUnknownKey re-fetches the key set when the token names a key id we
// have never seen, at most once a minute. It reports whether a refresh happened.
func (a *oidcAuth) refreshForUnknownKey(ctx context.Context, token string, known map[string]*JWTKey) bool {
	kid := tokenKeyID(token)
	if kid == "" {
		return false
	}
	if _, found := known[kid]; found {
		return false
	}
	a.mu.Lock()
	if time.Since(a.lastAttempt) < time.Minute {
		a.mu.Unlock()
		return false
	}
	a.lastAttempt = time.Now()
	a.mu.Unlock()
	return a.fetch(ctx) == nil
}

func tokenKeyID(token string) string {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) < 2 {
		return ""
	}
	decoded, err := base64Raw.DecodeString(parts[0])
	if err != nil {
		return ""
	}
	var header struct {
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(decoded, &header); err != nil {
		return ""
	}
	return header.Kid
}

// ---------------------------------------------------------------------------
// Chained authenticators
// ---------------------------------------------------------------------------

// chainAuth tries each authenticator in order. It is the ordinary shape of a
// real application: a browser presents a session cookie, a mobile client a
// bearer token, a partner an API key, and all three are the same user model.
type chainAuth struct {
	links []spi.Authenticator
	names []string
}

func openChainAuth(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("auth.chain", spec.Config, "authenticators"); err != nil {
		return nil, nil, err
	}
	names := configStrings(spec.Config, "authenticators")
	if len(names) == 0 {
		return nil, nil, fmt.Errorf("resource %q: auth.chain needs config.authenticators", spec.Name)
	}
	chain := &chainAuth{names: names}
	for _, name := range names {
		resolved, ok := spec.resolved[name]
		if !ok {
			return nil, nil, fmt.Errorf("resource %q: auth.chain names unknown resource %q", spec.Name, name)
		}
		authenticator, ok := asAuthenticator(resolved)
		if !ok {
			return nil, nil, fmt.Errorf("resource %q: resource %q is not an authenticator", spec.Name, name)
		}
		chain.links = append(chain.links, authenticator)
	}
	return chain, nil, nil
}

// Authenticate implements spi.Authenticator.
func (c *chainAuth) Authenticate(ctx context.Context, creds Credentials) (Principal, error) {
	for _, link := range c.links {
		principal, err := link.Authenticate(ctx, creds)
		if err == nil && principal.ID != "" {
			return principal, nil
		}
	}
	return Principal{}, errUnauthenticated
}

// asAuthenticator accepts either SPI shape, so host code written against the
// older invocation.PrincipalHint signature keeps working.
func asAuthenticator(resource Resource) (spi.Authenticator, bool) {
	switch typed := resource.(type) {
	case spi.Authenticator:
		return typed, true
	case LegacyAuthenticator:
		return CredentialAuthenticatorFunc(func(ctx context.Context, creds Credentials) (Principal, error) {
			return typed.Authenticate(ctx, credentialsToHint(creds))
		}), true
	default:
		return nil, false
	}
}

// ---------------------------------------------------------------------------
// RBAC
// ---------------------------------------------------------------------------

// rbacAuthorizer answers permission questions from the document's role blocks.
//
// Inheritance is flattened once at load time into a role-to-permission-set map,
// so a request-time check is a map lookup rather than a graph walk. Cycles in
// the inheritance graph are rejected during flattening — a cycle is a
// configuration error, and resolving it "as far as possible" would silently
// grant a different permission set than the author wrote.
type rbacAuthorizer struct {
	roles        map[string]map[string]struct{}
	wildcards    map[string][]string
	superusers   map[string]struct{}
	defaultRoles []string
}

func openRBAC(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("authz.rbac", spec.Config, "superuser_roles", "default_roles", "roles"); err != nil {
		return nil, nil, err
	}
	authorizer := &rbacAuthorizer{
		roles:        map[string]map[string]struct{}{},
		wildcards:    map[string][]string{},
		superusers:   map[string]struct{}{},
		defaultRoles: configStrings(spec.Config, "default_roles"),
	}
	for _, role := range configStrings(spec.Config, "superuser_roles") {
		authorizer.superusers[role] = struct{}{}
	}
	// Role definitions come from the document's role blocks, injected by the
	// compiler rather than duplicated into this resource's config, so one role
	// set serves every authorizer in the application.
	specs, _ := spec.Config["roles"].([]RoleSpec)
	if err := authorizer.load(specs); err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	return authorizer, nil, nil
}

func (a *rbacAuthorizer) load(specs []RoleSpec) error {
	direct := make(map[string]RoleSpec, len(specs))
	for _, role := range specs {
		if _, exists := direct[role.Name]; exists {
			return fmt.Errorf("duplicate role %q", role.Name)
		}
		direct[role.Name] = role
	}
	for _, role := range specs {
		for _, parent := range role.Inherits {
			if _, ok := direct[parent]; !ok {
				return fmt.Errorf("role %q inherits unknown role %q", role.Name, parent)
			}
		}
	}
	var resolve func(name string, path map[string]bool) (map[string]struct{}, error)
	resolve = func(name string, path map[string]bool) (map[string]struct{}, error) {
		if existing, done := a.roles[name]; done {
			return existing, nil
		}
		if path[name] {
			return nil, fmt.Errorf("role %q participates in an inheritance cycle", name)
		}
		path[name] = true
		defer delete(path, name)

		permissions := map[string]struct{}{}
		role := direct[name]
		for _, permission := range role.Permissions {
			permissions[permission] = struct{}{}
		}
		for _, parent := range role.Inherits {
			inherited, err := resolve(parent, path)
			if err != nil {
				return nil, err
			}
			for permission := range inherited {
				permissions[permission] = struct{}{}
			}
		}
		a.roles[name] = permissions
		return permissions, nil
	}
	for _, role := range specs {
		if _, err := resolve(role.Name, map[string]bool{}); err != nil {
			return err
		}
	}
	// Precompute the wildcard prefixes each role grants, so a check for
	// "order:refund" against a role holding "order:*" is still a lookup plus a
	// short prefix scan rather than a scan of every permission.
	for role, permissions := range a.roles {
		for permission := range permissions {
			if strings.HasSuffix(permission, ":*") {
				a.wildcards[role] = append(a.wildcards[role], strings.TrimSuffix(permission, "*"))
			} else if permission == "*" {
				a.wildcards[role] = append(a.wildcards[role], "")
			}
		}
		slices.Sort(a.wildcards[role])
	}
	return nil
}

// HasPermission implements spi.Authorizer.
func (a *rbacAuthorizer) HasPermission(_ context.Context, principal Principal, permission string) (bool, error) {
	if permission == "" {
		return true, nil
	}
	for _, role := range a.effectiveRoles(principal) {
		if _, superuser := a.superusers[role]; superuser {
			return true, nil
		}
		held, known := a.roles[role]
		if !known {
			continue
		}
		if _, ok := held[permission]; ok {
			return true, nil
		}
		for _, prefix := range a.wildcards[role] {
			if strings.HasPrefix(permission, prefix) {
				return true, nil
			}
		}
	}
	// A principal's scopes are honoured as permissions too, which is what lets
	// an OIDC access token carry fine-grained grants without every scope also
	// having to exist as a role.
	return principal.HasScope(permission), nil
}

// Permissions implements spi.Authorizer.
func (a *rbacAuthorizer) Permissions(_ context.Context, principal Principal) ([]string, error) {
	seen := map[string]struct{}{}
	for _, role := range a.effectiveRoles(principal) {
		if _, superuser := a.superusers[role]; superuser {
			return []string{"*"}, nil
		}
		for permission := range a.roles[role] {
			seen[permission] = struct{}{}
		}
	}
	for _, scope := range principal.Scopes {
		seen[scope] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for permission := range seen {
		out = append(out, permission)
	}
	slices.Sort(out)
	return out, nil
}

func (a *rbacAuthorizer) effectiveRoles(principal Principal) []string {
	if len(a.defaultRoles) == 0 {
		return principal.Roles
	}
	return append(slices.Clone(principal.Roles), a.defaultRoles...)
}

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

// errUnauthenticated is the single failure every authenticator returns. It maps
// to HTTP 401 and carries no detail about why.
var errUnauthenticated = intent.Failure{
	Code:     "UNAUTHENTICATED",
	Category: intent.CategoryAuth,
	Message:  "authentication failed",
}

var (
	_ spi.Authenticator = (*apiKeyAuth)(nil)
	_ spi.Authenticator = (*basicAuth)(nil)
	_ spi.Authenticator = (*jwtAuth)(nil)
	_ spi.Authenticator = (*oidcAuth)(nil)
	_ spi.Authenticator = (*chainAuth)(nil)
	_ spi.Authorizer    = (*rbacAuthorizer)(nil)
)

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

// sessionAuth turns a signed session cookie into a principal.
//
// It exists so auth.chain can mean what it says: a browser presents a cookie, an
// API client presents a bearer token, and the route enforces one identity model
// over both. Without it, a route that names an authenticator refuses every
// cookie-bearing request — which fails closed, correctly, but leaves no way to
// build an application that serves both.
//
// The session is read from the store by the id the transport already verified,
// never from the cookie value itself: signature checking belongs to the session
// manager, and doing it twice in two places is how the two drift apart.
type sessionAuth struct {
	name     string
	store    kv.Store
	subject  string
	roles    string
	tenant   string
	scopes   string
	username string
	email    string
}

func openSessionAuth(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("auth.session", spec.Config,
		"session", "subject_claim", "roles_claim", "tenant_claim", "scopes_claim",
		"username_claim", "email_claim"); err != nil {
		return nil, nil, err
	}
	name, err := requiredString(spec.Config, "session")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	resolved, ok := spec.resolved[name]
	if !ok {
		return nil, nil, fmt.Errorf("resource %q: auth.session names unknown resource %q", spec.Name, name)
	}
	manager, ok := resolved.(*SessionManager)
	if !ok {
		return nil, nil, fmt.Errorf("resource %q: resource %q is not a session manager. Use session.sql, session.file or session.memory", spec.Name, name)
	}
	return &sessionAuth{
		name:     name,
		store:    manager.Store(),
		subject:  configString(spec.Config, "subject_claim", "user_id"),
		roles:    configString(spec.Config, "roles_claim", "roles"),
		tenant:   configString(spec.Config, "tenant_claim", "tenant_id"),
		scopes:   configString(spec.Config, "scopes_claim", "scopes"),
		username: configString(spec.Config, "username_claim", "username"),
		email:    configString(spec.Config, "email_claim", "email"),
	}, nil, nil
}

// Authenticate implements spi.Authenticator.
func (s *sessionAuth) Authenticate(_ context.Context, creds Credentials) (Principal, error) {
	if creds.SessionID == "" {
		return Principal{}, errUnauthenticated
	}
	stored, err := session.GetSession(s.store, creds.SessionID)
	if err != nil || stored == nil || stored.Expired() {
		// A store failure is indistinguishable from an unknown session, on purpose:
		// the caller learns only that they are not authenticated.
		return Principal{}, errUnauthenticated
	}
	subject := Stringify(stored.Get(s.subject))
	if subject == "" {
		// A session exists but holds no identity — an anonymous visitor's session.
		// That is not an authentication.
		return Principal{}, errUnauthenticated
	}
	return Principal{
		ID:       subject,
		Username: Stringify(stored.Get(s.username)),
		Email:    Stringify(stored.Get(s.email)),
		TenantID: Stringify(stored.Get(s.tenant)),
		Roles:    stringSlice(stored.Get(s.roles)),
		Scopes:   stringSlice(stored.Get(s.scopes)),
	}, nil
}
