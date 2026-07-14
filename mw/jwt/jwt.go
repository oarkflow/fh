package jwt

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

const ClaimsLocalKey = "jwt_claims"
const SubjectLocalKey = "jwt_subject"
const RolesLocalKey = "jwt_roles"
const PrincipalLocalKey = "fh.principal"

type KeyFunc func(ctx fh.Ctx, header map[string]any, claims map[string]any) ([]byte, error)
type ValidateFunc func(ctx fh.Ctx, header map[string]any, claims map[string]any) error
type ErrorHandler func(ctx fh.Ctx, err error) error

type Config struct {
	Header      string
	Scheme      string
	Secret      []byte
	KeyFunc     KeyFunc
	Algorithms  []string
	Leeway      time.Duration
	Audience    string
	Issuer      string
	ClaimsLocal string
	Error       ErrorHandler
	Validate    ValidateFunc
	Next        func(fh.Ctx) bool
	// Clock allows deterministic validation in tests. Defaults to time.Now.
	Clock func() time.Time
	// RequiredClaims must be present in the JWT claims set.
	RequiredClaims []string
	// Revoked returns true when a jti should be rejected.
	Revoked func(jti string) bool
	// PublicKeyPEM/PublicKeys enable RS*/ES* verification without a custom KeyFunc.
	// PublicKeys are selected by JWT header kid.
	PublicKeyPEM []byte
	PublicKeys   map[string][]byte

	// Principal integration. JWT is authentication only; authorization remains in
	// fh.RequireRole/RequirePermission/RequireScope or contrib/mw/authz.
	SetPrincipal     bool
	DisablePrincipal bool
	SubjectClaim     string
	TenantClaim      string
	RolesClaim       string
	ScopesClaim      string
	PermissionsClaim string
	PrincipalType    string
}

func New(cfg Config) fh.HandlerFunc {
	applyDefaults(&cfg)
	allowed := allowedAlgorithms(cfg.Algorithms)
	if cfg.Error == nil {
		cfg.Error = func(c fh.Ctx, err error) error {
			return c.Status(fh.StatusUnauthorized).JSON(fh.Map{"error": "jwt_invalid", "message": err.Error()})
		}
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		token := strings.TrimSpace(c.Get(cfg.Header))
		if token == "" {
			return cfg.Error(c, errors.New("missing token"))
		}
		if cfg.Scheme != "" {
			prefix := cfg.Scheme + " "
			if !strings.HasPrefix(strings.ToLower(token), strings.ToLower(prefix)) {
				return cfg.Error(c, errors.New("invalid token scheme"))
			}
			token = strings.TrimSpace(token[len(prefix):])
		}
		_, claims, err := Verify(c, token, cfg, allowed)
		if err != nil {
			return cfg.Error(c, err)
		}
		c.Locals(cfg.ClaimsLocal, claims)
		if sub := stringClaim(claims, cfg.SubjectClaim); sub != "" {
			c.Locals(SubjectLocalKey, sub)
		}
		roles := stringSliceClaim(claims, cfg.RolesClaim)
		if len(roles) > 0 {
			c.Locals(RolesLocalKey, roles)
		}
		if cfg.SetPrincipal && !cfg.DisablePrincipal {
			fh.SetPrincipal(c, principalFromClaims(claims, cfg, roles))
		}
		return c.Next()
	}
}

func Verify(c fh.Ctx, token string, cfg Config, allowed map[string]bool) (map[string]any, map[string]any, error) {
	applyDefaults(&cfg)
	if allowed == nil {
		allowed = allowedAlgorithms(cfg.Algorithms)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, nil, errors.New("malformed token")
	}
	hb, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, nil, fmt.Errorf("bad header encoding: %w", err)
	}
	cb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, nil, fmt.Errorf("bad claims encoding: %w", err)
	}
	var header map[string]any
	if err := json.Unmarshal(hb, &header); err != nil {
		return nil, nil, fmt.Errorf("bad header json: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(cb, &claims); err != nil {
		return nil, nil, fmt.Errorf("bad claims json: %w", err)
	}
	alg, _ := header["alg"].(string)
	alg = strings.ToUpper(alg)
	if !allowed[alg] {
		return nil, nil, fmt.Errorf("algorithm %s is not allowed", alg)
	}
	// Key selection is strictly bound to the token's algorithm family so a
	// public key configured for RS*/ES*/PS* verification can never be handed
	// to the HMAC path (and vice versa). Without this, a token with a forged
	// "alg":"HS256" header could be verified using an asymmetric public key's
	// bytes as the HMAC secret — public keys are not secret, so anyone with
	// the public key (routinely distributed, e.g. via JWKS) could forge
	// arbitrary tokens. See CVE class: JWT algorithm confusion.
	var key []byte
	_, isHMAC := jwtHash(alg)
	switch {
	case cfg.KeyFunc != nil:
		key, err = cfg.KeyFunc(c, header, claims)
		if err != nil || key == nil {
			if err == nil {
				err = ErrInvalidKey
			}
			return nil, nil, err
		}
		if isHMAC && isAsymmetricKey(key) {
			return nil, nil, fmt.Errorf("jwt: algorithm %s requires symmetric key, but KeyFunc returned asymmetric key", header["alg"])
		}
		if !isHMAC && !isAsymmetricKey(key) {
			return nil, nil, fmt.Errorf("jwt: algorithm %s requires asymmetric key, but KeyFunc returned symmetric key", header["alg"])
		}
	case isHMAC:
		key = cfg.Secret
	default:
		if kid, _ := header["kid"].(string); kid != "" && len(cfg.PublicKeys) > 0 {
			key = cfg.PublicKeys[kid]
		} else if len(cfg.PublicKeys) == 0 {
			key = cfg.PublicKeyPEM
		}
	}
	if len(key) == 0 {
		return nil, nil, errors.New("missing verification key")
	}
	got, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, nil, fmt.Errorf("bad signature encoding: %w", err)
	}
	if err := verifySignature(alg, key, []byte(parts[0]+"."+parts[1]), got); err != nil {
		return nil, nil, err
	}
	now := time.Now()
	if cfg.Clock != nil {
		now = cfg.Clock()
	}
	leeway := cfg.Leeway
	if exp, ok := numberTime(claims["exp"]); ok && now.After(exp.Add(leeway)) {
		return nil, nil, errors.New("token expired")
	}
	if nbf, ok := numberTime(claims["nbf"]); ok && now.Add(leeway).Before(nbf) {
		return nil, nil, errors.New("token not yet valid")
	}
	if iss, _ := claims["iss"].(string); cfg.Issuer != "" && iss != cfg.Issuer {
		return nil, nil, errors.New("issuer mismatch")
	}
	if cfg.Audience != "" && !hasAudience(claims["aud"], cfg.Audience) {
		return nil, nil, errors.New("audience mismatch")
	}
	for _, claim := range cfg.RequiredClaims {
		if strings.TrimSpace(claim) != "" {
			if _, ok := claims[claim]; !ok {
				return nil, nil, fmt.Errorf("missing required claim %s", claim)
			}
		}
	}
	if cfg.Revoked != nil {
		jti, _ := claims["jti"].(string)
		if jti != "" && cfg.Revoked(jti) {
			return nil, nil, errors.New("token revoked")
		}
	}
	if cfg.Validate != nil {
		if err := cfg.Validate(c, header, claims); err != nil {
			return nil, nil, err
		}
	}
	return header, claims, nil
}

// Sign creates a compact HMAC JWT for tests, examples, service-to-service calls,
// and local development. It supports HS256/HS384/HS512 and intentionally does
// not implement authorization; put roles/scopes/permissions in claims and let
// fh.Require* or contrib/mw/authz make access decisions.
func Sign(claims map[string]any, secret []byte, alg string) (string, error) {
	if len(secret) < 32 {
		return "", errors.New("jwt: signing secret must be at least 32 bytes")
	}
	if alg == "" {
		alg = "HS256"
	}
	alg = strings.ToUpper(alg)
	h, ok := jwtHash(alg)
	if !ok {
		return "", fmt.Errorf("algorithm %s is not supported", alg)
	}
	if claims == nil {
		claims = map[string]any{}
	}
	header := map[string]any{"alg": alg, "typ": "JWT"}
	hb, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	headerPart := base64.RawURLEncoding.EncodeToString(hb)
	claimsPart := base64.RawURLEncoding.EncodeToString(cb)
	mac := hmac.New(h, secret)
	mac.Write([]byte(headerPart + "." + claimsPart))
	return headerPart + "." + claimsPart + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func allowedAlgorithms(in []string) map[string]bool {
	if len(in) == 0 {
		in = []string{"HS256"}
	}
	allowed := map[string]bool{}
	for _, a := range in {
		allowed[strings.ToUpper(a)] = true
	}
	return allowed
}

func applyDefaults(cfg *Config) {
	if cfg.Header == "" {
		cfg.Header = "Authorization"
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "Bearer"
	}
	if cfg.ClaimsLocal == "" {
		cfg.ClaimsLocal = ClaimsLocalKey
	}
	if len(cfg.Algorithms) == 0 {
		cfg.Algorithms = []string{"HS256"}
	}
	if cfg.SubjectClaim == "" {
		cfg.SubjectClaim = "sub"
	}
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant_id"
	}
	if cfg.RolesClaim == "" {
		cfg.RolesClaim = "roles"
	}
	if cfg.ScopesClaim == "" {
		cfg.ScopesClaim = "scope"
	}
	if cfg.PermissionsClaim == "" {
		cfg.PermissionsClaim = "permissions"
	}
	if cfg.PrincipalType == "" {
		cfg.PrincipalType = "user"
	}
	// Default to true so JWT authentication immediately works with existing
	// core authorization helpers and contrib/mw/authz SubjectFromPrincipal.
	cfg.SetPrincipal = true
}

func principalFromClaims(claims map[string]any, cfg Config, roles []string) fh.Principal {
	return fh.Principal{
		ID:          stringClaim(claims, cfg.SubjectClaim),
		Type:        cfg.PrincipalType,
		TenantID:    stringClaim(claims, cfg.TenantClaim),
		Subject:     stringClaim(claims, cfg.SubjectClaim),
		Roles:       roles,
		Scopes:      stringSliceClaim(claims, cfg.ScopesClaim),
		Permissions: stringSliceClaim(claims, cfg.PermissionsClaim),
		Claims:      claims,
		AuthMethod:  "jwt",
	}
}

func jwtHash(alg string) (func() hash.Hash, bool) {
	switch alg {
	case "HS256":
		return sha256.New, true
	case "HS384":
		return sha512.New384, true
	case "HS512":
		return sha512.New, true
	default:
		return nil, false
	}
}
func numberTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case float64:
		return time.Unix(int64(x), 0), true
	case json.Number:
		i, _ := x.Int64()
		return time.Unix(i, 0), true
	case int64:
		return time.Unix(x, 0), true
	case int:
		return time.Unix(int64(x), 0), true
	default:
		return time.Time{}, false
	}
}
func hasAudience(v any, want string) bool {
	switch x := v.(type) {
	case string:
		return x == want
	case []any:
		for _, it := range x {
			if s, _ := it.(string); s == want {
				return true
			}
		}
	}
	return false
}
func stringClaim(claims map[string]any, key string) string {
	if key == "" || claims == nil {
		return ""
	}
	s, _ := claims[key].(string)
	return strings.TrimSpace(s)
}
func stringSliceClaim(claims map[string]any, key string) []string {
	if key == "" || claims == nil {
		return nil
	}
	switch v := claims[key].(type) {
	case string:
		return splitSpaceComma(v)
	case []string:
		return clean(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, it := range v {
			if s, _ := it.(string); s != "" {
				out = append(out, s)
			}
		}
		return clean(out)
	default:
		return nil
	}
}
func splitSpaceComma(s string) []string {
	if s == "" {
		return nil
	}
	repl := strings.NewReplacer(",", " ")
	parts := strings.Fields(repl.Replace(s))
	return clean(parts)
}
func clean(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

var ErrInvalidKey = errors.New("jwt: invalid key")

func isAsymmetricKey(key interface{}) bool {
	switch key.(type) {
	case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey:
		return true
	case []byte:
		return false
	default:
		return false
	}
}
