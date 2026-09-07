// Package csrf implements a signed, browser-session-bound, origin-aware
// double-submit CSRF defense for fh.
//
// Security properties:
//   - HMAC-SHA256 signed CSRF tokens with signing-key rotation.
//   - Tokens are bound to an HttpOnly browser-binding cookie and may also be
//     bound to an application/authentication session via SessionBinding.
//   - Exact scheme + host + effective-port Origin/Referer validation.
//   - Fetch Metadata validation (Sec-Fetch-Site) as defense in depth.
//   - Strict Origin validation for WebSocket handshakes.
//   - Secure, SameSite=Lax, __Host- cookies by default.
//   - Constant-time comparison of submitted and cookie tokens.
//   - Header and HTML form token extraction; custom extraction is supported.
//
// For horizontally scaled production deployments, configure Secret or Keys so
// every instance shares the same signing material. If neither is configured, a
// cryptographically random process-local key is used; that is secure for a
// single process but tokens will not survive restarts or work across replicas.
package csrf

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

const (
	tokenVersion         = "v1"
	defaultKeyID         = "default"
	defaultTokenLocalKey = "csrf_token"
	defaultCookieName    = "__Host-csrf"
	defaultBindingName   = "__Host-csrf-bind"
	insecureCookieName   = "csrf_token"
	insecureBindingName  = "csrf_bind"
	defaultHeaderName    = "X-CSRF-Token"
	defaultFormField     = "_csrf"
	nonceSize            = 32
	bindingSize          = 32
	macSize              = sha256.Size
	minimumSecretSize    = 32
	maximumTokenLength   = 512
	maximumBindingLength = 128
	maximumKeyIDLength   = 32
)

var (
	ErrInvalidConfig = errors.New("csrf: invalid configuration")
	ErrInvalidToken  = errors.New("csrf: invalid token")

	processSecret = mustRandom(minimumSecretSize)
)

// Key is an HMAC signing key. The first configured key signs newly-issued
// tokens. All configured keys verify existing tokens, allowing safe rotation.
//
// ID is embedded in the token and must consist only of ASCII letters, digits,
// '-' or '_'. Secret must be at least 32 bytes.
type Key struct {
	ID     string
	Secret []byte
}

// SessionBinding returns stable, non-secret bytes identifying the current
// application/authentication session. When configured, CSRF tokens become
// invalid automatically when that binding changes (for example after login,
// logout, account switching, or session-ID regeneration).
//
// Do not return a password, access token, or other credential. A random internal
// session ID is ideal. The binding is used only as HMAC input and is never sent
// to the client by this package.
type SessionBinding func(fh.Ctx) ([]byte, error)

// TokenExtractor overrides the default request-token extraction logic. It is
// useful for custom body formats. Tokens should never be accepted from URL query
// parameters because URLs commonly leak into logs, browser history and Referer.
type TokenExtractor func(fh.Ctx) (string, error)

// TargetOriginResolver returns the externally-visible target origin for the
// current request, such as "https://example.com". Use this behind a trusted
// reverse proxy when c.BaseURL() reflects the internal hop rather than the
// browser-visible scheme/host. The returned value must contain only an origin:
// scheme + host + optional port, with no path/query/fragment/userinfo.
//
// Do not blindly copy Forwarded/X-Forwarded-* headers in this callback unless a
// trusted-proxy middleware has already validated and sanitized them.
type TargetOriginResolver func(fh.Ctx) (string, error)

// Config controls CSRF protection.
type Config struct {
	CookieName   string
	HeaderName   string
	FormField    string
	CookiePath   string
	CookieDomain string
	CookieSecure bool

	// AllowInsecureCookie is required to disable Secure on CSRF cookies. This
	// explicit opt-out prevents a partial Config literal from silently weakening
	// the secure default. Intended for loopback/local development only.
	AllowInsecureCookie bool

	CookieSameSite fh.SameSite

	// CookieMaxAge controls both CSRF cookies. Zero creates session cookies.
	// A session cookie is the default so CSRF lifetime does not accidentally
	// outlive or conflict with an application's authentication lifetime.
	CookieMaxAge time.Duration

	// BindingCookieName names the HttpOnly random browser-session binding cookie.
	// It is included in the CSRF-token MAC and prevents cookie-injection/fixation
	// attacks against a naive double-submit construction.
	BindingCookieName string

	// DisableBrowserBinding explicitly disables the HttpOnly browser-binding
	// cookie. This weakens the default and should normally be used only when a
	// strong SessionBinding is always available.
	DisableBrowserBinding bool

	// Secret is a convenient single active key. For key rotation, prefer Keys.
	// If both Secret and Keys are configured, configuration is rejected.
	Secret []byte

	// Keys enables signing-key rotation. The first key signs; every key verifies.
	Keys []Key

	// SessionBinding optionally binds tokens to the application's auth/session
	// identity in addition to the default browser-session binding cookie.
	SessionBinding SessionBinding

	// TrustedOrigins are exact additional origins allowed to submit unsafe
	// requests, e.g. "https://app.example.com". Wildcards are intentionally not
	// supported.
	TrustedOrigins []string

	// TargetOrigin pins the browser-visible target origin. It is strongly
	// recommended behind TLS-terminating reverse proxies/load balancers.
	TargetOrigin string

	// TargetOriginResolver is the dynamic equivalent of TargetOrigin. Configure
	// at most one of TargetOrigin and TargetOriginResolver.
	TargetOriginResolver TargetOriginResolver

	// RequireOriginHeader is retained for source compatibility. The secure
	// default is true. Because bool zero-values cannot express an explicit false
	// in a partial Config, use AllowMissingOrigin to opt out.
	RequireOriginHeader bool

	// AllowMissingOrigin permits unsafe requests carrying neither Origin nor
	// Referer. Prefer bypassing CSRF on authenticated non-browser routes instead.
	AllowMissingOrigin bool

	// AllowUntrustedOrigin disables Origin/Referer and Fetch Metadata enforcement
	// while retaining token verification. Intended only for local development.
	AllowUntrustedOrigin bool

	// CheckFetchMetadata is retained as an enable switch. It is enabled by
	// default. Use DisableFetchMetadata for an explicit opt-out.
	CheckFetchMetadata bool

	// DisableFetchMetadata explicitly disables Sec-Fetch-Site validation.
	DisableFetchMetadata bool

	// ProtectWebSockets enables strict Origin/Fetch-Metadata checks for WebSocket
	// handshakes even though HTTP GET is normally a safe method. Enabled by
	// default. Browser WebSocket APIs cannot reliably attach the CSRF header, so
	// WebSocket protection is origin-based rather than token-header-based.
	ProtectWebSockets bool

	// AllowUnprotectedWebSockets explicitly disables the secure WebSocket default.
	AllowUnprotectedWebSockets bool

	// Extractor overrides HeaderName/FormField extraction when non-nil.
	Extractor TokenExtractor

	// Next bypasses this middleware for selected routes. Prefer this for
	// authenticated machine-to-machine/non-browser APIs instead of weakening
	// global origin policy.
	Next func(fh.Ctx) bool
}

// DefaultConfig is a compatibility snapshot of the defaults.
//
// Deprecated: New does not read this mutable variable; mutating it does not
// alter middleware defaults. This prevents global mutation/races from silently
// weakening security. Construct Config values explicitly instead.
var DefaultConfig = defaultConfig()

// Protector is an immutable, validated CSRF configuration. It can be reused by
// middleware and explicit token-rotation calls.
type Protector struct {
	cfg            Config
	keys           []Key
	keyByID        map[string][]byte
	trustedOrigins map[string]struct{}
	targetOrigin   string
}

func defaultConfig() Config {
	return Config{
		CookieName:          defaultCookieName,
		HeaderName:          defaultHeaderName,
		FormField:           defaultFormField,
		CookiePath:          "/",
		CookieSecure:        true,
		CookieSameSite:      fh.SameSiteLax,
		CookieMaxAge:        0,
		BindingCookieName:   defaultBindingName,
		RequireOriginHeader: true,
		CheckFetchMetadata:  true,
		ProtectWebSockets:   true,
	}
}

// New returns CSRF middleware and preserves the historical fh middleware API.
// Invalid configuration panics at application startup rather than silently
// running with weakened protection. Use NewWithError when explicit error
// handling is preferred.
func New(config ...Config) fh.HandlerFunc {
	h, err := NewWithError(config...)
	if err != nil {
		panic(err)
	}
	return h
}

// NewWithError validates config and returns a middleware handler.
func NewWithError(config ...Config) (fh.HandlerFunc, error) {
	p, err := NewProtector(config...)
	if err != nil {
		return nil, err
	}
	return p.Middleware, nil
}

// NewProtector constructs an immutable Protector for middleware and explicit
// rotation/clearing operations.
func NewProtector(config ...Config) (*Protector, error) {
	cfg := defaultConfig()
	var supplied Config
	if len(config) > 1 {
		return nil, fmt.Errorf("%w: expected at most one Config", ErrInvalidConfig)
	}
	if len(config) == 1 {
		supplied = config[0]
		merge(&cfg, supplied)
	}

	// __Host- cookies cannot be used without Secure or with Domain. When the
	// caller explicitly chooses one of those compatibility modes and did not
	// explicitly name cookies, fall back to ordinary host/domain cookie names.
	if (cfg.AllowInsecureCookie || cfg.CookieDomain != "") && supplied.CookieName == "" {
		cfg.CookieName = insecureCookieName
	}
	if (cfg.AllowInsecureCookie || cfg.CookieDomain != "") && supplied.BindingCookieName == "" {
		cfg.BindingCookieName = insecureBindingName
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	p := &Protector{
		cfg:            cfg,
		trustedOrigins: make(map[string]struct{}, len(cfg.TrustedOrigins)),
		keyByID:        make(map[string][]byte),
	}

	if err := p.initKeys(); err != nil {
		return nil, err
	}

	for _, raw := range cfg.TrustedOrigins {
		origin, err := canonicalConfiguredOrigin(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: trusted origin %q: %v", ErrInvalidConfig, raw, err)
		}
		p.trustedOrigins[origin] = struct{}{}
	}

	if cfg.TargetOrigin != "" {
		origin, err := canonicalConfiguredOrigin(cfg.TargetOrigin)
		if err != nil {
			return nil, fmt.Errorf("%w: target origin %q: %v", ErrInvalidConfig, cfg.TargetOrigin, err)
		}
		p.targetOrigin = origin
	}

	return p, nil
}

// Middleware enforces CSRF protection.
func (p *Protector) Middleware(c fh.Ctx) error {
	if p == nil {
		return fh.InternalError(errors.New("csrf: nil protector"))
	}
	if p.cfg.Next != nil && p.cfg.Next(c) {
		return c.Next()
	}

	websocket := p.cfg.ProtectWebSockets && isWebSocketHandshake(c)
	unsafe := !safeMethod(c.Method())

	// Validate request provenance before issuing/replacing cookies on unsafe or
	// WebSocket requests. This prevents a rejected cross-site request from being
	// used merely to rotate a victim's CSRF state (a nuisance/DoS vector).
	var explicitlyTrusted bool
	if (unsafe || websocket) && !p.cfg.AllowUntrustedOrigin {
		var err error
		explicitlyTrusted, err = p.validateRequestOrigin(c)
		if err != nil {
			return csrfError()
		}
		if p.cfg.CheckFetchMetadata && !p.validateFetchMetadata(c, explicitlyTrusted) {
			return csrfError()
		}
	}

	binding, err := p.ensureBrowserBinding(c)
	if err != nil {
		return fh.InternalError(err)
	}
	appBinding, err := p.applicationBinding(c)
	if err != nil {
		return fh.InternalError(fmt.Errorf("csrf: session binding: %w", err))
	}

	token := c.GetCookie(p.cfg.CookieName)
	validCookieToken := token != "" && p.verifyToken(token, binding, appBinding)
	if !validCookieToken {
		token, err = p.issueToken(c, binding, appBinding)
		if err != nil {
			return fh.InternalError(err)
		}
	}
	c.Locals(defaultTokenLocalKey, token)

	if websocket {
		// Browser WebSocket handshakes are protected by strict provenance checks.
		// This also covers RFC 8441 extended CONNECT, whose method is not GET.
		return c.Next()
	}
	if !unsafe {
		return c.Next()
	}

	provided, err := p.extractRequestToken(c)
	if err != nil {
		return csrfError()
	}
	if !constantTimeStringEqual(provided, token) {
		return csrfError()
	}

	// Verify cryptographic validity again at the decision point. This is cheap,
	// keeps the enforcement invariant explicit, and protects future refactors of
	// token issuance/selection logic.
	if !p.verifyToken(provided, binding, appBinding) {
		return csrfError()
	}

	return c.Next()
}

// RotateToken issues a fresh CSRF token using the Protector's exact immutable
// configuration. Call this after authentication/session transitions.
func (p *Protector) RotateToken(c fh.Ctx) (string, error) {
	if p == nil {
		return "", errors.New("csrf: nil protector")
	}
	binding, err := p.ensureBrowserBinding(c)
	if err != nil {
		return "", err
	}
	appBinding, err := p.applicationBinding(c)
	if err != nil {
		return "", fmt.Errorf("csrf: session binding: %w", err)
	}
	return p.issueToken(c, binding, appBinding)
}

// Clear expires both CSRF cookies using the configured path/domain/security
// attributes. It is useful on logout/account switching.
func (p *Protector) Clear(c fh.Ctx) {
	if p == nil {
		return
	}
	p.expireCookie(c, p.cfg.CookieName, false)
	if !p.cfg.DisableBrowserBinding {
		p.expireCookie(c, p.cfg.BindingCookieName, true)
	}
	c.Locals(defaultTokenLocalKey, "")
}

// RotateToken preserves the historical package-level API. For repeated use,
// prefer constructing a Protector once and calling protector.RotateToken(c).
func RotateToken(c fh.Ctx, config ...Config) (string, error) {
	p, err := NewProtector(config...)
	if err != nil {
		return "", err
	}
	return p.RotateToken(c)
}

// Token returns the token stored by middleware for the current request, or the
// request cookie when middleware has not populated the local yet.
func Token(c fh.Ctx, config ...Config) string {
	if v := c.Locals(defaultTokenLocalKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	cfg := defaultConfig()
	if len(config) == 1 {
		merge(&cfg, config[0])
		if (cfg.AllowInsecureCookie || cfg.CookieDomain != "") && config[0].CookieName == "" {
			cfg.CookieName = insecureCookieName
		}
	}
	return c.GetCookie(cfg.CookieName)
}

func (p *Protector) initKeys() error {
	var keys []Key
	switch {
	case len(p.cfg.Keys) > 0:
		keys = p.cfg.Keys
	case len(p.cfg.Secret) > 0:
		keys = []Key{{ID: defaultKeyID, Secret: p.cfg.Secret}}
	default:
		// Safe single-process default; callers running multiple replicas should
		// configure stable shared keys.
		keys = []Key{{ID: defaultKeyID, Secret: processSecret}}
	}

	p.keys = make([]Key, 0, len(keys))
	for _, key := range keys {
		if !validKeyID(key.ID) {
			return fmt.Errorf("%w: invalid signing key id %q", ErrInvalidConfig, key.ID)
		}
		if len(key.Secret) < minimumSecretSize {
			return fmt.Errorf("%w: signing key %q must be at least %d bytes", ErrInvalidConfig, key.ID, minimumSecretSize)
		}
		if _, exists := p.keyByID[key.ID]; exists {
			return fmt.Errorf("%w: duplicate signing key id %q", ErrInvalidConfig, key.ID)
		}
		secret := append([]byte(nil), key.Secret...)
		p.keys = append(p.keys, Key{ID: key.ID, Secret: secret})
		p.keyByID[key.ID] = secret
	}
	return nil
}

func validateConfig(cfg Config) error {
	if cfg.CookieName == "" || !validHTTPToken(cfg.CookieName) {
		return fmt.Errorf("%w: invalid CookieName", ErrInvalidConfig)
	}
	if cfg.HeaderName == "" || !validHTTPToken(cfg.HeaderName) {
		return fmt.Errorf("%w: invalid HeaderName", ErrInvalidConfig)
	}
	if cfg.FormField == "" {
		return fmt.Errorf("%w: FormField must not be empty", ErrInvalidConfig)
	}
	if strings.ContainsAny(cfg.FormField, "\x00\r\n") || len(cfg.FormField) > 256 {
		return fmt.Errorf("%w: invalid FormField", ErrInvalidConfig)
	}
	if cfg.CookiePath == "" || strings.ContainsAny(cfg.CookiePath, ";\x00\r\n") {
		return fmt.Errorf("%w: invalid CookiePath", ErrInvalidConfig)
	}
	if cfg.CookieMaxAge < 0 {
		return fmt.Errorf("%w: CookieMaxAge must be >= 0", ErrInvalidConfig)
	}
	if cfg.CookieMaxAge > 0 {
		seconds := int64(cfg.CookieMaxAge / time.Second)
		if seconds <= 0 || uint64(seconds) > uint64(maxInt()) {
			return fmt.Errorf("%w: CookieMaxAge is out of range", ErrInvalidConfig)
		}
	}
	if cfg.CookieSameSite < fh.SameSiteLax || cfg.CookieSameSite > fh.SameSiteNone {
		return fmt.Errorf("%w: invalid CookieSameSite", ErrInvalidConfig)
	}
	if cfg.TargetOrigin != "" && cfg.TargetOriginResolver != nil {
		return fmt.Errorf("%w: configure only one of TargetOrigin and TargetOriginResolver", ErrInvalidConfig)
	}
	if len(cfg.Secret) > 0 && len(cfg.Keys) > 0 {
		return fmt.Errorf("%w: configure only one of Secret and Keys", ErrInvalidConfig)
	}
	if !cfg.DisableBrowserBinding {
		if cfg.BindingCookieName == "" || !validHTTPToken(cfg.BindingCookieName) {
			return fmt.Errorf("%w: invalid BindingCookieName", ErrInvalidConfig)
		}
		if cfg.BindingCookieName == cfg.CookieName {
			return fmt.Errorf("%w: BindingCookieName must differ from CookieName", ErrInvalidConfig)
		}
	}
	if cfg.DisableBrowserBinding && cfg.SessionBinding == nil {
		return fmt.Errorf("%w: disabling browser binding requires SessionBinding", ErrInvalidConfig)
	}

	probe := &fh.Cookie{
		Name:     cfg.CookieName,
		Value:    "probe",
		Path:     cfg.CookiePath,
		Domain:   cfg.CookieDomain,
		Secure:   cfg.CookieSecure,
		HttpOnly: false,
		SameSite: cfg.CookieSameSite,
	}
	if err := probe.Valid(); err != nil {
		return fmt.Errorf("%w: CSRF cookie: %v", ErrInvalidConfig, err)
	}
	if !cfg.DisableBrowserBinding {
		bindingProbe := &fh.Cookie{
			Name:     cfg.BindingCookieName,
			Value:    "probe",
			Path:     cfg.CookiePath,
			Domain:   cfg.CookieDomain,
			Secure:   cfg.CookieSecure,
			HttpOnly: true,
			SameSite: cfg.CookieSameSite,
		}
		if err := bindingProbe.Valid(); err != nil {
			return fmt.Errorf("%w: binding cookie: %v", ErrInvalidConfig, err)
		}
	}
	return nil
}

func merge(dst *Config, src Config) {
	if src.CookieName != "" {
		dst.CookieName = src.CookieName
	}
	if src.HeaderName != "" {
		dst.HeaderName = src.HeaderName
	}
	if src.FormField != "" {
		dst.FormField = src.FormField
	}
	if src.CookiePath != "" {
		dst.CookiePath = src.CookiePath
	}
	if src.CookieDomain != "" {
		dst.CookieDomain = src.CookieDomain
	}
	if src.CookieMaxAge > 0 {
		dst.CookieMaxAge = src.CookieMaxAge
	}
	if src.BindingCookieName != "" {
		dst.BindingCookieName = src.BindingCookieName
	}
	if src.TrustedOrigins != nil {
		dst.TrustedOrigins = append([]string(nil), src.TrustedOrigins...)
	}
	if len(src.Secret) > 0 {
		dst.Secret = append([]byte(nil), src.Secret...)
	}
	if src.Keys != nil {
		dst.Keys = make([]Key, len(src.Keys))
		for i := range src.Keys {
			dst.Keys[i] = Key{ID: src.Keys[i].ID, Secret: append([]byte(nil), src.Keys[i].Secret...)}
		}
	}

	if src.AllowInsecureCookie {
		dst.CookieSecure = false
		dst.AllowInsecureCookie = true
	} else if src.CookieSecure {
		dst.CookieSecure = true
	}

	// SameSiteLax is the fh zero value, so assigning it from a partial Config is
	// safe and preserves the secure default.
	dst.CookieSameSite = src.CookieSameSite

	if src.AllowMissingOrigin {
		dst.RequireOriginHeader = false
		dst.AllowMissingOrigin = true
	} else if src.RequireOriginHeader {
		dst.RequireOriginHeader = true
	}

	if src.DisableFetchMetadata {
		dst.CheckFetchMetadata = false
		dst.DisableFetchMetadata = true
	} else if src.CheckFetchMetadata {
		dst.CheckFetchMetadata = true
	}

	if src.AllowUnprotectedWebSockets {
		dst.ProtectWebSockets = false
		dst.AllowUnprotectedWebSockets = true
	} else if src.ProtectWebSockets {
		dst.ProtectWebSockets = true
	}

	dst.DisableBrowserBinding = src.DisableBrowserBinding
	dst.AllowUntrustedOrigin = src.AllowUntrustedOrigin
	dst.TargetOrigin = src.TargetOrigin
	dst.TargetOriginResolver = src.TargetOriginResolver
	dst.SessionBinding = src.SessionBinding
	dst.Extractor = src.Extractor
	dst.Next = src.Next
}

func (p *Protector) ensureBrowserBinding(c fh.Ctx) ([]byte, error) {
	if p.cfg.DisableBrowserBinding {
		return nil, nil
	}

	raw := c.GetCookie(p.cfg.BindingCookieName)
	if raw != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err == nil && len(decoded) == bindingSize && len(raw) <= maximumBindingLength &&
			base64.RawURLEncoding.EncodeToString(decoded) == raw {
			return decoded, nil
		}
	}

	binding := make([]byte, bindingSize)
	if _, err := io.ReadFull(rand.Reader, binding); err != nil {
		return nil, fmt.Errorf("csrf: generate browser binding: %w", err)
	}
	raw = base64.RawURLEncoding.EncodeToString(binding)
	if err := p.setCookie(c, p.cfg.BindingCookieName, raw, true); err != nil {
		return nil, err
	}
	return binding, nil
}

func (p *Protector) applicationBinding(c fh.Ctx) ([]byte, error) {
	if p.cfg.SessionBinding == nil {
		return nil, nil
	}
	binding, err := p.cfg.SessionBinding(c)
	if err != nil {
		return nil, err
	}
	// Empty is permitted so applications can support an anonymous pre-login
	// state, but authenticated integrations should return a stable session ID.
	return append([]byte(nil), binding...), nil
}

func (p *Protector) issueToken(c fh.Ctx, browserBinding, appBinding []byte) (string, error) {
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("csrf: generate nonce: %w", err)
	}

	key := p.keys[0]
	encodedNonce := base64.RawURLEncoding.EncodeToString(nonce)
	mac := p.computeMAC(key.Secret, key.ID, nonce, browserBinding, appBinding)
	token := tokenVersion + "." + key.ID + "." + encodedNonce + "." + base64.RawURLEncoding.EncodeToString(mac)
	if len(token) > maximumTokenLength {
		return "", errors.New("csrf: generated token exceeds maximum length")
	}
	if err := p.setCookie(c, p.cfg.CookieName, token, false); err != nil {
		return "", err
	}
	c.Locals(defaultTokenLocalKey, token)
	return token, nil
}

func (p *Protector) verifyToken(token string, browserBinding, appBinding []byte) bool {
	if token == "" || len(token) > maximumTokenLength {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != tokenVersion || !validKeyID(parts[1]) {
		return false
	}

	secret, ok := p.keyByID[parts[1]]
	if !ok {
		return false
	}

	nonce, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(nonce) != nonceSize || base64.RawURLEncoding.EncodeToString(nonce) != parts[2] {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || len(sig) != macSize || base64.RawURLEncoding.EncodeToString(sig) != parts[3] {
		return false
	}

	expected := p.computeMAC(secret, parts[1], nonce, browserBinding, appBinding)
	return subtle.ConstantTimeCompare(sig, expected) == 1
}

func (p *Protector) computeMAC(secret []byte, keyID string, nonce, browserBinding, appBinding []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	writeMACField(mac, []byte("fh.csrf"))
	writeMACField(mac, []byte(tokenVersion))
	writeMACField(mac, []byte(keyID))
	writeMACField(mac, []byte(p.cfg.CookieName))
	writeMACField(mac, []byte(p.cfg.BindingCookieName))
	writeMACField(mac, browserBinding)
	writeMACField(mac, appBinding)
	writeMACField(mac, nonce)
	return mac.Sum(nil)
}

func writeMACField(mac hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = mac.Write(length[:])
	_, _ = mac.Write(value)
}

func (p *Protector) setCookie(c fh.Ctx, name, value string, httpOnly bool) error {
	cookie := &fh.Cookie{
		Name:     name,
		Value:    value,
		Path:     p.cfg.CookiePath,
		Domain:   p.cfg.CookieDomain,
		MaxAge:   durationMaxAge(p.cfg.CookieMaxAge),
		Secure:   p.cfg.CookieSecure,
		HttpOnly: httpOnly,
		SameSite: p.cfg.CookieSameSite,
	}
	if err := cookie.Valid(); err != nil {
		return fmt.Errorf("csrf: invalid response cookie %q: %w", name, err)
	}
	c.SetCookie(cookie)
	return nil
}

func (p *Protector) expireCookie(c fh.Ctx, name string, httpOnly bool) {
	cookie := &fh.Cookie{
		Name:     name,
		Value:    "",
		Path:     p.cfg.CookiePath,
		Domain:   p.cfg.CookieDomain,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		Secure:   p.cfg.CookieSecure,
		HttpOnly: httpOnly,
		SameSite: p.cfg.CookieSameSite,
	}
	if cookie.Valid() == nil {
		c.SetCookie(cookie)
	}
}

func (p *Protector) extractRequestToken(c fh.Ctx) (string, error) {
	if p.cfg.Extractor != nil {
		token, err := p.cfg.Extractor(c)
		if err != nil {
			return "", err
		}
		if len(token) > maximumTokenLength {
			return "", ErrInvalidToken
		}
		return token, nil
	}

	if token := strings.TrimSpace(c.Get(p.cfg.HeaderName)); token != "" {
		if len(token) > maximumTokenLength {
			return "", ErrInvalidToken
		}
		return token, nil
	}

	contentType := c.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", nil
	}

	switch strings.ToLower(mediaType) {
	case "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(string(c.Body()))
		if err != nil {
			return "", err
		}
		token := values.Get(p.cfg.FormField)
		if len(token) > maximumTokenLength {
			return "", ErrInvalidToken
		}
		return token, nil

	case "multipart/form-data":
		boundary := params["boundary"]
		if boundary == "" {
			return "", errors.New("csrf: multipart boundary missing")
		}
		return extractMultipartToken(c.Body(), boundary, p.cfg.FormField)
	}

	return "", nil
}

func extractMultipartToken(body []byte, boundary, field string) (string, error) {
	r := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := r.NextPart()
		if errors.Is(err, io.EOF) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		if part.FormName() != field || part.FileName() != "" {
			_ = part.Close()
			continue
		}
		data, err := io.ReadAll(io.LimitReader(part, maximumTokenLength+1))
		_ = part.Close()
		if err != nil {
			return "", err
		}
		if len(data) > maximumTokenLength {
			return "", ErrInvalidToken
		}
		return string(data), nil
	}
}

func (p *Protector) validateRequestOrigin(c fh.Ctx) (explicitlyTrusted bool, err error) {
	rawOrigin := strings.TrimSpace(c.Get("Origin"))
	rawReferer := strings.TrimSpace(c.Get("Referer"))

	var source string
	switch {
	case rawOrigin != "":
		if strings.EqualFold(rawOrigin, "null") {
			return false, errors.New("csrf: null Origin")
		}
		source, err = canonicalRequestOrigin(rawOrigin, false)
	case rawReferer != "":
		source, err = canonicalRequestOrigin(rawReferer, true)
	default:
		if p.cfg.AllowMissingOrigin || !p.cfg.RequireOriginHeader {
			return false, nil
		}
		return false, errors.New("csrf: Origin and Referer are missing")
	}
	if err != nil {
		return false, err
	}

	target, err := p.resolveTargetOrigin(c)
	if err != nil {
		return false, err
	}
	if source == target {
		return false, nil
	}
	if _, ok := p.trustedOrigins[source]; ok {
		return true, nil
	}
	return false, errors.New("csrf: request origin is not allowed")
}

func (p *Protector) resolveTargetOrigin(c fh.Ctx) (string, error) {
	if p.targetOrigin != "" {
		return p.targetOrigin, nil
	}
	if p.cfg.TargetOriginResolver != nil {
		raw, err := p.cfg.TargetOriginResolver(c)
		if err != nil {
			return "", err
		}
		return canonicalConfiguredOrigin(raw)
	}
	return canonicalConfiguredOrigin(c.BaseURL())
}

func (p *Protector) validateFetchMetadata(c fh.Ctx, explicitlyTrusted bool) bool {
	site := strings.ToLower(strings.TrimSpace(c.Get("Sec-Fetch-Site")))
	if site == "" {
		// Older browsers and non-browser clients may not send Fetch Metadata.
		// Origin/Referer + signed token remain mandatory unless explicitly opted out.
		return true
	}
	switch site {
	case "same-origin":
		return true
	case "same-site":
		// Same-site is not treated as same-origin. It reaches this point only after
		// exact Origin/Referer validation above, which rejects hostile siblings.
		return true
	case "cross-site":
		// Cross-site is allowed only when its exact Origin is explicitly trusted.
		return explicitlyTrusted
	case "none":
		// User-initiated/top-level contexts can report "none". Origin/Referer and
		// the signed token still provide the authorization decision.
		return true
	default:
		// Fail closed on a present but unknown value.
		return false
	}
}

func canonicalConfiguredOrigin(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("empty origin")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("origin must not contain userinfo, path, query, or fragment")
	}
	return canonicalURLOrigin(u)
}

func canonicalRequestOrigin(raw string, allowPath bool) (string, error) {
	if raw == "" || strings.ContainsAny(raw, "\x00\r\n") {
		return "", errors.New("invalid origin")
	}
	// A serialized Origin is a single origin. Reject obvious lists/whitespace
	// constructions rather than trying to guess which component the browser meant.
	if !allowPath && strings.ContainsAny(raw, " \t,") {
		return "", errors.New("invalid Origin header")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.User != nil {
		return "", errors.New("origin contains userinfo")
	}
	if !allowPath && (u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.Fragment != "") {
		return "", errors.New("Origin header contains path/query/fragment")
	}
	return canonicalURLOrigin(u)
}

func canonicalURLOrigin(u *url.URL) (string, error) {
	if u == nil {
		return "", errors.New("nil URL")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("origin scheme must be http or https")
	}
	if u.Host == "" {
		return "", errors.New("origin host is missing")
	}
	hostport, err := canonicalHostPort(scheme, u.Host)
	if err != nil {
		return "", err
	}
	return scheme + "://" + hostport, nil
}

func canonicalHostPort(scheme, rawHost string) (string, error) {
	if rawHost == "" || strings.ContainsAny(rawHost, "\x00\r\n\t /\\") {
		return "", errors.New("invalid host")
	}

	var host, port string
	if strings.HasPrefix(rawHost, "[") {
		end := strings.IndexByte(rawHost, ']')
		if end < 0 {
			return "", errors.New("invalid IPv6 host")
		}
		host = rawHost[1:end]
		rest := rawHost[end+1:]
		if rest != "" {
			if !strings.HasPrefix(rest, ":") || len(rest) == 1 || strings.Contains(rest[1:], ":") {
				return "", errors.New("invalid host port")
			}
			port = rest[1:]
		}
		if ip := net.ParseIP(host); ip == nil || !strings.Contains(host, ":") {
			return "", errors.New("invalid bracketed IPv6 host")
		}
	} else {
		colonCount := strings.Count(rawHost, ":")
		switch colonCount {
		case 0:
			host = rawHost
		case 1:
			idx := strings.LastIndexByte(rawHost, ':')
			host, port = rawHost[:idx], rawHost[idx+1:]
			if host == "" || port == "" {
				return "", errors.New("invalid host port")
			}
		default:
			return "", errors.New("IPv6 hosts must be bracketed")
		}
	}

	if !asciiHost(host) {
		return "", errors.New("host must be ASCII/punycode")
	}
	host = strings.ToLower(host)
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", errors.New("invalid port")
		}
		if (scheme == "http" && n == 80) || (scheme == "https" && n == 443) {
			port = ""
		} else {
			port = strconv.Itoa(n)
		}
	}

	if ip := net.ParseIP(host); ip != nil && strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" {
		return host + ":" + port, nil
	}
	return host, nil
}

func asciiHost(host string) bool {
	if host == "" || len(host) > 253 && net.ParseIP(host) == nil {
		return false
	}
	for i := 0; i < len(host); i++ {
		c := host[i]
		if c >= 0x80 || c <= 0x20 || c == '/' || c == '\\' || c == '%' || c == '@' {
			return false
		}
	}
	return true
}

func safeMethod(method string) bool {
	switch method {
	case "GET", "HEAD", "OPTIONS", "QUERY":
		return true
	default:
		return false
	}
}

func isWebSocketHandshake(c fh.Ctx) bool {
	if strings.EqualFold(c.ConnectProtocol(), "websocket") {
		return true
	}
	if !strings.EqualFold(strings.TrimSpace(c.Get("Upgrade")), "websocket") {
		return false
	}
	for _, token := range strings.Split(c.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return true
		}
	}
	return false
}

func constantTimeStringEqual(a, b string) bool {
	if a == "" || b == "" || len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func csrfError() *fh.HTTPError {
	// Keep external failure text generic so callers cannot distinguish missing,
	// malformed, origin-rejected and cryptographically-invalid states.
	return fh.NewHTTPError(fh.StatusForbidden, "CSRF_INVALID", "CSRF validation failed")
}

func durationMaxAge(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d / time.Second)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func validKeyID(s string) bool {
	if s == "" || len(s) > maximumKeyIDLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func validHTTPToken(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= 0x20 || c >= 0x7f || strings.ContainsRune("()<>@,;:\\\"/[]?={}", rune(c)) {
			return false
		}
	}
	return true
}

func mustRandom(n int) []byte {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(fmt.Errorf("csrf: secure randomness unavailable: %w", err))
	}
	return b
}
