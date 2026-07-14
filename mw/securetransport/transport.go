// Package securetransport provides application-layer encrypted, device-bound,
// replay-resistant request/response transport for fh applications.
package securetransport

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

const (
	DefaultPrefix = "/__fh/secure/v1"
	LocalSession  = "fh.secure.session"
	LocalDevice   = "fh.secure.device"
	LocalRequest  = "fh.secure.request_id"
)

type SecurityEvent struct {
	Type      string
	IP        string
	DeviceID  string
	SessionID string
	RequestID string
	Detail    string
	At        time.Time
}

type Config struct {
	Prefix           string
	KeyID            string
	ServerPrivateKey []byte
	// AllowEphemeralServerKey is development-only. Production deployments must
	// provide a persistent 32-byte X25519 private key so client pinning and
	// active sessions survive process restarts.
	AllowEphemeralServerKey bool
	SessionTTL              time.Duration
	HandshakeTTL            time.Duration
	RequestTTL              time.Duration
	MaxClockSkew            time.Duration
	Limits                  protocol.Limits
	MaxReplayEntries        int

	DeviceStore  DeviceStore
	SessionStore SessionStore
	ReplayStore  ReplayStore

	// AuthorizeDeviceRegistration must authenticate/authorize registration and
	// may return the application principal bound to the device. A nil callback
	// rejects registration unless AllowUnauthenticatedDeviceRegistration is set.
	AuthorizeDeviceRegistration            func(fh.Ctx, protocol.DeviceRegistrationRequest) (principal string, err error)
	AllowUnauthenticatedDeviceRegistration bool

	// ValidateSession runs after successful decryption and header restoration,
	// before the application handler. Use it to bind the device session to the
	// current login, tenant/workspace, token, or risk decision.
	ValidateSession func(fh.Ctx, SessionInfo) error
	// SkipDeviceStatusCheck avoids a device-store revocation lookup on every
	// request. Keep false for the strongest revocation semantics.
	SkipDeviceStatusCheck bool

	// RequireSecure rejects plaintext requests selected by Protect. Requests
	// carrying X-FH-Secure: 1 are always processed as secure requests.
	RequireSecure bool
	Protect       func(fh.Ctx) bool
	Skip          func(fh.Ctx) bool

	AllowedOrigins []string
	RequireOrigin  bool
	AllowSameSite  bool
	// PreserveOuterHeaders adds exact lower/upper-case-insensitive names to the
	// small browser-controlled outer-header allowlist. All other application
	// headers must arrive inside the encrypted payload.
	PreserveOuterHeaders []string

	// HideResponseHeaders defaults to true. ExposeResponseHeaders is an explicit compatibility escape hatch.
	HideResponseHeaders   bool
	ExposeResponseHeaders bool
	OnSecurityEvent       func(SecurityEvent)
}

type Transport struct {
	cfg           Config
	privateKey    *ecdh.PrivateKey
	publicKey     [32]byte
	allowedOrigin map[string]struct{}
	preserveOuter map[string]struct{}
}

func New(config Config) (*Transport, error) {
	if config.Prefix == "" {
		config.Prefix = DefaultPrefix
	}
	config.Prefix = strings.TrimRight(config.Prefix, "/")
	if config.KeyID == "" {
		config.KeyID = "fh-server-1"
	}
	if config.SessionTTL <= 0 {
		config.SessionTTL = 15 * time.Minute
	}
	if config.HandshakeTTL <= 0 {
		config.HandshakeTTL = 2 * time.Minute
	}
	if config.RequestTTL <= 0 {
		config.RequestTTL = 90 * time.Second
	}
	if config.MaxClockSkew <= 0 {
		config.MaxClockSkew = 30 * time.Second
	}
	if config.DeviceStore == nil {
		config.DeviceStore = NewMemoryDeviceStore()
	}
	if config.SessionStore == nil {
		config.SessionStore = NewMemorySessionStore()
	}
	if config.ReplayStore == nil {
		config.ReplayStore = NewMemoryReplayStore(config.MaxReplayEntries)
	}
	// Hiding application metadata is the safe default. ExposeResponseHeaders
	// is an explicit compatibility escape hatch.
	if config.ExposeResponseHeaders {
		config.HideResponseHeaders = false
	} else {
		config.HideResponseHeaders = true
	}
	if config.Limits.MaxBody <= 0 {
		config.Limits.MaxBody = protocol.DefaultMaxBody
	}
	if config.Limits.MaxHeaders <= 0 {
		config.Limits.MaxHeaders = protocol.DefaultMaxHeaders
	}
	if config.Limits.MaxHeaderName <= 0 {
		config.Limits.MaxHeaderName = protocol.DefaultMaxHeaderName
	}
	if config.Limits.MaxHeaderValue <= 0 {
		config.Limits.MaxHeaderValue = protocol.DefaultMaxHeaderValue
	}

	curve := ecdh.X25519()
	var privateKey *ecdh.PrivateKey
	var err error
	if len(config.ServerPrivateKey) == 0 {
		if !config.AllowEphemeralServerKey {
			return nil, errors.New("secure transport: persistent ServerPrivateKey is required; set AllowEphemeralServerKey only for development")
		}
		privateKey, err = curve.GenerateKey(rand.Reader)
	} else {
		keyCopy := append([]byte(nil), config.ServerPrivateKey...)
		privateKey, err = curve.NewPrivateKey(keyCopy)
		wipe(keyCopy)
	}
	// Do not retain raw private-key bytes in the copied runtime config.
	config.ServerPrivateKey = nil
	if err != nil {
		return nil, fmt.Errorf("secure transport server key: %w", err)
	}
	pub := privateKey.PublicKey().Bytes()
	if len(pub) != protocol.X25519KeySize {
		return nil, errors.New("secure transport: invalid server public key size")
	}

	t := &Transport{
		cfg: config, privateKey: privateKey,
		allowedOrigin: make(map[string]struct{}, len(config.AllowedOrigins)),
		preserveOuter: make(map[string]struct{}, len(config.PreserveOuterHeaders)),
	}
	copy(t.publicKey[:], pub)
	for _, origin := range config.AllowedOrigins {
		origin = strings.TrimRight(strings.TrimSpace(origin), "/")
		if origin != "" {
			t.allowedOrigin[origin] = struct{}{}
		}
	}
	for _, name := range config.PreserveOuterHeaders {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" && protocol.ValidProtectedHeader(name, "value") {
			t.preserveOuter[name] = struct{}{}
		}
	}
	return t, nil
}

// Install enables the middleware and registers the device/session endpoints.
func Install(app *fh.App, config Config) (*Transport, error) {
	t, err := New(config)
	if err != nil {
		return nil, err
	}
	app.Use(t.Middleware())
	t.Register(app)
	return t, nil
}

func (t *Transport) Register(app *fh.App) {
	app.Post(t.cfg.Prefix+"/device/register", t.registerDevice)
	app.Post(t.cfg.Prefix+"/session", t.createSession)
	app.Post(t.cfg.Prefix+"/session/revoke", t.revokeSession)
}

func (t *Transport) PublicKey() [32]byte { return t.publicKey }
func (t *Transport) PublicKeyBase64() string {
	return base64.RawURLEncoding.EncodeToString(t.publicKey[:])
}
func (t *Transport) KeyID() string { return t.cfg.KeyID }

func (t *Transport) Middleware() fh.HandlerFunc {
	return func(c fh.Ctx) error {
		if t.isControlPath(c.Path()) || (t.cfg.Skip != nil && t.cfg.Skip(c)) {
			return c.Next()
		}
		secureRequested := c.Get(protocol.HeaderSecure) == "1" || strings.EqualFold(c.Get(fh.HeaderContentTypeStr), protocol.MediaTypeRequest)
		// Browser CORS preflights are plaintext OPTIONS requests. An explicit
		// secure OPTIONS request still goes through the protocol.
		if c.Method() == fh.MethodOPTIONSStr && !secureRequested {
			return c.Next()
		}
		mustProtect := t.cfg.RequireSecure && (t.cfg.Protect == nil || t.cfg.Protect(c))
		if !secureRequested {
			if mustProtect {
				t.event(c, "plaintext_rejected", "", "", "", "secure transport required")
				return fh.NewHTTPError(fh.StatusUpgradeRequired, "FH_SECURE_REQUIRED", "secure transport is required")
			}
			return c.Next()
		}
		if err := t.validateBrowserContext(c); err != nil {
			t.event(c, "browser_context_rejected", "", "", "", err.Error())
			return err
		}
		return t.handleSecure(c)
	}
}

func (t *Transport) handleSecure(c fh.Ctx) error {
	raw, err := t.readEnvelope(c)
	if err != nil {
		return err
	}
	meta, err := protocol.DecodeRequestEnvelope(raw, t.cfg.Limits.MaxBody+64<<10)
	if err != nil {
		t.event(c, "malformed_envelope", "", "", "", "request envelope rejected")
		return t.authFailure()
	}
	session, err := t.cfg.SessionStore.Get(meta.SessionID)
	if err != nil || !session.ExpiresAt.After(time.Now()) {
		t.event(c, "unknown_session", "", protocol.EncodeID(meta.SessionID), protocol.EncodeID(meta.RequestID), "session unavailable")
		return t.authFailure()
	}
	defer zeroSession(&session)
	if err := protocol.ValidateTime(meta.IssuedAt, meta.ExpiresAt, time.Now(), t.cfg.MaxClockSkew); err != nil || meta.Sequence == 0 {
		t.event(c, "expired_request", protocol.EncodeID(session.DeviceID), protocol.EncodeID(session.ID), protocol.EncodeID(meta.RequestID), "request time or sequence invalid")
		return t.authFailure()
	}
	if time.UnixMilli(meta.ExpiresAt).Sub(time.UnixMilli(meta.IssuedAt)) > t.cfg.RequestTTL+t.cfg.MaxClockSkew || meta.ExpiresAt > session.ExpiresAt.UnixMilli()+t.cfg.MaxClockSkew.Milliseconds() {
		return t.authFailure()
	}
	if !t.cfg.SkipDeviceStatusCheck {
		if _, err := t.cfg.DeviceStore.Get(session.DeviceID); err != nil {
			t.event(c, "revoked_device_rejected", protocol.EncodeID(session.DeviceID), protocol.EncodeID(session.ID), protocol.EncodeID(meta.RequestID), "device is revoked or unavailable")
			return t.authFailure()
		}
	}
	decoded, payload, err := protocol.DecryptRequest(session.Keys.ClientToServer, c.Method(), c.OriginalURL(), raw, t.cfg.Limits)
	if err != nil {
		t.event(c, "request_authentication_failed", protocol.EncodeID(session.DeviceID), protocol.EncodeID(session.ID), protocol.EncodeID(meta.RequestID), "ciphertext authentication failed")
		return t.authFailure()
	}
	if !protocol.EqualID(decoded.SessionID, session.ID) {
		return t.authFailure()
	}
	replayExpiry := time.UnixMilli(decoded.ExpiresAt).Add(t.cfg.MaxClockSkew)
	sequenceKey := "request-sequence:" + protocol.EncodeID(session.ID) + ":" + strconv.FormatUint(decoded.Sequence, 10)
	accepted, err := t.cfg.ReplayStore.CheckAndStore(sequenceKey, replayExpiry)
	if err != nil {
		return fh.NewHTTPError(fh.StatusServiceUnavailable, "FH_SECURE_REPLAY_STORE", "secure request could not be accepted")
	}
	if !accepted {
		t.event(c, "replay_rejected", protocol.EncodeID(session.DeviceID), protocol.EncodeID(session.ID), protocol.EncodeID(decoded.RequestID), "duplicate request sequence")
		return fh.NewHTTPError(fh.StatusConflict, "FH_SECURE_REPLAY", "request replay detected")
	}
	requestIDKey := "request-id:" + protocol.EncodeID(session.ID) + ":" + protocol.EncodeID(decoded.RequestID)
	accepted, err = t.cfg.ReplayStore.CheckAndStore(requestIDKey, replayExpiry)
	if err != nil {
		return fh.NewHTTPError(fh.StatusServiceUnavailable, "FH_SECURE_REPLAY_STORE", "secure request could not be accepted")
	}
	if !accepted {
		t.event(c, "replay_rejected", protocol.EncodeID(session.DeviceID), protocol.EncodeID(session.ID), protocol.EncodeID(decoded.RequestID), "duplicate request id")
		return fh.NewHTTPError(fh.StatusConflict, "FH_SECURE_REPLAY", "request replay detected")
	}

	t.sanitizeOuterRequestHeaders(c)
	if !fh.ReplaceRequestBody(c, payload.Body) {
		return fh.NewHTTPError(fh.StatusInternalServerError, "FH_SECURE_CONTEXT", "request body replacement is unavailable")
	}
	if payload.ContentType != "" {
		c.RequestHeader().Set(fh.HeaderContentTypeStr, payload.ContentType)
	} else {
		c.RequestHeader().Del(fh.HeaderContentTypeStr)
	}
	for _, header := range payload.Headers {
		if protocol.ValidProtectedHeader(header.Name, header.Value) {
			c.RequestHeader().Set(header.Name, header.Value)
		}
	}
	c.RequestHeader().Del(protocol.HeaderEnvelope)
	info := publicSession(session)
	c.Locals(LocalSession, info)
	c.Locals(LocalDevice, session.DeviceID)
	c.Locals(LocalRequest, decoded.RequestID)
	_ = t.cfg.DeviceStore.Touch(session.DeviceID, time.Now())
	if t.cfg.ValidateSession != nil {
		if err := t.cfg.ValidateSession(c, info); err != nil {
			return err
		}
	}

	decoded.Ciphertext = nil
	t.installResponseProtection(c, session, decoded)
	return c.Next()
}

func (t *Transport) installResponseProtection(c fh.Ctx, session Session, request protocol.RequestEnvelope) {
	c.AddBodyTransform(func(body []byte) ([]byte, error) {
		defer zeroSession(&session)
		status := c.StatusCode()
		contentType := c.GetRespHeader(fh.HeaderContentTypeStr)
		headers, err := t.collectResponseHeaders(c)
		if err != nil {
			return nil, err
		}
		responseBody := body
		if c.Method() == fh.MethodHEADStr || status == fh.StatusNoContent || status == fh.StatusResetContent || status == fh.StatusNotModified || (status >= 100 && status < 200) {
			responseBody = nil
		}
		payload := protocol.ResponsePayload{
			Status: status, ContentType: contentType, Headers: headers,
			Body: append([]byte(nil), responseBody...),
		}
		defer wipe(payload.Body)
		now := time.Now()
		expires := now.Add(t.cfg.RequestTTL)
		if expires.After(session.ExpiresAt) {
			expires = session.ExpiresAt
		}
		if !expires.After(now) {
			return nil, errors.New("secure transport: session expired before response could be protected")
		}
		nonce, err := protocol.NewAEADNonce()
		if err != nil {
			return nil, err
		}
		env := protocol.ResponseEnvelope{
			SessionID: session.ID,
			RequestID: request.RequestID,
			Sequence:  request.Sequence,
			IssuedAt:  now.UnixMilli(),
			ExpiresAt: expires.UnixMilli(),
			Nonce:     nonce,
		}
		encrypted, err := protocol.EncryptResponse(session.Keys.ServerToClient, status, env, payload, t.cfg.Limits)
		if err != nil {
			return nil, err
		}
		if t.cfg.HideResponseHeaders {
			t.hideResponseHeaders(c)
		}
		c.Set(protocol.HeaderSecure, "1")
		c.Set("Cache-Control", "no-store")
		c.Set("Pragma", "no-cache")
		c.Set("X-Content-Type-Options", "nosniff")
		c.Append("Vary", protocol.HeaderSecure)
		c.Type(protocol.MediaTypeRequest)
		if c.Method() == fh.MethodHEADStr || status == fh.StatusNoContent || status == fh.StatusResetContent || status == fh.StatusNotModified || (status >= 100 && status < 200) {
			c.Set(protocol.HeaderResponse, base64.RawURLEncoding.EncodeToString(encrypted))
			return nil, nil
		}
		return encrypted, nil
	})
}

// sanitizeOuterRequestHeaders removes application-controlled plaintext
// headers before restoring the authenticated headers carried by the envelope.
// Browser-generated context headers remain available to downstream security
// policy and diagnostics.
func (t *Transport) sanitizeOuterRequestHeaders(c fh.Ctx) {
	for name := range c.GetReqHeaders() {
		if t.preserveOuterRequestHeader(name) {
			continue
		}
		c.RequestHeader().Del(name)
	}
}

func (t *Transport) preserveOuterRequestHeader(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if _, ok := t.preserveOuter[lower]; ok {
		return true
	}
	switch lower {
	case "host", "cookie", "user-agent", "origin", "referer", "accept-encoding", "accept-language":
		return true
	default:
		return strings.HasPrefix(lower, "sec-fetch-") || strings.HasPrefix(lower, "sec-ch-ua")
	}
}

func (t *Transport) collectResponseHeaders(c fh.Ctx) ([]protocol.Header, error) {
	all := c.GetRespHeaders()
	out := make([]protocol.Header, 0, len(all))
	for name, values := range all {
		lower := strings.ToLower(name)
		if lower == "content-type" || lower == "set-cookie" || isHopByHop(lower) || lower == strings.ToLower(protocol.HeaderResponse) || lower == strings.ToLower(protocol.HeaderSecure) {
			continue
		}
		for _, value := range values {
			if len(out) >= t.cfg.Limits.MaxHeaders {
				return nil, protocol.ErrTooLarge
			}
			if len(lower) > t.cfg.Limits.MaxHeaderName || len(value) > t.cfg.Limits.MaxHeaderValue || strings.ContainsAny(value, "\x00\r\n") {
				return nil, protocol.ErrMalformed
			}
			out = append(out, protocol.Header{Name: lower, Value: value})
		}
	}
	return out, nil
}

func (t *Transport) hideResponseHeaders(c fh.Ctx) {
	for name := range c.GetRespHeaders() {
		if preserveOuterHeader(name) {
			continue
		}
		fh.DeleteResponseHeader(c, name)
	}
}

func preserveOuterHeader(name string) bool {
	lower := strings.ToLower(name)
	if lower == "set-cookie" || lower == "vary" || lower == "cache-control" || lower == "strict-transport-security" || lower == "content-security-policy" || lower == "permissions-policy" || lower == "referrer-policy" || lower == "cross-origin-opener-policy" || lower == "cross-origin-embedder-policy" || lower == "cross-origin-resource-policy" {
		return true
	}
	return strings.HasPrefix(lower, "access-control-")
}

func isHopByHop(lower string) bool {
	switch lower {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "content-length":
		return true
	default:
		return false
	}
}

func (t *Transport) readEnvelope(c fh.Ctx) ([]byte, error) {
	if value := strings.TrimSpace(c.Get(protocol.HeaderEnvelope)); value != "" {
		if len(value) > 32<<10 {
			return nil, fh.NewHTTPError(fh.StatusRequestHeaderFieldsTooLarge, "FH_SECURE_HEADER_TOO_LARGE", "secure envelope header is too large")
		}
		data, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil {
			return nil, t.authFailure()
		}
		return data, nil
	}
	if len(c.BodyRaw()) == 0 {
		return nil, t.authFailure()
	}
	return c.BodyRaw(), nil
}

func (t *Transport) registerDevice(c fh.Ctx) error {
	if err := t.validateBrowserContext(c); err != nil {
		return err
	}
	if len(c.BodyRaw()) > 4096 {
		return fh.NewHTTPError(fh.StatusPayloadTooLarge, "FH_DEVICE_REQUEST_TOO_LARGE", "device registration request is too large")
	}
	request, err := protocol.DecodeDeviceRegistrationRequest(c.BodyRaw())
	if err != nil {
		return fh.NewHTTPError(fh.StatusBadRequest, "FH_DEVICE_REQUEST_INVALID", "device registration request is invalid")
	}
	now := time.Now()
	if delta := now.Sub(time.UnixMilli(request.IssuedAt)); delta > t.cfg.HandshakeTTL+t.cfg.MaxClockSkew || delta < -t.cfg.MaxClockSkew {
		return fh.NewHTTPError(fh.StatusUnauthorized, "FH_DEVICE_REQUEST_EXPIRED", "device registration request expired")
	}
	principal := ""
	if t.cfg.AuthorizeDeviceRegistration != nil {
		principal, err = t.cfg.AuthorizeDeviceRegistration(c, request)
		if err != nil {
			return err
		}
	} else if !t.cfg.AllowUnauthenticatedDeviceRegistration {
		return fh.NewHTTPError(fh.StatusForbidden, "FH_DEVICE_REGISTRATION_DISABLED", "device registration is not authorized")
	}
	replayKey := "device-registration:" + base64.RawURLEncoding.EncodeToString(request.Nonce[:])
	accepted, err := t.cfg.ReplayStore.CheckAndStore(replayKey, now.Add(t.cfg.HandshakeTTL))
	if err != nil || !accepted {
		return fh.NewHTTPError(fh.StatusConflict, "FH_DEVICE_REPLAY", "device registration request was already used")
	}
	id, err := protocol.NewID()
	if err != nil {
		return err
	}
	device := Device{ID: id, PublicKey: request.PublicKey, Name: request.Name, Principal: principal, CreatedAt: now, LastSeen: now}
	if err := t.cfg.DeviceStore.Register(device); err != nil {
		return err
	}
	response, _ := (protocol.DeviceRegistrationResponse{DeviceID: id, CreatedAt: now.UnixMilli()}).Encode()
	t.secureControlResponse(c)
	t.event(c, "device_registered", protocol.EncodeID(id), "", "", "")
	return c.Type(protocol.MediaTypeHandshake).SendBytes(response)
}

func (t *Transport) createSession(c fh.Ctx) error {
	if err := t.validateBrowserContext(c); err != nil {
		return err
	}
	if len(c.BodyRaw()) > 4096 {
		return fh.NewHTTPError(fh.StatusPayloadTooLarge, "FH_SESSION_REQUEST_TOO_LARGE", "session request is too large")
	}
	hello, err := protocol.DecodeClientHello(c.BodyRaw())
	if err != nil {
		return t.authFailure()
	}
	now := time.Now()
	if err := protocol.ValidateTime(hello.IssuedAt, hello.ExpiresAt, now, t.cfg.MaxClockSkew); err != nil || time.UnixMilli(hello.ExpiresAt).Sub(time.UnixMilli(hello.IssuedAt)) > t.cfg.HandshakeTTL {
		return t.authFailure()
	}
	device, err := t.cfg.DeviceStore.Get(hello.DeviceID)
	if err != nil {
		return t.authFailure()
	}
	signingBytes, err := hello.SigningBytes()
	if err != nil || !ed25519.Verify(ed25519.PublicKey(device.PublicKey[:]), signingBytes, hello.Signature[:]) {
		t.event(c, "device_proof_failed", protocol.EncodeID(hello.DeviceID), "", "", "invalid device signature")
		return t.authFailure()
	}
	replayKey := "handshake:" + protocol.EncodeID(hello.DeviceID) + ":" + base64.RawURLEncoding.EncodeToString(hello.Nonce[:])
	accepted, err := t.cfg.ReplayStore.CheckAndStore(replayKey, time.UnixMilli(hello.ExpiresAt).Add(t.cfg.MaxClockSkew))
	if err != nil || !accepted {
		return fh.NewHTTPError(fh.StatusConflict, "FH_SESSION_REPLAY", "session request was already used")
	}
	clientPublic, err := ecdh.X25519().NewPublicKey(hello.ClientPublic[:])
	if err != nil {
		return t.authFailure()
	}
	staticShared, err := t.privateKey.ECDH(clientPublic)
	if err != nil {
		return t.authFailure()
	}
	defer wipe(staticShared)
	ephemeralPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	ephemeralShared, err := ephemeralPrivate.ECDH(clientPublic)
	if err != nil {
		return t.authFailure()
	}
	defer wipe(ephemeralShared)
	combinedShared := make([]byte, 0, len(staticShared)+len(ephemeralShared))
	combinedShared = append(combinedShared, staticShared...)
	combinedShared = append(combinedShared, ephemeralShared...)
	defer wipe(combinedShared)
	sessionID, err := protocol.NewID()
	if err != nil {
		return err
	}
	serverNonce, err := protocol.NewNonce16()
	if err != nil {
		return err
	}
	expires := now.Add(t.cfg.SessionTTL)
	serverHello := protocol.ServerHello{ServerPublic: t.publicKey, SessionID: sessionID, ServerNonce: serverNonce, ExpiresAt: expires.UnixMilli(), KeyID: t.cfg.KeyID}
	copy(serverHello.ServerEphemeral[:], ephemeralPrivate.PublicKey().Bytes())
	clientRaw, _ := hello.Encode()
	serverCore, _ := serverHello.CoreBytes()
	keys := protocol.DeriveSessionKeys(combinedShared, clientRaw, serverCore)
	serverHello.Proof = protocol.ServerProof(keys.ServerToClient, clientRaw, serverCore)
	session := Session{ID: sessionID, DeviceID: device.ID, Principal: device.Principal, Keys: keys, CreatedAt: now, ExpiresAt: expires, KeyID: t.cfg.KeyID}
	if err := t.cfg.SessionStore.Create(session); err != nil {
		zeroSession(&session)
		wipe(keys.ClientToServer[:])
		wipe(keys.ServerToClient[:])
		return err
	}
	zeroSession(&session)
	wipe(keys.ClientToServer[:])
	wipe(keys.ServerToClient[:])
	response, _ := serverHello.Encode()
	t.secureControlResponse(c)
	t.event(c, "session_created", protocol.EncodeID(device.ID), protocol.EncodeID(sessionID), "", "")
	return c.Type(protocol.MediaTypeHandshake).SendBytes(response)
}

func (t *Transport) revokeSession(c fh.Ctx) error {
	// This endpoint itself must be called through secure transport. It is left as
	// a normal route so the middleware decrypts and binds LocalSession first.
	session, ok := SessionFromContext(c)
	if !ok {
		return t.authFailure()
	}
	if err := t.cfg.SessionStore.Delete(session.ID); err != nil {
		return err
	}
	t.event(c, "session_revoked", protocol.EncodeID(session.DeviceID), protocol.EncodeID(session.ID), "", "")
	return c.SendStatus(fh.StatusNoContent)
}

func (t *Transport) validateBrowserContext(c fh.Ctx) error {
	origin := strings.TrimRight(strings.TrimSpace(c.Get(fh.HeaderOriginStr)), "/")
	fetchSite := strings.ToLower(strings.TrimSpace(c.Get("Sec-Fetch-Site")))
	if origin == "" {
		// Browsers commonly omit Origin on same-origin GET/HEAD requests. Unsafe
		// methods and control-plane POSTs may also omit it when mediated by a
		// service worker, so accept explicit same-origin Fetch Metadata plus a
		// Host that matches an allowed origin.
		if t.cfg.RequireOrigin && c.Method() != fh.MethodGETStr && c.Method() != fh.MethodHEADStr && c.Method() != fh.MethodOPTIONSStr {
			if fetchSite != "same-origin" || !t.hostMatchesAllowedOrigin(c.Get(fh.HeaderHostStr)) {
				return fh.NewHTTPError(fh.StatusForbidden, "FH_ORIGIN_REQUIRED", "request origin is required")
			}
		}
	} else if len(t.allowedOrigin) > 0 {
		if _, ok := t.allowedOrigin[origin]; !ok {
			return fh.NewHTTPError(fh.StatusForbidden, "FH_ORIGIN_REJECTED", "request origin is not allowed")
		}
	} else if !sameOriginHost(origin, c.Get(fh.HeaderHostStr)) {
		return fh.NewHTTPError(fh.StatusForbidden, "FH_ORIGIN_REJECTED", "request origin does not match host")
	}
	if fetchSite == "cross-site" || (!t.cfg.AllowSameSite && fetchSite == "same-site") {
		return fh.NewHTTPError(fh.StatusForbidden, "FH_FETCH_CONTEXT_REJECTED", "cross-site secure request is not allowed")
	}
	return nil
}

func (t *Transport) hostMatchesAllowedOrigin(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if len(t.allowedOrigin) == 0 {
		return true
	}
	for origin := range t.allowedOrigin {
		parsed, err := url.Parse(origin)
		if err == nil && strings.EqualFold(parsed.Host, host) {
			return true
		}
	}
	return false
}

func sameOriginHost(origin, host string) bool {
	parsedOrigin, err := url.Parse(origin)
	if err != nil || parsedOrigin.Host == "" || parsedOrigin.User != nil {
		return false
	}
	parsedHost, err := url.Parse("//" + strings.TrimSpace(host))
	if err != nil || parsedHost.Host == "" || parsedHost.User != nil {
		return false
	}
	if !strings.EqualFold(parsedOrigin.Hostname(), parsedHost.Hostname()) {
		return false
	}
	originPort := parsedOrigin.Port()
	if originPort == "" {
		originPort = defaultOriginPort(parsedOrigin.Scheme)
	}
	hostPort := parsedHost.Port()
	if hostPort == "" {
		hostPort = defaultOriginPort(parsedOrigin.Scheme)
	}
	return originPort != "" && originPort == hostPort
}

func defaultOriginPort(scheme string) string {
	switch strings.ToLower(scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}

func (t *Transport) secureControlResponse(c fh.Ctx) {
	c.Set("Cache-Control", "no-store")
	c.Set("Pragma", "no-cache")
	c.Set("X-Content-Type-Options", "nosniff")
	c.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; sandbox")
	c.Append("Vary", "Origin")
	c.Set(protocol.HeaderServerKey, t.PublicKeyBase64())
}

func (t *Transport) isControlPath(path string) bool {
	return path == t.cfg.Prefix+"/device/register" || path == t.cfg.Prefix+"/session"
}

func (t *Transport) authFailure() error {
	return fh.NewHTTPError(fh.StatusUnauthorized, "FH_SECURE_AUTHENTICATION_FAILED", "secure request authentication failed")
}

func (t *Transport) event(c fh.Ctx, kind, deviceID, sessionID, requestID, detail string) {
	if t.cfg.OnSecurityEvent == nil {
		return
	}
	t.cfg.OnSecurityEvent(SecurityEvent{Type: kind, IP: c.IP(), DeviceID: deviceID, SessionID: sessionID, RequestID: requestID, Detail: detail, At: time.Now().UTC()})
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// ConstantTimePublicKeyMatch is exposed for pinned-key policies and tests.
func ConstantTimePublicKeyMatch(expected, actual [32]byte) bool {
	return subtle.ConstantTimeCompare(expected[:], actual[:]) == 1
}
