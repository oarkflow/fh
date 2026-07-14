//go:build js && wasm

package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"syscall/js"
	"time"

	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

type deviceState struct {
	ID        protocol.ID16
	PublicKey [32]byte
	Name      string
}

type clientSession struct {
	ID        protocol.ID16
	Keys      protocol.SessionKeys
	ExpiresAt time.Time
	sequence  uint64
}

type clientConfig struct {
	BaseURL           string
	Prefix            string
	ClientBuild       string
	DeviceName        string
	Credentials       string
	RegistrationToken string
	PinnedServer      [32]byte
	HasPinnedKey      bool
	HandshakeTTL      time.Duration
	ClockSkew         time.Duration
	RequestTTL        time.Duration
	Limits            protocol.Limits
}

type secureClient struct {
	mu          sync.Mutex
	handshakeMu sync.Mutex
	cfg         clientConfig
	device      *deviceState
	session     *clientSession
	nativeFetch js.Value
	storage     js.Value
}

var (
	client = &secureClient{}
	kept   []js.Func
)

func main() {
	api := js.Global().Get("Object").New()
	register(api, "initialize", client.initializeJS)
	register(api, "request", client.requestJS)
	register(api, "revokeSession", client.revokeSessionJS)
	register(api, "sessionInfo", client.sessionInfoJS)
	js.Global().Set("FHSecureWasm", api)
	select {}
}

func register(api js.Value, name string, fn func(js.Value, []js.Value) any) {
	f := js.FuncOf(fn)
	kept = append(kept, f)
	api.Set(name, f)
}

func (c *secureClient) initializeJS(_ js.Value, args []js.Value) any {
	return promise(func() (js.Value, error) {
		if len(args) == 0 || args[0].Type() != js.TypeObject {
			return js.Undefined(), errors.New("fh secure fetch: configuration is required")
		}
		if err := c.configure(args[0]); err != nil {
			return js.Undefined(), err
		}
		device, err := c.loadOrRegisterDevice()
		if err != nil {
			return js.Undefined(), err
		}
		c.mu.Lock()
		c.device = device
		c.mu.Unlock()
		if err := c.establishSessionWithRecovery(); err != nil {
			return js.Undefined(), err
		}
		return c.infoObject(), nil
	})
}

func (c *secureClient) requestJS(_ js.Value, args []js.Value) any {
	return promise(func() (js.Value, error) {
		if len(args) == 0 || args[0].Type() != js.TypeObject {
			return js.Undefined(), errors.New("fh secure fetch: request descriptor is required")
		}
		return c.secureRequest(args[0])
	})
}

func (c *secureClient) revokeSessionJS(_ js.Value, _ []js.Value) any {
	return promise(func() (js.Value, error) {
		result, err := c.secureRequest(object(map[string]any{
			"url":         c.endpoint("/session/revoke"),
			"target":      c.cfg.Prefix + "/session/revoke",
			"method":      "POST",
			"headers":     js.Global().Get("Array").New(),
			"body":        uint8Array(nil),
			"credentials": c.cfg.Credentials,
		}))
		if err != nil {
			return js.Undefined(), err
		}
		c.mu.Lock()
		if c.session != nil {
			wipe(c.session.Keys.ClientToServer[:])
			wipe(c.session.Keys.ServerToClient[:])
		}
		c.session = nil
		c.mu.Unlock()
		return result, nil
	})
}

func (c *secureClient) sessionInfoJS(_ js.Value, _ []js.Value) any { return c.infoObject() }

func (c *secureClient) configure(v js.Value) error {
	baseURL := stringValue(v.Get("baseURL"))
	if baseURL == "" {
		location := js.Global().Get("location")
		if location.IsUndefined() || location.IsNull() {
			return errors.New("fh secure fetch: baseURL is required outside a browser window")
		}
		baseURL = location.Get("origin").String()
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("fh secure fetch: baseURL must be an absolute URL")
	}
	if parsed.Scheme != "https" && !isLoopbackHost(parsed.Hostname()) {
		return errors.New("fh secure fetch: HTTPS is required outside loopback development")
	}
	prefix := stringValue(v.Get("prefix"))
	if prefix == "" {
		prefix = "/__fh/secure/v1"
	}
	prefix = "/" + strings.Trim(prefix, "/")
	credentials := stringValue(v.Get("credentials"))
	if credentials == "" {
		credentials = "same-origin"
	}
	cfg := clientConfig{
		BaseURL:           strings.TrimRight(baseURL, "/"),
		Prefix:            prefix,
		ClientBuild:       defaultString(stringValue(v.Get("clientBuild")), "fh-secure-wasm-dev"),
		DeviceName:        defaultString(stringValue(v.Get("deviceName")), "Browser device"),
		Credentials:       credentials,
		RegistrationToken: stringValue(v.Get("registrationToken")),
		HandshakeTTL:      durationMillis(v.Get("handshakeTTL"), 2*time.Minute),
		ClockSkew:         durationMillis(v.Get("clockSkew"), 30*time.Second),
		RequestTTL:        durationMillis(v.Get("requestTTL"), 90*time.Second),
		Limits: protocol.Limits{
			MaxBody:        intValue(v.Get("maxBody"), protocol.DefaultMaxBody),
			MaxHeaders:     intValue(v.Get("maxHeaders"), protocol.DefaultMaxHeaders),
			MaxHeaderName:  protocol.DefaultMaxHeaderName,
			MaxHeaderValue: protocol.DefaultMaxHeaderValue,
		},
	}
	if pin := stringValue(v.Get("pinnedServerKey")); pin != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(pin)
		if err != nil || len(decoded) != len(cfg.PinnedServer) {
			return errors.New("fh secure fetch: pinnedServerKey must be a base64url X25519 public key")
		}
		copy(cfg.PinnedServer[:], decoded)
		wipe(decoded)
		cfg.HasPinnedKey = true
	}
	if !cfg.HasPinnedKey && !boolValue(v.Get("allowUnpinnedServerKey")) {
		return errors.New("fh secure fetch: pinnedServerKey is required; allowUnpinnedServerKey is development-only")
	}
	nativeFetch := js.Global().Get("__fhNativeFetch")
	if nativeFetch.Type() != js.TypeFunction {
		nativeFetch = js.Global().Get("fetch")
	}
	if nativeFetch.Type() != js.TypeFunction {
		return errors.New("fh secure fetch: native fetch is unavailable")
	}
	storage := js.Global().Get("__fhSecureStorage")
	if storage.IsUndefined() || storage.IsNull() {
		return errors.New("fh secure fetch: secure IndexedDB storage bridge is unavailable")
	}
	c.mu.Lock()
	if c.session != nil {
		wipe(c.session.Keys.ClientToServer[:])
		wipe(c.session.Keys.ServerToClient[:])
	}
	c.cfg = cfg
	c.nativeFetch = nativeFetch
	c.storage = storage
	c.session = nil
	c.mu.Unlock()
	return nil
}

func (c *secureClient) loadOrRegisterDevice() (*deviceState, error) {
	loaded, err := await(c.storage.Call("loadDevice"))
	if err != nil {
		return nil, fmt.Errorf("fh secure fetch: load device: %w", err)
	}
	if !loaded.IsNull() && !loaded.IsUndefined() {
		device, err := decodeStoredDevice(loaded)
		if err != nil {
			return nil, err
		}
		return device, nil
	}
	generated, err := await(c.storage.Call("createDeviceKey"))
	if err != nil {
		return nil, fmt.Errorf("fh secure fetch: create non-extractable device key: %w", err)
	}
	publicValue := generated.Get("publicKey")
	public, err := bytesFromJS(publicValue)
	if !publicValue.IsUndefined() && !publicValue.IsNull() {
		publicValue.Call("fill", 0)
	}
	if err != nil || len(public) != protocol.Ed25519PublicSize {
		return nil, errors.New("fh secure fetch: browser returned an invalid device public key")
	}
	defer wipe(public)
	request := protocol.DeviceRegistrationRequest{IssuedAt: time.Now().UnixMilli(), Name: c.cfg.DeviceName}
	nonce, err := protocol.NewNonce16()
	if err != nil {
		return nil, err
	}
	request.Nonce = nonce
	copy(request.PublicKey[:], public)
	encoded, err := request.Encode()
	if err != nil {
		return nil, err
	}
	response, err := c.fetchBinary(c.endpoint("/device/register"), "POST", encoded, protocol.MediaTypeHandshake, js.Undefined())
	if err != nil {
		return nil, err
	}
	defer wipe(response.body)
	registration, err := protocol.DecodeDeviceRegistrationResponse(response.body)
	if err != nil {
		return nil, err
	}
	device := &deviceState{ID: registration.DeviceID, Name: c.cfg.DeviceName}
	copy(device.PublicKey[:], public)
	stored := object(map[string]any{
		"id":        uint8Array(device.ID[:]),
		"publicKey": uint8Array(device.PublicKey[:]),
		"name":      device.Name,
	})
	if _, err := await(c.storage.Call("saveDevice", stored)); err != nil {
		return nil, fmt.Errorf("fh secure fetch: save device: %w", err)
	}
	return device, nil
}

func (c *secureClient) establishSessionWithRecovery() error {
	err := c.establishSession()
	if err == nil || !recoverableDeviceAuthFailure(err) {
		return err
	}
	if _, clearErr := await(c.storage.Call("clearDevice")); clearErr != nil {
		return fmt.Errorf("fh secure fetch: clear rejected device: %w", clearErr)
	}
	c.mu.Lock()
	if c.session != nil {
		wipe(c.session.Keys.ClientToServer[:])
		wipe(c.session.Keys.ServerToClient[:])
	}
	c.session = nil
	c.device = nil
	c.mu.Unlock()
	device, err := c.loadOrRegisterDevice()
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.device = device
	c.mu.Unlock()
	return c.establishSession()
}

func (c *secureClient) establishSession() error {
	c.handshakeMu.Lock()
	defer c.handshakeMu.Unlock()

	c.mu.Lock()
	if c.session != nil && time.Until(c.session.ExpiresAt) > 30*time.Second {
		c.mu.Unlock()
		return nil
	}
	device := c.device
	cfg := c.cfg
	c.mu.Unlock()
	if device == nil {
		return errors.New("fh secure fetch: device is not initialized")
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now()
	hello := protocol.ClientHello{DeviceID: device.ID, IssuedAt: now.UnixMilli(), ExpiresAt: now.Add(cfg.HandshakeTTL).UnixMilli(), ClientBuild: cfg.ClientBuild}
	copy(hello.ClientPublic[:], ephemeral.PublicKey().Bytes())
	nonce, err := protocol.NewNonce16()
	if err != nil {
		return err
	}
	hello.Nonce = nonce
	signing, err := hello.SigningBytes()
	if err != nil {
		return err
	}
	signingValue := uint8Array(signing)
	signatureValue, err := await(c.storage.Call("signClientHello", signingValue))
	signingValue.Call("fill", 0)
	if err != nil {
		return fmt.Errorf("fh secure fetch: device proof: %w", err)
	}
	signature, err := bytesFromJS(signatureValue)
	if !signatureValue.IsUndefined() && !signatureValue.IsNull() {
		signatureValue.Call("fill", 0)
	}
	if err != nil || len(signature) != protocol.Ed25519SignatureSize {
		wipe(signature)
		return errors.New("fh secure fetch: browser returned an invalid device signature")
	}
	copy(hello.Signature[:], signature)
	wipe(signature)
	clientRaw, err := hello.Encode()
	if err != nil {
		return err
	}
	response, err := c.fetchBinary(c.endpoint("/session"), "POST", clientRaw, protocol.MediaTypeHandshake, js.Undefined())
	if err != nil {
		return err
	}
	defer wipe(response.body)
	serverHello, err := protocol.DecodeServerHello(response.body)
	if err != nil {
		return err
	}
	if cfg.HasPinnedKey && subtle.ConstantTimeCompare(cfg.PinnedServer[:], serverHello.ServerPublic[:]) != 1 {
		return errors.New("fh secure fetch: server public key pin mismatch")
	}
	serverPublic, err := ecdh.X25519().NewPublicKey(serverHello.ServerPublic[:])
	if err != nil {
		return errors.New("fh secure fetch: invalid server public key")
	}
	serverEphemeral, err := ecdh.X25519().NewPublicKey(serverHello.ServerEphemeral[:])
	if err != nil {
		return errors.New("fh secure fetch: invalid server ephemeral key")
	}
	staticShared, err := ephemeral.ECDH(serverPublic)
	if err != nil {
		return errors.New("fh secure fetch: static key agreement failed")
	}
	defer wipe(staticShared)
	ephemeralShared, err := ephemeral.ECDH(serverEphemeral)
	if err != nil {
		return errors.New("fh secure fetch: ephemeral key agreement failed")
	}
	defer wipe(ephemeralShared)
	combinedShared := make([]byte, 0, len(staticShared)+len(ephemeralShared))
	combinedShared = append(combinedShared, staticShared...)
	combinedShared = append(combinedShared, ephemeralShared...)
	defer wipe(combinedShared)
	serverCore, _ := serverHello.CoreBytes()
	keys := protocol.DeriveSessionKeys(combinedShared, clientRaw, serverCore)
	defer wipe(keys.ClientToServer[:])
	defer wipe(keys.ServerToClient[:])
	if !protocol.VerifyServerProof(keys.ServerToClient, clientRaw, serverCore, serverHello.Proof) {
		return errors.New("fh secure fetch: server proof verification failed")
	}
	expires := time.UnixMilli(serverHello.ExpiresAt)
	if !expires.After(time.Now()) {
		return errors.New("fh secure fetch: server returned an expired session")
	}
	c.mu.Lock()
	if c.session != nil {
		wipe(c.session.Keys.ClientToServer[:])
		wipe(c.session.Keys.ServerToClient[:])
	}
	c.session = &clientSession{ID: serverHello.SessionID, Keys: keys, ExpiresAt: expires}
	c.mu.Unlock()
	return nil
}

func (c *secureClient) secureRequest(v js.Value) (js.Value, error) {
	if err := c.establishSessionWithRecovery(); err != nil {
		return js.Undefined(), err
	}
	requestURL := stringValue(v.Get("url"))
	target := stringValue(v.Get("target"))
	method := strings.ToUpper(defaultString(stringValue(v.Get("method")), "GET"))
	if requestURL == "" || target == "" || !strings.HasPrefix(target, "/") {
		return js.Undefined(), errors.New("fh secure fetch: url and request target are required")
	}
	if err := c.validateDestination(requestURL); err != nil {
		return js.Undefined(), err
	}
	body, err := bytesFromJS(v.Get("body"))
	if err != nil {
		return js.Undefined(), err
	}
	defer wipe(body)
	headers, contentType, err := headersFromJS(v.Get("headers"))
	if err != nil {
		return js.Undefined(), err
	}

	c.mu.Lock()
	session := c.session
	if session == nil {
		c.mu.Unlock()
		return js.Undefined(), errors.New("fh secure fetch: session is unavailable")
	}
	session.sequence++
	sequence := session.sequence
	sessionID := session.ID
	keys := session.Keys
	sessionExpiry := session.ExpiresAt
	cfg := c.cfg
	c.mu.Unlock()
	defer wipe(keys.ClientToServer[:])
	defer wipe(keys.ServerToClient[:])

	now := time.Now()
	expires := now.Add(cfg.RequestTTL)
	if expires.After(sessionExpiry) {
		expires = sessionExpiry
	}
	requestID, err := protocol.NewID()
	if err != nil {
		return js.Undefined(), err
	}
	nonce, err := protocol.NewAEADNonce()
	if err != nil {
		return js.Undefined(), err
	}
	env := protocol.RequestEnvelope{SessionID: sessionID, RequestID: requestID, Sequence: sequence, IssuedAt: now.UnixMilli(), ExpiresAt: expires.UnixMilli(), Nonce: nonce}
	encrypted, err := protocol.EncryptRequest(keys.ClientToServer, method, target, env, protocol.RequestPayload{ContentType: contentType, Headers: headers, Body: body}, cfg.Limits)
	if err != nil {
		return js.Undefined(), err
	}
	defer wipe(encrypted)

	outerHeaders := js.Global().Get("Headers").New()
	outerHeaders.Call("set", protocol.HeaderSecure, "1")
	outerHeaders.Call("set", "Accept", protocol.MediaTypeRequest)
	var outerBody js.Value
	if method == "GET" || method == "HEAD" {
		outerHeaders.Call("set", protocol.HeaderEnvelope, base64.RawURLEncoding.EncodeToString(encrypted))
		outerBody = js.Undefined()
	} else {
		outerHeaders.Call("set", "Content-Type", protocol.MediaTypeRequest)
		outerBody = uint8Array(encrypted)
	}
	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	opts.Set("headers", outerHeaders)
	opts.Set("credentials", defaultString(stringValue(v.Get("credentials")), cfg.Credentials))
	opts.Set("cache", "no-store")
	opts.Set("redirect", "error")
	if !outerBody.IsUndefined() {
		opts.Set("body", outerBody)
	}
	if signal := v.Get("signal"); !signal.IsUndefined() && !signal.IsNull() {
		opts.Set("signal", signal)
	}
	if mode := stringValue(v.Get("mode")); mode != "" {
		if mode != "cors" && mode != "same-origin" {
			return js.Undefined(), errors.New("fh secure fetch: only cors and same-origin request modes are allowed")
		}
		opts.Set("mode", mode)
	}
	opts.Set("referrerPolicy", "no-referrer")
	responseValue, err := await(c.nativeFetch.Invoke(requestURL, opts))
	if err != nil {
		return js.Undefined(), fmt.Errorf("fh secure fetch: network request failed: %w", err)
	}
	status := responseValue.Get("status").Int()
	responseHeaders := responseValue.Get("headers")
	if responseHeaders.Call("get", protocol.HeaderSecure).String() != "1" {
		return js.Undefined(), errors.New("fh secure fetch: server returned an unprotected response")
	}
	var encryptedResponse []byte
	if value := responseHeaders.Call("get", protocol.HeaderResponse); !value.IsNull() && value.String() != "" {
		encryptedResponse, err = base64.RawURLEncoding.DecodeString(value.String())
		if err != nil {
			return js.Undefined(), errors.New("fh secure fetch: malformed response envelope header")
		}
	} else {
		buffer, awaitErr := await(responseValue.Call("arrayBuffer"))
		if awaitErr != nil {
			return js.Undefined(), awaitErr
		}
		encryptedResponse, err = bytesFromJS(js.Global().Get("Uint8Array").New(buffer))
		if err != nil {
			return js.Undefined(), err
		}
	}
	responseEnv, payload, err := protocol.DecryptResponse(keys.ServerToClient, status, encryptedResponse, cfg.Limits)
	wipe(encryptedResponse)
	if err != nil {
		return js.Undefined(), errors.New("fh secure fetch: response authentication failed")
	}
	if !protocol.EqualID(responseEnv.SessionID, sessionID) || !protocol.EqualID(responseEnv.RequestID, requestID) || responseEnv.Sequence != sequence || payload.Status != status {
		return js.Undefined(), errors.New("fh secure fetch: response binding mismatch")
	}
	if err := protocol.ValidateTime(responseEnv.IssuedAt, responseEnv.ExpiresAt, time.Now(), cfg.ClockSkew); err != nil {
		return js.Undefined(), errors.New("fh secure fetch: response is expired")
	}
	resultHeaders := js.Global().Get("Array").New()
	for _, header := range payload.Headers {
		pair := js.Global().Get("Array").New()
		pair.Call("push", header.Name)
		pair.Call("push", header.Value)
		resultHeaders.Call("push", pair)
	}
	if payload.ContentType != "" {
		pair := js.Global().Get("Array").New()
		pair.Call("push", "content-type")
		pair.Call("push", payload.ContentType)
		resultHeaders.Call("push", pair)
	}
	result := js.Global().Get("Object").New()
	result.Set("status", payload.Status)
	result.Set("headers", resultHeaders)
	result.Set("body", uint8Array(payload.Body))
	wipe(payload.Body)
	result.Set("url", requestURL)
	result.Set("requestId", protocol.EncodeID(requestID))
	return result, nil
}

type binaryResponse struct {
	status int
	body   []byte
}

type controlEndpointError struct {
	endpoint string
	status   int
}

func (e controlEndpointError) Error() string {
	return fmt.Sprintf("fh secure fetch: control endpoint %s returned HTTP %d", e.endpoint, e.status)
}

func recoverableDeviceAuthFailure(err error) bool {
	var controlErr controlEndpointError
	if !errors.As(err, &controlErr) {
		return false
	}
	return strings.HasSuffix(controlErr.endpoint, "/session") && (controlErr.status == 401 || controlErr.status == 403)
}

func (c *secureClient) fetchBinary(endpoint, method string, body []byte, contentType string, signal js.Value) (binaryResponse, error) {
	headers := js.Global().Get("Headers").New()
	headers.Call("set", "Content-Type", contentType)
	headers.Call("set", "Accept", contentType)
	if strings.HasSuffix(endpoint, "/device/register") && c.cfg.RegistrationToken != "" {
		headers.Call("set", protocol.HeaderDeviceRegistration, c.cfg.RegistrationToken)
	}
	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	opts.Set("headers", headers)
	opts.Set("credentials", c.cfg.Credentials)
	opts.Set("cache", "no-store")
	opts.Set("redirect", "error")
	opts.Set("body", uint8Array(body))
	if !signal.IsUndefined() && !signal.IsNull() {
		opts.Set("signal", signal)
	}
	response, err := await(c.nativeFetch.Invoke(endpoint, opts))
	if err != nil {
		return binaryResponse{}, err
	}
	status := response.Get("status").Int()
	buffer, err := await(response.Call("arrayBuffer"))
	if err != nil {
		return binaryResponse{}, err
	}
	responseBody, err := bytesFromJS(js.Global().Get("Uint8Array").New(buffer))
	if err != nil {
		return binaryResponse{}, err
	}
	if status < 200 || status >= 300 {
		wipe(responseBody)
		return binaryResponse{}, controlEndpointError{endpoint: endpoint, status: status}
	}
	return binaryResponse{status: status, body: responseBody}, nil
}

func (c *secureClient) endpoint(suffix string) string { return c.cfg.BaseURL + c.cfg.Prefix + suffix }

func (c *secureClient) validateDestination(raw string) error {
	destination, err := url.Parse(raw)
	if err != nil || destination.Scheme == "" || destination.Host == "" {
		return errors.New("fh secure fetch: request URL must be absolute")
	}
	base, _ := url.Parse(c.cfg.BaseURL)
	if !strings.EqualFold(destination.Scheme, base.Scheme) || !strings.EqualFold(destination.Host, base.Host) {
		return errors.New("fh secure fetch: destination is outside the configured FH origin")
	}
	return nil
}

func (c *secureClient) infoObject() js.Value {
	result := js.Global().Get("Object").New()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.device != nil {
		result.Set("deviceId", protocol.EncodeID(c.device.ID))
		result.Set("deviceName", c.device.Name)
	}
	if c.session != nil {
		result.Set("sessionId", protocol.EncodeID(c.session.ID))
		result.Set("expiresAt", c.session.ExpiresAt.UnixMilli())
		result.Set("sequence", float64(c.session.sequence))
	}
	return result
}

func decodeStoredDevice(v js.Value) (*deviceState, error) {
	id, err := bytesFromJS(v.Get("id"))
	if err != nil || len(id) != protocol.DeviceIDSize {
		return nil, errors.New("fh secure fetch: stored device id is invalid")
	}
	public, err := bytesFromJS(v.Get("publicKey"))
	if err != nil || len(public) != protocol.Ed25519PublicSize {
		return nil, errors.New("fh secure fetch: stored device public key is invalid")
	}
	device := &deviceState{Name: stringValue(v.Get("name"))}
	copy(device.ID[:], id)
	copy(device.PublicKey[:], public)
	wipe(public)
	return device, nil
}

func headersFromJS(v js.Value) ([]protocol.Header, string, error) {
	if v.IsUndefined() || v.IsNull() {
		return nil, "", nil
	}
	length := v.Length()
	if length > protocol.DefaultMaxHeaders {
		return nil, "", protocol.ErrTooLarge
	}
	headers := make([]protocol.Header, 0, length)
	contentType := ""
	for i := 0; i < length; i++ {
		pair := v.Index(i)
		if pair.Length() < 2 {
			return nil, "", protocol.ErrMalformed
		}
		name := strings.ToLower(strings.TrimSpace(pair.Index(0).String()))
		value := pair.Index(1).String()
		if name == "content-type" {
			contentType = value
			continue
		}
		if !protocol.ValidProtectedHeader(name, value) {
			continue
		}
		headers = append(headers, protocol.Header{Name: name, Value: value})
	}
	return headers, contentType, nil
}

func promise(work func() (js.Value, error)) js.Value {
	constructor := js.Global().Get("Promise")
	executor := js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve, reject := args[0], args[1]
		go func() {
			value, err := work()
			if err != nil {
				reject.Invoke(js.Global().Get("Error").New(err.Error()))
				return
			}
			resolve.Invoke(value)
		}()
		return nil
	})
	p := constructor.New(executor)
	executor.Release()
	return p
}

func await(promise js.Value) (js.Value, error) {
	type result struct {
		value js.Value
		err   error
	}
	ch := make(chan result, 1)
	then := js.FuncOf(func(_ js.Value, args []js.Value) any {
		value := js.Undefined()
		if len(args) > 0 {
			value = args[0]
		}
		ch <- result{value: value}
		return nil
	})
	catch := js.FuncOf(func(_ js.Value, args []js.Value) any {
		message := "JavaScript promise rejected"
		if len(args) > 0 {
			if m := args[0].Get("message"); !m.IsUndefined() {
				message = m.String()
			} else {
				message = args[0].String()
			}
		}
		ch <- result{err: errors.New(message)}
		return nil
	})
	promise.Call("then", then).Call("catch", catch)
	out := <-ch
	then.Release()
	catch.Release()
	return out.value, out.err
}

func bytesFromJS(v js.Value) ([]byte, error) {
	if v.IsUndefined() || v.IsNull() {
		return nil, nil
	}
	length := v.Get("byteLength")
	if length.IsUndefined() {
		length = v.Get("length")
	}
	if length.IsUndefined() {
		return nil, errors.New("fh secure fetch: expected Uint8Array")
	}
	out := make([]byte, length.Int())
	if len(out) > 0 && js.CopyBytesToGo(out, v) != len(out) {
		return nil, errors.New("fh secure fetch: incomplete byte copy")
	}
	return out, nil
}

func uint8Array(data []byte) js.Value {
	array := js.Global().Get("Uint8Array").New(len(data))
	if len(data) > 0 {
		js.CopyBytesToJS(array, data)
	}
	return array
}

func object(values map[string]any) js.Value {
	obj := js.Global().Get("Object").New()
	for key, value := range values {
		obj.Set(key, value)
	}
	return obj
}

func stringValue(v js.Value) string {
	if v.IsUndefined() || v.IsNull() {
		return ""
	}
	return v.String()
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func boolValue(v js.Value) bool {
	return !v.IsUndefined() && !v.IsNull() && v.Type() == js.TypeBoolean && v.Bool()
}

func intValue(v js.Value, fallback int) int {
	if v.IsUndefined() || v.IsNull() || v.Type() != js.TypeNumber || v.Int() <= 0 {
		return fallback
	}
	return v.Int()
}

func durationMillis(v js.Value, fallback time.Duration) time.Duration {
	if v.IsUndefined() || v.IsNull() || v.Type() != js.TypeNumber || v.Int() <= 0 {
		return fallback
	}
	return time.Duration(v.Int()) * time.Millisecond
}

func isLoopbackHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
