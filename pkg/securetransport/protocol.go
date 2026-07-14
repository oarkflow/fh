// Package securetransport implements the versioned binary protocol shared by
// the fh secure-transport middleware and its Go WebAssembly fetch client.
//
// The protocol is an application-layer security envelope. It is deliberately
// independent of fh and net/http so it can be reused by browsers, native
// clients, gateways, and tests without introducing an HTTP implementation.
package securetransport

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	Version = byte(1)

	MediaTypeRequest         = "application/fh-secure"
	MediaTypeHandshake       = "application/fh-secure-handshake"
	HeaderSecure             = "X-FH-Secure"
	HeaderEnvelope           = "X-FH-Envelope"
	HeaderResponse           = "X-FH-Response"
	HeaderServerKey          = "X-FH-Server-Key"
	HeaderDeviceRegistration = "X-FH-Device-Registration"

	DeviceIDSize         = 16
	SessionIDSize        = 16
	RequestIDSize        = 16
	RandomNonceSize      = 16
	AEADNonceSize        = 12
	X25519KeySize        = 32
	Ed25519PublicSize    = 32
	Ed25519SignatureSize = 64
	ProofSize            = 32

	DefaultMaxBody        = 16 << 20
	DefaultMaxHeaders     = 48
	DefaultMaxHeaderName  = 128
	DefaultMaxHeaderValue = 16 << 10
)

var (
	ErrMalformed      = errors.New("fh secure transport: malformed message")
	ErrUnsupported    = errors.New("fh secure transport: unsupported protocol version")
	ErrAuthentication = errors.New("fh secure transport: authentication failed")
	ErrExpired        = errors.New("fh secure transport: message expired")
	ErrTooLarge       = errors.New("fh secure transport: message exceeds configured limit")
)

var (
	magicDeviceRequest   = [4]byte{'F', 'H', 'D', '1'}
	magicDeviceResponse  = [4]byte{'F', 'H', 'D', '2'}
	magicClientHello     = [4]byte{'F', 'H', 'C', '1'}
	magicServerHello     = [4]byte{'F', 'H', 'S', '1'}
	magicRequest         = [4]byte{'F', 'H', 'R', '1'}
	magicResponse        = [4]byte{'F', 'H', 'O', '1'}
	magicRequestPayload  = [4]byte{'F', 'H', 'P', '1'}
	magicResponsePayload = [4]byte{'F', 'H', 'P', '2'}
)

type ID16 [16]byte

type Limits struct {
	MaxBody        int
	MaxHeaders     int
	MaxHeaderName  int
	MaxHeaderValue int
}

func (l Limits) withDefaults() Limits {
	if l.MaxBody <= 0 {
		l.MaxBody = DefaultMaxBody
	}
	if l.MaxHeaders <= 0 {
		l.MaxHeaders = DefaultMaxHeaders
	}
	if l.MaxHeaderName <= 0 {
		l.MaxHeaderName = DefaultMaxHeaderName
	}
	if l.MaxHeaderValue <= 0 {
		l.MaxHeaderValue = DefaultMaxHeaderValue
	}
	return l
}

func NewID() (ID16, error) {
	var id ID16
	_, err := rand.Read(id[:])
	return id, err
}

func NewNonce16() ([16]byte, error) {
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	return nonce, err
}

func NewAEADNonce() ([12]byte, error) {
	var nonce [12]byte
	_, err := rand.Read(nonce[:])
	return nonce, err
}

func EncodeID(id ID16) string { return base64.RawURLEncoding.EncodeToString(id[:]) }

func DecodeID(value string) (ID16, error) {
	var id ID16
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return id, fmt.Errorf("fh secure transport: decode id: %w", err)
	}
	if len(b) != len(id) {
		return id, ErrMalformed
	}
	copy(id[:], b)
	return id, nil
}

func EqualID(a, b ID16) bool { return subtle.ConstantTimeCompare(a[:], b[:]) == 1 }

// DeviceRegistrationRequest registers a per-installation signing public key.
// Registration authorization belongs to the application and is enforced by
// the middleware's AuthorizeDeviceRegistration callback.
type DeviceRegistrationRequest struct {
	IssuedAt  int64
	Nonce     [16]byte
	PublicKey [32]byte
	Name      string
}

func (m DeviceRegistrationRequest) Encode() ([]byte, error) {
	if len(m.Name) > 256 {
		return nil, ErrTooLarge
	}
	w := newWriter(4 + 1 + 8 + 16 + 32 + 2 + len(m.Name))
	w.bytes(magicDeviceRequest[:])
	w.u8(Version)
	w.i64(m.IssuedAt)
	w.bytes(m.Nonce[:])
	w.bytes(m.PublicKey[:])
	w.str16(m.Name)
	return w.done()
}

func DecodeDeviceRegistrationRequest(data []byte) (DeviceRegistrationRequest, error) {
	var out DeviceRegistrationRequest
	r := newReader(data)
	if !r.magic(magicDeviceRequest) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	out.IssuedAt = r.i64()
	r.fixed(out.Nonce[:])
	r.fixed(out.PublicKey[:])
	out.Name = r.str16(256)
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

type DeviceRegistrationResponse struct {
	DeviceID  ID16
	CreatedAt int64
}

func (m DeviceRegistrationResponse) Encode() ([]byte, error) {
	w := newWriter(4 + 1 + 16 + 8)
	w.bytes(magicDeviceResponse[:])
	w.u8(Version)
	w.bytes(m.DeviceID[:])
	w.i64(m.CreatedAt)
	return w.done()
}

func DecodeDeviceRegistrationResponse(data []byte) (DeviceRegistrationResponse, error) {
	var out DeviceRegistrationResponse
	r := newReader(data)
	if !r.magic(magicDeviceResponse) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	r.fixed(out.DeviceID[:])
	out.CreatedAt = r.i64()
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

type ClientHello struct {
	DeviceID     ID16
	ClientPublic [32]byte
	IssuedAt     int64
	ExpiresAt    int64
	Nonce        [16]byte
	ClientBuild  string
	Signature    [64]byte
}

func (m ClientHello) SigningBytes() ([]byte, error) {
	if len(m.ClientBuild) > 128 {
		return nil, ErrTooLarge
	}
	w := newWriter(4 + 1 + 16 + 32 + 8 + 8 + 16 + 2 + len(m.ClientBuild))
	w.bytes(magicClientHello[:])
	w.u8(Version)
	w.bytes(m.DeviceID[:])
	w.bytes(m.ClientPublic[:])
	w.i64(m.IssuedAt)
	w.i64(m.ExpiresAt)
	w.bytes(m.Nonce[:])
	w.str16(m.ClientBuild)
	return w.done()
}

func (m ClientHello) Encode() ([]byte, error) {
	prefix, err := m.SigningBytes()
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(prefix)+len(m.Signature))
	copy(out, prefix)
	copy(out[len(prefix):], m.Signature[:])
	return out, nil
}

func DecodeClientHello(data []byte) (ClientHello, error) {
	var out ClientHello
	if len(data) < 4+1+16+32+8+8+16+2+64 {
		return out, ErrMalformed
	}
	r := newReader(data)
	if !r.magic(magicClientHello) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	r.fixed(out.DeviceID[:])
	r.fixed(out.ClientPublic[:])
	out.IssuedAt = r.i64()
	out.ExpiresAt = r.i64()
	r.fixed(out.Nonce[:])
	out.ClientBuild = r.str16(128)
	r.fixed(out.Signature[:])
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

type ServerHello struct {
	// ServerPublic is the persistent pinned X25519 public key.
	ServerPublic [32]byte
	// ServerEphemeral is a per-session X25519 public key. Combining both
	// agreements gives server authentication through the pin and forward secrecy
	// after the ephemeral private key is discarded.
	ServerEphemeral [32]byte
	SessionID       ID16
	ServerNonce     [16]byte
	ExpiresAt       int64
	KeyID           string
	Proof           [32]byte
}

func (m ServerHello) CoreBytes() ([]byte, error) {
	if len(m.KeyID) > 128 {
		return nil, ErrTooLarge
	}
	w := newWriter(4 + 1 + 32 + 32 + 16 + 16 + 8 + 2 + len(m.KeyID))
	w.bytes(magicServerHello[:])
	w.u8(Version)
	w.bytes(m.ServerPublic[:])
	w.bytes(m.ServerEphemeral[:])
	w.bytes(m.SessionID[:])
	w.bytes(m.ServerNonce[:])
	w.i64(m.ExpiresAt)
	w.str16(m.KeyID)
	return w.done()
}

func (m ServerHello) Encode() ([]byte, error) {
	core, err := m.CoreBytes()
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(core)+len(m.Proof))
	copy(out, core)
	copy(out[len(core):], m.Proof[:])
	return out, nil
}

func DecodeServerHello(data []byte) (ServerHello, error) {
	var out ServerHello
	if len(data) < 4+1+32+32+16+16+8+2+32 {
		return out, ErrMalformed
	}
	r := newReader(data)
	if !r.magic(magicServerHello) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	r.fixed(out.ServerPublic[:])
	r.fixed(out.ServerEphemeral[:])
	r.fixed(out.SessionID[:])
	r.fixed(out.ServerNonce[:])
	out.ExpiresAt = r.i64()
	out.KeyID = r.str16(128)
	r.fixed(out.Proof[:])
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

type SessionKeys struct {
	ClientToServer [32]byte
	ServerToClient [32]byte
}

func DeriveSessionKeys(sharedSecret, clientHello, serverCore []byte) SessionKeys {
	transcript := sha256.New()
	transcript.Write([]byte("fh-secure-transport-v1"))
	transcript.Write(clientHello)
	transcript.Write(serverCore)
	salt := transcript.Sum(nil)
	prk := hkdfExtract(salt, sharedSecret)
	var keys SessionKeys
	copy(keys.ClientToServer[:], hkdfExpand(prk, []byte("fh-secure-v1 client-to-server"), 32))
	copy(keys.ServerToClient[:], hkdfExpand(prk, []byte("fh-secure-v1 server-to-client"), 32))
	zero(prk)
	zero(salt)
	return keys
}

func ServerProof(key [32]byte, clientHello, serverCore []byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	mac.Write([]byte("fh-secure-v1 server proof"))
	mac.Write(clientHello)
	mac.Write(serverCore)
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
}

func VerifyServerProof(key [32]byte, clientHello, serverCore []byte, proof [32]byte) bool {
	expected := ServerProof(key, clientHello, serverCore)
	return hmac.Equal(expected[:], proof[:])
}

type Header struct {
	Name  string
	Value string
}

type RequestPayload struct {
	ContentType string
	Headers     []Header
	Body        []byte
}

type ResponsePayload struct {
	Status      int
	ContentType string
	Headers     []Header
	Body        []byte
}

type RequestEnvelope struct {
	SessionID  ID16
	RequestID  ID16
	Sequence   uint64
	IssuedAt   int64
	ExpiresAt  int64
	Nonce      [12]byte
	Ciphertext []byte
}

type ResponseEnvelope struct {
	SessionID  ID16
	RequestID  ID16
	Sequence   uint64
	IssuedAt   int64
	ExpiresAt  int64
	Nonce      [12]byte
	Ciphertext []byte
}

func (p RequestPayload) encode(limits Limits) ([]byte, error) {
	limits = limits.withDefaults()
	if len(p.Body) > limits.MaxBody || len(p.Headers) > limits.MaxHeaders || len(p.ContentType) > limits.MaxHeaderValue {
		return nil, ErrTooLarge
	}
	headers := append([]Header(nil), p.Headers...)
	sort.SliceStable(headers, func(i, j int) bool {
		if strings.EqualFold(headers[i].Name, headers[j].Name) {
			return headers[i].Value < headers[j].Value
		}
		return strings.ToLower(headers[i].Name) < strings.ToLower(headers[j].Name)
	})
	w := newWriter(64 + len(p.Body))
	w.bytes(magicRequestPayload[:])
	w.u8(Version)
	w.str16(p.ContentType)
	w.u16(uint16(len(headers)))
	for _, h := range headers {
		name := strings.ToLower(strings.TrimSpace(h.Name))
		if len(name) == 0 || len(name) > limits.MaxHeaderName || len(h.Value) > limits.MaxHeaderValue || !ValidProtectedHeader(name, h.Value) {
			return nil, ErrMalformed
		}
		w.str16(name)
		w.bytes32([]byte(h.Value))
	}
	w.bytes32(p.Body)
	return w.done()
}

func decodeRequestPayload(data []byte, limits Limits) (RequestPayload, error) {
	var out RequestPayload
	limits = limits.withDefaults()
	r := newReader(data)
	if !r.magic(magicRequestPayload) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	out.ContentType = r.str16(limits.MaxHeaderValue)
	count := int(r.u16())
	if count > limits.MaxHeaders {
		return out, ErrTooLarge
	}
	out.Headers = make([]Header, 0, count)
	for i := 0; i < count; i++ {
		name := r.str16(limits.MaxHeaderName)
		value := string(r.bytes32(limits.MaxHeaderValue))
		if !ValidProtectedHeader(name, value) {
			return out, ErrMalformed
		}
		out.Headers = append(out.Headers, Header{Name: name, Value: value})
	}
	out.Body = append([]byte(nil), r.bytes32(limits.MaxBody)...)
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

func (p ResponsePayload) encode(limits Limits) ([]byte, error) {
	limits = limits.withDefaults()
	if p.Status < 100 || p.Status > 999 || len(p.Body) > limits.MaxBody || len(p.Headers) > limits.MaxHeaders || len(p.ContentType) > limits.MaxHeaderValue {
		return nil, ErrTooLarge
	}
	headers := append([]Header(nil), p.Headers...)
	sort.SliceStable(headers, func(i, j int) bool {
		if strings.EqualFold(headers[i].Name, headers[j].Name) {
			return headers[i].Value < headers[j].Value
		}
		return strings.ToLower(headers[i].Name) < strings.ToLower(headers[j].Name)
	})
	w := newWriter(64 + len(p.Body))
	w.bytes(magicResponsePayload[:])
	w.u8(Version)
	w.u16(uint16(p.Status))
	w.str16(p.ContentType)
	w.u16(uint16(len(headers)))
	for _, h := range headers {
		name := strings.ToLower(strings.TrimSpace(h.Name))
		if len(name) == 0 || len(name) > limits.MaxHeaderName || len(h.Value) > limits.MaxHeaderValue || !validHeaderSyntax(name, h.Value) {
			return nil, ErrMalformed
		}
		w.str16(name)
		w.bytes32([]byte(h.Value))
	}
	w.bytes32(p.Body)
	return w.done()
}

func decodeResponsePayload(data []byte, limits Limits) (ResponsePayload, error) {
	var out ResponsePayload
	limits = limits.withDefaults()
	r := newReader(data)
	if !r.magic(magicResponsePayload) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	out.Status = int(r.u16())
	if out.Status < 100 || out.Status > 999 {
		return out, ErrMalformed
	}
	out.ContentType = r.str16(limits.MaxHeaderValue)
	count := int(r.u16())
	if count > limits.MaxHeaders {
		return out, ErrTooLarge
	}
	out.Headers = make([]Header, 0, count)
	for i := 0; i < count; i++ {
		name := r.str16(limits.MaxHeaderName)
		value := string(r.bytes32(limits.MaxHeaderValue))
		if !validHeaderSyntax(name, value) {
			return out, ErrMalformed
		}
		out.Headers = append(out.Headers, Header{Name: name, Value: value})
	}
	out.Body = append([]byte(nil), r.bytes32(limits.MaxBody)...)
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

func EncryptRequest(key [32]byte, method, target string, env RequestEnvelope, payload RequestPayload, limits Limits) ([]byte, error) {
	plain, err := payload.encode(limits)
	if err != nil {
		return nil, err
	}
	defer zero(plain)
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	aad, err := requestAAD(method, target, env)
	if err != nil {
		return nil, fmt.Errorf("fh secure transport: build request aad: %w", err)
	}
	env.Ciphertext = aead.Seal(nil, env.Nonce[:], plain, aad)
	return env.Encode()
}

func DecryptRequest(key [32]byte, method, target string, data []byte, limits Limits) (RequestEnvelope, RequestPayload, error) {
	var payload RequestPayload
	env, err := DecodeRequestEnvelope(data, limits.withDefaults().MaxBody+64<<10)
	if err != nil {
		return env, payload, err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return env, payload, err
	}
	aad, err := requestAAD(method, target, env)
	if err != nil {
		return env, payload, fmt.Errorf("fh secure transport: build request aad: %w", err)
	}
	plain, err := aead.Open(nil, env.Nonce[:], env.Ciphertext, aad)
	if err != nil {
		return env, payload, fmt.Errorf("fh secure transport: decrypt: %w (underlying: %w)", ErrAuthentication, err)
	}
	defer zero(plain)
	payload, err = decodeRequestPayload(plain, limits)
	return env, payload, err
}

func EncryptResponse(key [32]byte, outerStatus int, env ResponseEnvelope, payload ResponsePayload, limits Limits) ([]byte, error) {
	plain, err := payload.encode(limits)
	if err != nil {
		return nil, err
	}
	defer zero(plain)
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	aad, err := responseAAD(outerStatus, env)
	if err != nil {
		return nil, fmt.Errorf("fh secure transport: build response aad: %w", err)
	}
	env.Ciphertext = aead.Seal(nil, env.Nonce[:], plain, aad)
	return env.Encode()
}

func DecryptResponse(key [32]byte, outerStatus int, data []byte, limits Limits) (ResponseEnvelope, ResponsePayload, error) {
	var payload ResponsePayload
	env, err := DecodeResponseEnvelope(data, limits.withDefaults().MaxBody+64<<10)
	if err != nil {
		return env, payload, err
	}
	aead, err := newAEAD(key)
	if err != nil {
		return env, payload, err
	}
	aad, err := responseAAD(outerStatus, env)
	if err != nil {
		return env, payload, fmt.Errorf("fh secure transport: build response aad: %w", err)
	}
	plain, err := aead.Open(nil, env.Nonce[:], env.Ciphertext, aad)
	if err != nil {
		return env, payload, fmt.Errorf("fh secure transport: decrypt: %w (underlying: %w)", ErrAuthentication, err)
	}
	defer zero(plain)
	payload, err = decodeResponsePayload(plain, limits)
	return env, payload, err
}

func (m RequestEnvelope) Encode() ([]byte, error) {
	if uint64(len(m.Ciphertext)) > uint64(^uint32(0)) {
		return nil, ErrTooLarge
	}
	w := newWriter(4 + 1 + 16 + 16 + 8 + 8 + 8 + 12 + 4 + len(m.Ciphertext))
	w.bytes(magicRequest[:])
	w.u8(Version)
	w.bytes(m.SessionID[:])
	w.bytes(m.RequestID[:])
	w.u64(m.Sequence)
	w.i64(m.IssuedAt)
	w.i64(m.ExpiresAt)
	w.bytes(m.Nonce[:])
	w.bytes32(m.Ciphertext)
	return w.done()
}

func DecodeRequestEnvelope(data []byte, maxCiphertext int) (RequestEnvelope, error) {
	var out RequestEnvelope
	r := newReader(data)
	if !r.magic(magicRequest) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	r.fixed(out.SessionID[:])
	r.fixed(out.RequestID[:])
	out.Sequence = r.u64()
	out.IssuedAt = r.i64()
	out.ExpiresAt = r.i64()
	r.fixed(out.Nonce[:])
	out.Ciphertext = append([]byte(nil), r.bytes32(maxCiphertext)...)
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

func (m ResponseEnvelope) Encode() ([]byte, error) {
	if uint64(len(m.Ciphertext)) > uint64(^uint32(0)) {
		return nil, ErrTooLarge
	}
	w := newWriter(4 + 1 + 16 + 16 + 8 + 8 + 8 + 12 + 4 + len(m.Ciphertext))
	w.bytes(magicResponse[:])
	w.u8(Version)
	w.bytes(m.SessionID[:])
	w.bytes(m.RequestID[:])
	w.u64(m.Sequence)
	w.i64(m.IssuedAt)
	w.i64(m.ExpiresAt)
	w.bytes(m.Nonce[:])
	w.bytes32(m.Ciphertext)
	return w.done()
}

func DecodeResponseEnvelope(data []byte, maxCiphertext int) (ResponseEnvelope, error) {
	var out ResponseEnvelope
	r := newReader(data)
	if !r.magic(magicResponse) || r.u8() != Version {
		return out, versionOrMalformed(r)
	}
	r.fixed(out.SessionID[:])
	r.fixed(out.RequestID[:])
	out.Sequence = r.u64()
	out.IssuedAt = r.i64()
	out.ExpiresAt = r.i64()
	r.fixed(out.Nonce[:])
	out.Ciphertext = append([]byte(nil), r.bytes32(maxCiphertext)...)
	if !r.finished() {
		return out, r.errOrMalformed()
	}
	return out, nil
}

func requestAAD(method, target string, m RequestEnvelope) ([]byte, error) {
	w := newWriter(128 + len(method) + len(target))
	w.bytes([]byte("FHREQ-AAD-1"))
	w.str16(strings.ToUpper(method))
	w.bytes32([]byte(target))
	w.bytes(m.SessionID[:])
	w.bytes(m.RequestID[:])
	w.u64(m.Sequence)
	w.i64(m.IssuedAt)
	w.i64(m.ExpiresAt)
	w.bytes(m.Nonce[:])
	return w.done()
}

func responseAAD(status int, m ResponseEnvelope) ([]byte, error) {
	w := newWriter(96)
	w.bytes([]byte("FHRES-AAD-1"))
	w.u16(uint16(status))
	w.bytes(m.SessionID[:])
	w.bytes(m.RequestID[:])
	w.u64(m.Sequence)
	w.i64(m.IssuedAt)
	w.i64(m.ExpiresAt)
	w.bytes(m.Nonce[:])
	return w.done()
}

func ValidateTime(issuedAt, expiresAt int64, now time.Time, maxClockSkew time.Duration) error {
	if issuedAt <= 0 || expiresAt <= issuedAt {
		return ErrMalformed
	}
	n := now.UnixMilli()
	skew := maxClockSkew.Milliseconds()
	if issuedAt > n+skew || expiresAt < n-skew {
		return ErrExpired
	}
	return nil
}

// ValidProtectedHeader rejects hop-by-hop, framing, browser security-envelope,
// and host headers. These values must never be restored from encrypted client
// input because doing so would permit request smuggling or protocol confusion.
func ValidProtectedHeader(name, value string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if !validHeaderSyntax(name, value) {
		return false
	}
	switch name {
	case "host", "content-length", "transfer-encoding", "connection", "upgrade", "proxy-connection", "keep-alive", "te", "trailer",
		strings.ToLower(HeaderSecure), strings.ToLower(HeaderEnvelope), strings.ToLower(HeaderResponse), strings.ToLower(HeaderDeviceRegistration), "sec-websocket-key", "sec-websocket-version":
		return false
	default:
		return true
	}
}

func validHeaderSyntax(name, value string) bool {
	if name == "" || strings.ContainsAny(value, "\x00\r\n") {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return false
		}
	}
	return true
}

func newAEAD(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func hkdfExtract(salt, ikm []byte) []byte {
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write(ikm)
	return mac.Sum(nil)
}

func hkdfExpand(prk, info []byte, length int) []byte {
	if length <= 0 || length > 255*sha256.Size {
		panic("securetransport: invalid hkdf length")
	}
	out := make([]byte, 0, length)
	var previous []byte
	for counter := byte(1); len(out) < length; counter++ {
		mac := hmac.New(sha256.New, prk)
		mac.Write(previous)
		mac.Write(info)
		mac.Write([]byte{counter})
		previous = mac.Sum(nil)
		need := length - len(out)
		if need > len(previous) {
			need = len(previous)
		}
		out = append(out, previous[:need]...)
	}
	zero(previous)
	return out
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

type writer struct {
	b   []byte
	err error
}

func newWriter(capacity int) *writer { return &writer{b: make([]byte, 0, capacity)} }
func (w *writer) bytes(v []byte) {
	if w.err == nil {
		w.b = append(w.b, v...)
	}
}
func (w *writer) u8(v byte)    { w.bytes([]byte{v}) }
func (w *writer) u16(v uint16) { var b [2]byte; binary.BigEndian.PutUint16(b[:], v); w.bytes(b[:]) }
func (w *writer) u32(v uint32) { var b [4]byte; binary.BigEndian.PutUint32(b[:], v); w.bytes(b[:]) }
func (w *writer) u64(v uint64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], v); w.bytes(b[:]) }
func (w *writer) i64(v int64)  { w.u64(uint64(v)) }
func (w *writer) str16(v string) {
	if len(v) > 0xffff {
		w.err = ErrTooLarge
		return
	}
	w.u16(uint16(len(v)))
	w.bytes([]byte(v))
}
func (w *writer) bytes32(v []byte) {
	if uint64(len(v)) > uint64(^uint32(0)) {
		w.err = ErrTooLarge
		return
	}
	w.u32(uint32(len(v)))
	w.bytes(v)
}
func (w *writer) done() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.b, nil
}

type reader struct {
	b               []byte
	off             int
	err             error
	versionMismatch bool
}

func newReader(b []byte) *reader { return &reader{b: b} }
func (r *reader) take(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b)-r.off {
		r.err = ErrMalformed
		return nil
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v
}
func (r *reader) magic(m [4]byte) bool { return bytes.Equal(r.take(4), m[:]) }
func (r *reader) u8() byte {
	b := r.take(1)
	if len(b) != 1 {
		return 0
	}
	if b[0] != Version && r.off == 5 {
		r.versionMismatch = true
	}
	return b[0]
}
func (r *reader) u16() uint16 {
	b := r.take(2)
	if len(b) != 2 {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (r *reader) u32() uint32 {
	b := r.take(4)
	if len(b) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (r *reader) u64() uint64 {
	b := r.take(8)
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
func (r *reader) i64() int64 { return int64(r.u64()) }
func (r *reader) fixed(dst []byte) {
	b := r.take(len(dst))
	if len(b) == len(dst) {
		copy(dst, b)
	}
}
func (r *reader) str16(max int) string {
	n := int(r.u16())
	if n > max {
		r.err = ErrTooLarge
		return ""
	}
	return string(r.take(n))
}
func (r *reader) bytes32(max int) []byte {
	n64 := uint64(r.u32())
	if n64 > uint64(max) {
		r.err = ErrTooLarge
		return nil
	}
	return r.take(int(n64))
}
func (r *reader) finished() bool { return r.err == nil && r.off == len(r.b) }
func (r *reader) errOrMalformed() error {
	if r.err != nil {
		return r.err
	}
	return ErrMalformed
}
func versionOrMalformed(r *reader) error {
	if r.versionMismatch {
		return ErrUnsupported
	}
	return r.errOrMalformed()
}

func FingerprintPublicKey(key []byte) string {
	sum := sha256.Sum256(key)
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}

func FormatError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrMalformed) || errors.Is(err, ErrUnsupported) || errors.Is(err, ErrAuthentication) || errors.Is(err, ErrExpired) || errors.Is(err, ErrTooLarge) {
		return err
	}
	return fmt.Errorf("fh secure transport: %w", err)
}
