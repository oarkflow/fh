package platform

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"strings"
	"time"
)

// JWT signing and verification.
//
// This is implemented here rather than pulled in as a dependency because the
// platform needs exactly one thing from a JWT library — verify this token
// against this key, and mint one — and because the failure modes of JWT
// verification are the ones worth owning:
//
//   - The algorithm comes from the *key*, never from the token's own header. A
//     verifier that trusts the header's alg is how "alg: none" and
//     RSA-key-as-HMAC-secret confusion happen. Here, a key configured for RS256
//     will only ever verify RS256.
//   - Expiry, not-before and issued-at are checked with a bounded clock skew,
//     and a token with no exp at all is rejected unless the resource explicitly
//     opted into that.
//   - Issuer and audience are checked when configured, and a configured
//     audience must actually match — an empty aud claim does not pass.
//   - Signature comparison is constant-time for HMAC.
//
// Only the algorithm families a deployment actually needs are supported: HMAC
// for symmetric secrets, RSA-PKCS1v15 and ECDSA for asymmetric keys and OIDC
// providers. "none" is not implemented at all, which is the only correct amount
// of support for it.

var base64Raw = base64.RawURLEncoding

// JWTAlgorithm is a supported signing algorithm.
type JWTAlgorithm string

// The supported algorithms.
const (
	HS256 JWTAlgorithm = "HS256"
	HS384 JWTAlgorithm = "HS384"
	HS512 JWTAlgorithm = "HS512"
	RS256 JWTAlgorithm = "RS256"
	RS384 JWTAlgorithm = "RS384"
	RS512 JWTAlgorithm = "RS512"
	ES256 JWTAlgorithm = "ES256"
	ES384 JWTAlgorithm = "ES384"
	ES512 JWTAlgorithm = "ES512"
)

// ErrTokenInvalid is the single error every verification failure maps to.
//
// It is deliberately opaque. A caller learns that the token is not acceptable,
// not which of expiry, audience, issuer or signature failed — distinguishing
// those for an untrusted caller is an oracle, and the detail belongs in a server
// log, not a response body.
var ErrTokenInvalid = errors.New("token is not valid")

// JWTKey is one configured key, with its algorithm fixed at load time.
type JWTKey struct {
	ID        string
	Algorithm JWTAlgorithm
	secret    []byte
	rsaPublic *rsa.PublicKey
	rsaKey    *rsa.PrivateKey
	ecPublic  *ecdsa.PublicKey
	ecKey     *ecdsa.PrivateKey
}

// CanSign reports whether this key holds signing material, as opposed to
// verification material only.
func (k *JWTKey) CanSign() bool {
	return len(k.secret) > 0 || k.rsaKey != nil || k.ecKey != nil
}

// NewHMACKey builds a symmetric key. The minimum length is enforced here rather
// than trusted to the caller: HS256 with a short secret is brute-forceable
// offline, and a deployment should not be able to configure that by accident.
func NewHMACKey(algorithm JWTAlgorithm, secret []byte) (*JWTKey, error) {
	switch algorithm {
	case HS256, HS384, HS512:
	default:
		return nil, fmt.Errorf("%s is not an HMAC algorithm", algorithm)
	}
	if len(secret) < 32 {
		return nil, fmt.Errorf("an HMAC signing secret must contain at least 32 bytes, got %d", len(secret))
	}
	return &JWTKey{Algorithm: algorithm, secret: secret}, nil
}

// ParsePublicKeyPEM parses a PEM-encoded verification key and pairs it with an
// algorithm. The algorithm must match the key type, so a configuration that
// mismatches them fails at load rather than on every request.
func ParsePublicKeyPEM(algorithm JWTAlgorithm, data []byte) (*JWTKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found in the configured public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		certificate, certErr := x509.ParseCertificate(block.Bytes)
		if certErr != nil {
			return nil, fmt.Errorf("parse public key: %w", err)
		}
		parsed = certificate.PublicKey
	}
	key := &JWTKey{Algorithm: algorithm}
	switch typed := parsed.(type) {
	case *rsa.PublicKey:
		if !strings.HasPrefix(string(algorithm), "RS") {
			return nil, fmt.Errorf("%s cannot verify with an RSA key", algorithm)
		}
		key.rsaPublic = typed
	case *ecdsa.PublicKey:
		if !strings.HasPrefix(string(algorithm), "ES") {
			return nil, fmt.Errorf("%s cannot verify with an ECDSA key", algorithm)
		}
		key.ecPublic = typed
	default:
		return nil, fmt.Errorf("unsupported public key type %T", parsed)
	}
	return key, nil
}

// ParsePrivateKeyPEM parses a PEM-encoded signing key.
func ParsePrivateKeyPEM(algorithm JWTAlgorithm, data []byte) (*JWTKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found in the configured private key")
	}
	key := &JWTKey{Algorithm: algorithm}
	if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		switch typed := parsed.(type) {
		case *rsa.PrivateKey:
			key.rsaKey, key.rsaPublic = typed, &typed.PublicKey
			return key, nil
		case *ecdsa.PrivateKey:
			key.ecKey, key.ecPublic = typed, &typed.PublicKey
			return key, nil
		}
	}
	if parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		key.rsaKey, key.rsaPublic = parsed, &parsed.PublicKey
		return key, nil
	}
	if parsed, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		key.ecKey, key.ecPublic = parsed, &parsed.PublicKey
		return key, nil
	}
	return nil, errors.New("the configured private key is not a supported PKCS#8, PKCS#1 or SEC1 key")
}

// jwkKey is the subset of a JSON Web Key this package understands.
type jwkKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseJWKS converts a JWKS document into verification keys. Keys whose type or
// curve this package does not support are skipped rather than failing the whole
// set: a provider routinely publishes keys for algorithms we do not use, and
// refusing the document because of one of them would break verification for the
// keys we do use.
func parseJWKS(data []byte) (map[string]*JWTKey, error) {
	var document struct {
		Keys []jwkKey `json:"keys"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}
	keys := make(map[string]*JWTKey, len(document.Keys))
	for _, jwk := range document.Keys {
		if jwk.Use != "" && jwk.Use != "sig" {
			continue
		}
		key, err := jwkToKey(jwk)
		if err != nil || key == nil {
			continue
		}
		key.ID = jwk.Kid
		keys[jwk.Kid] = key
	}
	if len(keys) == 0 {
		return nil, errors.New("the JWKS document contained no usable signing keys")
	}
	return keys, nil
}

func jwkToKey(jwk jwkKey) (*JWTKey, error) {
	switch jwk.Kty {
	case "RSA":
		modulus, err := base64Raw.DecodeString(jwk.N)
		if err != nil {
			return nil, err
		}
		exponent, err := base64Raw.DecodeString(jwk.E)
		if err != nil {
			return nil, err
		}
		algorithm := JWTAlgorithm(jwk.Alg)
		switch algorithm {
		case RS256, RS384, RS512:
		default:
			algorithm = RS256
		}
		return &JWTKey{
			Algorithm: algorithm,
			rsaPublic: &rsa.PublicKey{
				N: new(big.Int).SetBytes(modulus),
				E: int(new(big.Int).SetBytes(exponent).Int64()),
			},
		}, nil
	case "EC":
		curve, algorithm, err := ecCurve(jwk.Crv)
		if err != nil {
			return nil, err
		}
		x, err := base64Raw.DecodeString(jwk.X)
		if err != nil {
			return nil, err
		}
		y, err := base64Raw.DecodeString(jwk.Y)
		if err != nil {
			return nil, err
		}
		return &JWTKey{
			Algorithm: algorithm,
			ecPublic: &ecdsa.PublicKey{
				Curve: curve,
				X:     new(big.Int).SetBytes(x),
				Y:     new(big.Int).SetBytes(y),
			},
		}, nil
	default:
		return nil, nil
	}
}

// JWTClaims is the registered claim set plus whatever else the token carried.
type JWTClaims struct {
	Issuer    string         `json:"iss,omitempty"`
	Subject   string         `json:"sub,omitempty"`
	Audience  []string       `json:"aud,omitempty"`
	ExpiresAt int64          `json:"exp,omitempty"`
	NotBefore int64          `json:"nbf,omitempty"`
	IssuedAt  int64          `json:"iat,omitempty"`
	ID        string         `json:"jti,omitempty"`
	Extra     map[string]any `json:"-"`
}

// JWTVerifyOptions is what a verifier checks beyond the signature.
type JWTVerifyOptions struct {
	// Issuer and Audience must match when set.
	Issuer   string
	Audience string
	// Skew tolerates clock drift between the issuer and this host. Default 60s.
	Skew time.Duration
	// AllowMissingExpiry permits a token with no exp claim. It defaults to
	// false, because a JWT without an expiry is a bearer credential that lives
	// forever, and that should be a deliberate decision.
	AllowMissingExpiry bool
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// SignJWT mints a token. Claims beyond the registered set are merged in from
// extra, which cannot override a registered claim — a caller-supplied "exp"
// must not be able to quietly extend a token's life past what the issuer chose.
func SignJWT(key *JWTKey, claims JWTClaims, extra map[string]any) (string, error) {
	if key == nil || !key.CanSign() {
		return "", errors.New("signing requires a key with private material")
	}
	header := map[string]any{"alg": string(key.Algorithm), "typ": "JWT"}
	if key.ID != "" {
		header["kid"] = key.ID
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}

	body := make(map[string]any, len(extra)+7)
	for name, value := range extra {
		body[name] = value
	}
	setClaim := func(name string, value any) {
		switch typed := value.(type) {
		case string:
			if typed == "" {
				return
			}
		case int64:
			if typed == 0 {
				return
			}
		case []string:
			if len(typed) == 0 {
				return
			}
		}
		body[name] = value
	}
	setClaim("iss", claims.Issuer)
	setClaim("sub", claims.Subject)
	setClaim("exp", claims.ExpiresAt)
	setClaim("nbf", claims.NotBefore)
	setClaim("iat", claims.IssuedAt)
	setClaim("jti", claims.ID)
	switch len(claims.Audience) {
	case 0:
	case 1:
		body["aud"] = claims.Audience[0]
	default:
		body["aud"] = claims.Audience
	}

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	signingInput := base64Raw.EncodeToString(headerJSON) + "." + base64Raw.EncodeToString(bodyJSON)
	signature, err := signJWTInput(key, []byte(signingInput))
	if err != nil {
		return "", err
	}
	return signingInput + "." + base64Raw.EncodeToString(signature), nil
}

// VerifyJWT verifies a token against one of the supplied keys and returns its
// claims.
//
// keys is a map from key id to key. A token with a kid selects that key; a token
// without one is tried against every key, which is what lets a deployment rotate
// a symmetric secret without a flag day.
func VerifyJWT(token string, keys map[string]*JWTKey, opts JWTVerifyOptions) (JWTClaims, map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return JWTClaims{}, nil, ErrTokenInvalid
	}
	headerJSON, err := base64Raw.DecodeString(parts[0])
	if err != nil {
		return JWTClaims{}, nil, ErrTokenInvalid
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return JWTClaims{}, nil, ErrTokenInvalid
	}
	// The header's algorithm is only used to reject an obvious mismatch; the
	// algorithm actually applied always comes from the configured key.
	if strings.EqualFold(header.Alg, "none") {
		return JWTClaims{}, nil, ErrTokenInvalid
	}

	signature, err := base64Raw.DecodeString(parts[2])
	if err != nil {
		return JWTClaims{}, nil, ErrTokenInvalid
	}
	signingInput := []byte(parts[0] + "." + parts[1])

	candidates := make([]*JWTKey, 0, len(keys))
	if header.Kid != "" {
		if key, ok := keys[header.Kid]; ok {
			candidates = append(candidates, key)
		}
	}
	if len(candidates) == 0 {
		for _, key := range keys {
			candidates = append(candidates, key)
		}
	}
	verified := false
	for _, key := range candidates {
		if key == nil || !strings.EqualFold(string(key.Algorithm), header.Alg) {
			continue
		}
		if verifyJWTSignature(key, signingInput, signature) {
			verified = true
			break
		}
	}
	if !verified {
		return JWTClaims{}, nil, ErrTokenInvalid
	}

	bodyJSON, err := base64Raw.DecodeString(parts[1])
	if err != nil {
		return JWTClaims{}, nil, ErrTokenInvalid
	}
	var body map[string]any
	if err := json.Unmarshal(bodyJSON, &body); err != nil {
		return JWTClaims{}, nil, ErrTokenInvalid
	}
	claims := claimsFrom(body)
	if err := validateClaims(claims, opts); err != nil {
		return JWTClaims{}, nil, err
	}
	return claims, body, nil
}

func claimsFrom(body map[string]any) JWTClaims {
	claims := JWTClaims{Extra: body}
	claims.Issuer, _ = body["iss"].(string)
	claims.Subject, _ = body["sub"].(string)
	claims.ID, _ = body["jti"].(string)
	if value, ok := ToFloat(body["exp"]); ok {
		claims.ExpiresAt = int64(value)
	}
	if value, ok := ToFloat(body["nbf"]); ok {
		claims.NotBefore = int64(value)
	}
	if value, ok := ToFloat(body["iat"]); ok {
		claims.IssuedAt = int64(value)
	}
	switch audience := body["aud"].(type) {
	case string:
		if audience != "" {
			claims.Audience = []string{audience}
		}
	case []any:
		for _, item := range audience {
			if text, ok := item.(string); ok {
				claims.Audience = append(claims.Audience, text)
			}
		}
	}
	return claims
}

func validateClaims(claims JWTClaims, opts JWTVerifyOptions) error {
	now := time.Now()
	if opts.Now != nil {
		now = opts.Now()
	}
	skew := opts.Skew
	if skew <= 0 {
		skew = time.Minute
	}
	if claims.ExpiresAt == 0 && !opts.AllowMissingExpiry {
		return ErrTokenInvalid
	}
	if claims.ExpiresAt != 0 && now.Add(-skew).Unix() >= claims.ExpiresAt {
		return ErrTokenInvalid
	}
	if claims.NotBefore != 0 && now.Add(skew).Unix() < claims.NotBefore {
		return ErrTokenInvalid
	}
	if claims.IssuedAt != 0 && now.Add(skew).Unix() < claims.IssuedAt {
		// A token issued in the future is either clock drift beyond our
		// tolerance or a forgery attempt. Neither is acceptable.
		return ErrTokenInvalid
	}
	if opts.Issuer != "" && claims.Issuer != opts.Issuer {
		return ErrTokenInvalid
	}
	if opts.Audience != "" {
		matched := false
		for _, audience := range claims.Audience {
			if audience == opts.Audience {
				matched = true
				break
			}
		}
		if !matched {
			return ErrTokenInvalid
		}
	}
	return nil
}

func signJWTInput(key *JWTKey, input []byte) ([]byte, error) {
	switch key.Algorithm {
	case HS256, HS384, HS512:
		mac := hmac.New(jwtHash(key.Algorithm), key.secret)
		mac.Write(input)
		return mac.Sum(nil), nil
	case RS256, RS384, RS512:
		if key.rsaKey == nil {
			return nil, errors.New("signing requires an RSA private key")
		}
		digest, cryptoHash := jwtDigest(key.Algorithm, input)
		return rsa.SignPKCS1v15(rand.Reader, key.rsaKey, cryptoHash, digest)
	case ES256, ES384, ES512:
		if key.ecKey == nil {
			return nil, errors.New("signing requires an ECDSA private key")
		}
		digest, _ := jwtDigest(key.Algorithm, input)
		r, s, err := ecdsa.Sign(rand.Reader, key.ecKey, digest)
		if err != nil {
			return nil, err
		}
		// JWS requires fixed-width, zero-padded R||S rather than the ASN.1
		// encoding ecdsa.SignASN1 would produce.
		size := (key.ecKey.Curve.Params().BitSize + 7) / 8
		signature := make([]byte, 2*size)
		r.FillBytes(signature[:size])
		s.FillBytes(signature[size:])
		return signature, nil
	default:
		return nil, fmt.Errorf("unsupported algorithm %q", key.Algorithm)
	}
}

func verifyJWTSignature(key *JWTKey, input, signature []byte) bool {
	switch key.Algorithm {
	case HS256, HS384, HS512:
		if len(key.secret) == 0 {
			return false
		}
		mac := hmac.New(jwtHash(key.Algorithm), key.secret)
		mac.Write(input)
		return subtle.ConstantTimeCompare(mac.Sum(nil), signature) == 1
	case RS256, RS384, RS512:
		if key.rsaPublic == nil {
			return false
		}
		digest, cryptoHash := jwtDigest(key.Algorithm, input)
		return rsa.VerifyPKCS1v15(key.rsaPublic, cryptoHash, digest, signature) == nil
	case ES256, ES384, ES512:
		if key.ecPublic == nil {
			return false
		}
		size := (key.ecPublic.Curve.Params().BitSize + 7) / 8
		if len(signature) != 2*size {
			return false
		}
		digest, _ := jwtDigest(key.Algorithm, input)
		r := new(big.Int).SetBytes(signature[:size])
		s := new(big.Int).SetBytes(signature[size:])
		return ecdsa.Verify(key.ecPublic, digest, r, s)
	default:
		return false
	}
}

func jwtHash(algorithm JWTAlgorithm) func() hash.Hash {
	switch algorithm {
	case HS384, RS384, ES384:
		return sha512.New384
	case HS512, RS512, ES512:
		return sha512.New
	default:
		return sha256.New
	}
}

func jwtDigest(algorithm JWTAlgorithm, input []byte) ([]byte, crypto.Hash) {
	switch algorithm {
	case RS384, ES384:
		sum := sha512.Sum384(input)
		return sum[:], crypto.SHA384
	case RS512, ES512:
		sum := sha512.Sum512(input)
		return sum[:], crypto.SHA512
	default:
		sum := sha256.Sum256(input)
		return sum[:], crypto.SHA256
	}
}

func ecCurve(name string) (elliptic.Curve, JWTAlgorithm, error) {
	switch name {
	case "P-256":
		return elliptic.P256(), ES256, nil
	case "P-384":
		return elliptic.P384(), ES384, nil
	case "P-521":
		return elliptic.P521(), ES512, nil
	default:
		return nil, "", fmt.Errorf("unsupported curve %q", name)
	}
}

// parseJWTAlgorithm validates a configured algorithm name.
func parseJWTAlgorithm(name string) (JWTAlgorithm, error) {
	algorithm := JWTAlgorithm(strings.ToUpper(strings.TrimSpace(name)))
	switch algorithm {
	case HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, ES512:
		return algorithm, nil
	case "":
		return HS256, nil
	case "NONE":
		return "", errors.New(`the "none" algorithm is not supported: an unsigned token is not a credential`)
	default:
		return "", fmt.Errorf("unsupported JWT algorithm %q", name)
	}
}
