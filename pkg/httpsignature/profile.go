// Package httpsignature implements a strict RFC 9421 response-signature
// profile using Ed25519 and RFC 9530 Content-Digest fields.
package httpsignature

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const (
	HeaderAcceptSignature = "Accept-Signature"
	HeaderSignatureInput  = "Signature-Input"
	HeaderSignature       = "Signature"
	HeaderContentDigest   = "Content-Digest"

	DefaultLabel = "sig1"
	DefaultTag   = "fh-rfc9421-response"
	Algorithm    = "ed25519"

	// CoveredComponents is the exact RFC 9421 Inner List used by this profile.
	// Request method and target URI bind the response to the request that caused
	// it; Content-Digest binds the exact response content bytes.
	CoveredComponents = "(\"@status\" \"content-digest\" \"content-type\" \"@method\";req \"@target-uri\";req)"
)

var (
	ErrMalformed   = errors.New("http signature: malformed structured field")
	ErrPolicy      = errors.New("http signature: profile policy violation")
	ErrSignature   = errors.New("http signature: verification failed")
	ErrDigest      = errors.New("http signature: content digest mismatch")
	ErrExpired     = errors.New("http signature: signature is expired or not yet valid")
	ErrNonce       = errors.New("http signature: nonce mismatch")
	ErrUnsupported = errors.New("http signature: unsupported signature parameters")
)

type Parameters struct {
	Created int64
	Expires int64
	Nonce   string
	KeyID   string
	Alg     string
	Tag     string
}

type AcceptRequest struct {
	Label string
	Nonce string
	KeyID string
	Alg   string
	Tag   string
	Raw   string
}

func NewNonce() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

// FormatAcceptSignature creates an RFC 9421 Accept-Signature dictionary member
// that requests this profile and asks the signer to echo the unique nonce.
func FormatAcceptSignature(label, nonce, keyID string) (string, error) {
	label = defaultString(label, DefaultLabel)
	if !validToken(label) || !validNonce(nonce) || !validString(keyID, 128) {
		return "", ErrPolicy
	}
	return label + "=" + CoveredComponents +
		";created;expires;nonce=" + quote(nonce) +
		";keyid=" + quote(keyID) +
		";alg=\"" + Algorithm + "\";tag=\"" + DefaultTag + "\"", nil
}

// ParseAcceptSignature parses the requested labeled member and enforces the
// exact covered-component and algorithm profile used by this package.
func ParseAcceptSignature(field, label string) (AcceptRequest, error) {
	label = defaultString(label, DefaultLabel)
	raw, err := dictionaryMember(field, label)
	if err != nil {
		return AcceptRequest{}, err
	}
	if !strings.HasPrefix(raw, CoveredComponents) {
		return AcceptRequest{}, ErrUnsupported
	}
	params, bare, err := parseParameters(raw[len(CoveredComponents):])
	if err != nil {
		return AcceptRequest{}, err
	}
	if !bare["created"] || !bare["expires"] || params.Nonce == "" || params.KeyID == "" || params.Alg != Algorithm || params.Tag != DefaultTag {
		return AcceptRequest{}, ErrUnsupported
	}
	if !validNonce(params.Nonce) {
		return AcceptRequest{}, ErrPolicy
	}
	return AcceptRequest{Label: label, Nonce: params.Nonce, KeyID: params.KeyID, Alg: params.Alg, Tag: params.Tag, Raw: raw}, nil
}

func FormatSignatureInput(label string, params Parameters) (string, string, error) {
	label = defaultString(label, DefaultLabel)
	if !validToken(label) || params.Created <= 0 || params.Expires <= params.Created || !validNonce(params.Nonce) || !validString(params.KeyID, 128) || params.Alg != Algorithm || params.Tag != DefaultTag {
		return "", "", ErrPolicy
	}
	raw := CoveredComponents +
		";created=" + strconv.FormatInt(params.Created, 10) +
		";expires=" + strconv.FormatInt(params.Expires, 10) +
		";nonce=" + quote(params.Nonce) +
		";keyid=" + quote(params.KeyID) +
		";alg=\"" + Algorithm + "\";tag=\"" + DefaultTag + "\""
	return label + "=" + raw, raw, nil
}

// SignatureBase constructs the exact RFC 9421 signature base for this profile.
func SignatureBase(status int, contentDigest, contentType, method, targetURI, signatureParams string) ([]byte, error) {
	if status < 100 || status > 999 || !validFieldValue(contentDigest) || !validFieldValue(contentType) || !validDerived(method) || !validDerived(targetURI) || !strings.HasPrefix(signatureParams, CoveredComponents) {
		return nil, ErrPolicy
	}
	var base strings.Builder
	fmt.Fprintf(&base, "\"@status\": %03d\n", status)
	base.WriteString("\"content-digest\": ")
	base.WriteString(contentDigest)
	base.WriteByte('\n')
	base.WriteString("\"content-type\": ")
	base.WriteString(contentType)
	base.WriteByte('\n')
	base.WriteString("\"@method\";req: ")
	base.WriteString(method)
	base.WriteByte('\n')
	base.WriteString("\"@target-uri\";req: ")
	base.WriteString(targetURI)
	base.WriteByte('\n')
	base.WriteString("\"@signature-params\": ")
	base.WriteString(signatureParams)
	return []byte(base.String()), nil
}

func FormatContentDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return "sha-256=:" + base64.StdEncoding.EncodeToString(sum[:]) + ":"
}

func VerifyContentDigest(field string, content []byte) error {
	expected := FormatContentDigest(content)
	if len(field) != len(expected) || !bytes.Equal([]byte(field), []byte(expected)) {
		return ErrDigest
	}
	return nil
}

func FormatSignature(label string, signature []byte) (string, error) {
	label = defaultString(label, DefaultLabel)
	if !validToken(label) || len(signature) != ed25519.SignatureSize {
		return "", ErrPolicy
	}
	return label + "=:" + base64.StdEncoding.EncodeToString(signature) + ":", nil
}

// SignResponse produces all three fields required by the response profile.
func SignResponse(privateKey ed25519.PrivateKey, label string, params Parameters, status int, contentType, method, targetURI string, content []byte) (contentDigest, signatureInput, signature string, err error) {
	contentDigest = FormatContentDigest(content)
	var rawParams string
	signatureInput, rawParams, err = FormatSignatureInput(label, params)
	if err != nil {
		return "", "", "", err
	}
	base, err := SignatureBase(status, contentDigest, contentType, method, targetURI, rawParams)
	if err != nil {
		return "", "", "", err
	}
	value, err := sign(privateKey, base)
	if err != nil {
		return "", "", "", err
	}
	signature, err = FormatSignature(label, value)
	if err != nil {
		return "", "", "", err
	}
	return contentDigest, signatureInput, signature, nil
}

func parseSignatureInput(field, label string) (string, Parameters, error) {
	raw, err := dictionaryMember(field, label)
	if err != nil {
		return "", Parameters{}, err
	}
	if !strings.HasPrefix(raw, CoveredComponents) {
		return "", Parameters{}, ErrUnsupported
	}
	params, bare, err := parseParameters(raw[len(CoveredComponents):])
	if err != nil {
		return "", Parameters{}, err
	}
	if len(bare) != 0 || params.Created <= 0 || params.Expires <= params.Created || params.Nonce == "" || params.KeyID == "" || params.Alg != Algorithm || params.Tag != DefaultTag {
		return "", Parameters{}, ErrUnsupported
	}
	return raw, params, nil
}

func parseSignature(field, label string) ([]byte, error) {
	raw, err := dictionaryMember(field, label)
	if err != nil {
		return nil, err
	}
	if len(raw) < 3 || raw[0] != ':' || raw[len(raw)-1] != ':' {
		return nil, ErrMalformed
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(raw[1 : len(raw)-1])
	if err != nil || len(decoded) != ed25519.SignatureSize {
		return nil, ErrMalformed
	}
	return decoded, nil
}

func parseParameters(input string) (Parameters, map[string]bool, error) {
	var out Parameters
	bare := make(map[string]bool)
	seen := make(map[string]bool)
	for input != "" {
		if input[0] != ';' {
			return out, nil, ErrMalformed
		}
		input = input[1:]
		i := 0
		for i < len(input) && isKeyChar(input[i]) {
			i++
		}
		if i == 0 {
			return out, nil, ErrMalformed
		}
		name := input[:i]
		if seen[name] {
			return out, nil, ErrMalformed
		}
		seen[name] = true
		input = input[i:]
		if input == "" || input[0] == ';' {
			if name != "created" && name != "expires" {
				return out, nil, ErrUnsupported
			}
			bare[name] = true
			continue
		}
		if input[0] != '=' {
			return out, nil, ErrMalformed
		}
		input = input[1:]
		if name == "created" || name == "expires" {
			end := 0
			for end < len(input) && input[end] >= '0' && input[end] <= '9' {
				end++
			}
			if end == 0 {
				return out, nil, ErrMalformed
			}
			value, err := strconv.ParseInt(input[:end], 10, 64)
			if err != nil {
				return out, nil, ErrMalformed
			}
			if name == "created" {
				out.Created = value
			} else {
				out.Expires = value
			}
			input = input[end:]
			continue
		}
		value, rest, err := consumeString(input)
		if err != nil {
			return out, nil, err
		}
		switch name {
		case "nonce":
			out.Nonce = value
		case "keyid":
			out.KeyID = value
		case "alg":
			out.Alg = value
		case "tag":
			out.Tag = value
		default:
			return out, nil, ErrUnsupported
		}
		input = rest
	}
	return out, bare, nil
}

func consumeString(input string) (string, string, error) {
	if len(input) < 2 || input[0] != '"' {
		return "", "", ErrMalformed
	}
	var value strings.Builder
	for i := 1; i < len(input); i++ {
		switch input[i] {
		case '"':
			return value.String(), input[i+1:], nil
		case '\\':
			i++
			if i >= len(input) || (input[i] != '\\' && input[i] != '"') {
				return "", "", ErrMalformed
			}
			value.WriteByte(input[i])
		default:
			if input[i] < 0x20 || input[i] > 0x7e {
				return "", "", ErrMalformed
			}
			value.WriteByte(input[i])
		}
	}
	return "", "", ErrMalformed
}

func dictionaryMember(field, label string) (string, error) {
	if !validToken(label) {
		return "", ErrPolicy
	}
	for _, member := range splitDictionary(field) {
		key, value, ok := strings.Cut(strings.TrimSpace(member), "=")
		if ok && key == label {
			value = strings.TrimSpace(value)
			if value == "" {
				return "", ErrMalformed
			}
			return value, nil
		}
	}
	return "", ErrMalformed
}

func splitDictionary(field string) []string {
	var out []string
	start, depth, quoted, bytesValue, escaped := 0, 0, false, false, false
	for i := 0; i < len(field); i++ {
		c := field[i]
		if escaped {
			escaped = false
			continue
		}
		if quoted {
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if bytesValue {
			if c == ':' {
				bytesValue = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case ':':
			bytesValue = true
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, field[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, field[start:])
	return out
}

func quote(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	return "\"" + strings.ReplaceAll(value, "\"", "\\\"") + "\""
}

func validNonce(value string) bool {
	if len(value) < 22 || len(value) > 128 {
		return false
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	return err == nil && len(decoded) >= 16 && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validToken(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := range value {
		if !isKeyChar(value[i]) {
			return false
		}
	}
	return true
}

func isKeyChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '*' || c == '.'
}

func validString(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for i := range value {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func validFieldValue(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && !strings.ContainsAny(value, "\x00\r\n")
}

func validDerived(value string) bool {
	return validFieldValue(value)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func joinedHeader(header http.Header, name string) string {
	values := header.Values(name)
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}
	return strings.Join(values, ", ")
}

func sign(privateKey ed25519.PrivateKey, base []byte) ([]byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, ErrPolicy
	}
	return ed25519.Sign(privateKey, base), nil
}

func verify(publicKey ed25519.PublicKey, base, signature []byte) bool {
	return len(publicKey) == ed25519.PublicKeySize && ed25519.Verify(publicKey, base, signature)
}
