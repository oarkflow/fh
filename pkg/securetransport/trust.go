package securetransport

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
)

var ErrTrustUnavailable = errors.New("secure transport: embedded trust bundle is unavailable")

// TrustBundle is an immutable client root of trust intended to be embedded in
// a signed application or WASM build. It contains public material only.
type TrustBundle struct {
	Origin                   string
	TransportPublicKey       [X25519KeySize]byte
	TransportKeyID           string
	ResponseSigningPublicKey [Ed25519PublicSize]byte
	ResponseSigningKeyID     string
}

// ParseTrustBundle validates build-injected trust values. Either every value
// must be present or every value must be empty; partial bundles fail closed.
func ParseTrustBundle(origin, transportPublicKey, transportKeyID, responseSigningPublicKey, responseSigningKeyID string) (TrustBundle, error) {
	values := []string{origin, transportPublicKey, transportKeyID, responseSigningPublicKey, responseSigningKeyID}
	present := 0
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			present++
		}
	}
	if present == 0 {
		return TrustBundle{}, ErrTrustUnavailable
	}
	if present != len(values) {
		return TrustBundle{}, errors.New("secure transport: embedded trust bundle is incomplete")
	}
	parsed, err := url.Parse(strings.TrimRight(origin, "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return TrustBundle{}, errors.New("secure transport: trusted origin must be an absolute origin")
	}
	if parsed.Scheme != "https" && !isTrustLoopback(parsed.Hostname()) {
		return TrustBundle{}, errors.New("secure transport: trusted origin requires HTTPS outside loopback")
	}
	transport, err := base64.RawURLEncoding.Strict().DecodeString(transportPublicKey)
	if err != nil || len(transport) != X25519KeySize {
		return TrustBundle{}, errors.New("secure transport: trusted X25519 public key is invalid")
	}
	response, err := base64.RawURLEncoding.Strict().DecodeString(responseSigningPublicKey)
	if err != nil || len(response) != Ed25519PublicSize {
		return TrustBundle{}, errors.New("secure transport: trusted Ed25519 response key is invalid")
	}
	if !validTrustID(transportKeyID) || !validTrustID(responseSigningKeyID) {
		return TrustBundle{}, errors.New("secure transport: trusted key ID is invalid")
	}
	if allZero(transport) || allZero(response) {
		return TrustBundle{}, errors.New("secure transport: trusted public keys must not be all zero")
	}
	bundle := TrustBundle{
		Origin:               strings.TrimRight(parsed.String(), "/"),
		TransportKeyID:       transportKeyID,
		ResponseSigningKeyID: responseSigningKeyID,
	}
	copy(bundle.TransportPublicKey[:], transport)
	copy(bundle.ResponseSigningPublicKey[:], response)
	clear(transport)
	clear(response)
	return bundle, nil
}

// Matches reports whether runtime configuration exactly matches the embedded
// bundle. It uses constant-time comparisons for keys and key identifiers.
func (b TrustBundle) Matches(origin string, transportPublicKey [X25519KeySize]byte, transportKeyID string, responseSigningPublicKey [Ed25519PublicSize]byte, responseSigningKeyID string) bool {
	return origin == b.Origin &&
		subtle.ConstantTimeCompare(transportPublicKey[:], b.TransportPublicKey[:]) == 1 &&
		constantTimeStringEqual(transportKeyID, b.TransportKeyID) &&
		subtle.ConstantTimeCompare(responseSigningPublicKey[:], b.ResponseSigningPublicKey[:]) == 1 &&
		constantTimeStringEqual(responseSigningKeyID, b.ResponseSigningKeyID)
}

func constantTimeStringEqual(a, b string) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func validTrustID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i := range value {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func isTrustLoopback(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "0.0.0.0", "::1":
		return true
	default:
		return false
	}
}

func allZero(value []byte) bool {
	var combined byte
	for _, b := range value {
		combined |= b
	}
	return combined == 0
}
