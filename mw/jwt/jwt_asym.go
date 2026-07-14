package jwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"strings"
)

func verifySignature(alg string, keyPEM []byte, signingInput, sig []byte) error {
	alg = strings.ToUpper(alg)
	if h, ok := jwtHash(alg); ok {
		mac := hmac.New(h, keyPEM)
		mac.Write(signingInput)
		if !hmac.Equal(sig, mac.Sum(nil)) {
			return errors.New("signature mismatch")
		}
		return nil
	}
	if alg == "EDDSA" && len(keyPEM) == ed25519.PublicKeySize {
		if !ed25519.Verify(ed25519.PublicKey(keyPEM), signingInput, sig) {
			return errors.New("signature mismatch")
		}
		return nil
	}
	pub, err := parsePublicKey(keyPEM)
	if err != nil {
		return err
	}
	switch alg {
	case "RS256", "RS384", "RS512":
		rp, ok := pub.(*rsa.PublicKey)
		if !ok {
			return errors.New("rsa public key required")
		}
		hashID, digest := digestForAlg(alg, signingInput)
		if hashID == 0 {
			return fmt.Errorf("algorithm %s is not supported", alg)
		}
		if err := rsa.VerifyPKCS1v15(rp, hashID, digest, sig); err != nil {
			return errors.New("signature mismatch")
		}
		return nil
	case "PS256", "PS384", "PS512":
		rp, ok := pub.(*rsa.PublicKey)
		if !ok {
			return errors.New("rsa public key required")
		}
		hashID, digest := digestForAlg(alg, signingInput)
		if hashID == 0 {
			return fmt.Errorf("algorithm %s is not supported", alg)
		}
		if err := rsa.VerifyPSS(rp, hashID, digest, sig, nil); err != nil {
			return errors.New("signature mismatch")
		}
		return nil
	case "ES256", "ES384", "ES512":
		ep, ok := pub.(*ecdsa.PublicKey)
		if !ok {
			return errors.New("ecdsa public key required")
		}
		_, digest := digestForAlg(alg, signingInput)
		if len(sig)%2 != 0 {
			return errors.New("bad ecdsa signature")
		}
		n := len(sig) / 2
		r := new(big.Int).SetBytes(sig[:n])
		s := new(big.Int).SetBytes(sig[n:])
		if !ecdsa.Verify(ep, digest, r, s) {
			return errors.New("signature mismatch")
		}
		return nil
	case "EDDSA":
		ep, ok := pub.(ed25519.PublicKey)
		if !ok {
			return errors.New("ed25519 public key required")
		}
		if !ed25519.Verify(ep, signingInput, sig) {
			return errors.New("signature mismatch")
		}
		return nil
	default:
		return fmt.Errorf("algorithm %s is not supported", alg)
	}
}

func parsePublicKey(raw []byte) (any, error) {
	block, _ := pem.Decode(raw)
	if block != nil {
		raw = block.Bytes
	}
	if pub, err := x509.ParsePKIXPublicKey(raw); err == nil {
		return pub, nil
	}
	if cert, err := x509.ParseCertificate(raw); err == nil {
		return cert.PublicKey, nil
	}
	return nil, errors.New("invalid public key: not a valid PKIX public key or X.509 certificate")
}

func digestForAlg(alg string, data []byte) (crypto.Hash, []byte) {
	switch strings.ToUpper(alg) {
	case "RS256", "PS256", "ES256":
		sum := sha256.Sum256(data)
		return crypto.SHA256, sum[:]
	case "RS384", "PS384", "ES384":
		sum := sha512.Sum384(data)
		return crypto.SHA384, sum[:]
	case "RS512", "PS512", "ES512":
		sum := sha512.Sum512(data)
		return crypto.SHA512, sum[:]
	default:
		return 0, nil
	}
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	K   string `json:"k"`
}

// ParseJWKS converts a JWKS JSON document into a kid -> PEM/secret map suitable
// for Config.PublicKeys. RSA and EC public keys are returned as DER bytes; oct
// keys are returned as raw shared secrets for HS* verification.
func ParseJWKS(data []byte) (map[string][]byte, error) {
	var set jwkSet
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for _, k := range set.Keys {
		kid := strings.TrimSpace(k.Kid)
		if kid == "" {
			continue
		}
		switch strings.ToUpper(k.Kty) {
		case "RSA":
			nb, err := base64.RawURLEncoding.DecodeString(k.N)
			if err != nil {
				return nil, err
			}
			eb, err := base64.RawURLEncoding.DecodeString(k.E)
			if err != nil {
				return nil, err
			}
			e := 0
			for _, b := range eb {
				e = e<<8 + int(b)
			}
			pub := &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: e}
			der, err := x509.MarshalPKIXPublicKey(pub)
			if err != nil {
				return nil, err
			}
			out[kid] = der
		case "EC":
			xb, err := base64.RawURLEncoding.DecodeString(k.X)
			if err != nil {
				return nil, err
			}
			yb, err := base64.RawURLEncoding.DecodeString(k.Y)
			if err != nil {
				return nil, err
			}
			curve := curveByName(k.Crv)
			if curve == nil {
				return nil, fmt.Errorf("unsupported jwk curve %s", k.Crv)
			}
			pub := &ecdsa.PublicKey{Curve: curve, X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)}
			der, err := x509.MarshalPKIXPublicKey(pub)
			if err != nil {
				return nil, err
			}
			out[kid] = der
		case "OKP":
			xb, err := base64.RawURLEncoding.DecodeString(k.X)
			if err != nil {
				return nil, err
			}
			out[kid] = ed25519.PublicKey(xb)
		case "OCT":
			kb, err := base64.RawURLEncoding.DecodeString(k.K)
			if err != nil {
				return nil, err
			}
			out[kid] = kb
		}
	}
	return out, nil
}

func curveByName(name string) elliptic.Curve {
	switch name {
	case "P-256":
		return elliptic.P256()
	case "P-384":
		return elliptic.P384()
	case "P-521":
		return elliptic.P521()
	default:
		return nil
	}
}

// SignRS256 signs claims with an RSA private key in PKCS#1 or PKCS#8 PEM form.
func SignRS256(claims map[string]any, privateKeyPEM []byte, kid string) (string, error) {
	return signRSA(claims, privateKeyPEM, kid, "RS256", false)
}
func SignPS256(claims map[string]any, privateKeyPEM []byte, kid string) (string, error) {
	return signRSA(claims, privateKeyPEM, kid, "PS256", true)
}

func signRSA(claims map[string]any, privateKeyPEM []byte, kid, alg string, pss bool) (string, error) {
	priv, err := parseRSAPrivateKey(privateKeyPEM)
	if err != nil {
		return "", err
	}
	header := map[string]any{"alg": alg, "typ": "JWT"}
	if kid != "" {
		header["kid"] = kid
	}
	input, err := signingInput(header, claims)
	if err != nil {
		return "", err
	}
	hashID, digest := digestForAlg(alg, []byte(input))
	var sig []byte
	if pss {
		sig, err = rsa.SignPSS(rand.Reader, priv, hashID, digest, nil)
	} else {
		sig, err = rsa.SignPKCS1v15(rand.Reader, priv, hashID, digest)
	}
	if err != nil {
		return "", err
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func parseRSAPrivateKey(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block != nil {
		raw = block.Bytes
	}
	if k, err := x509.ParsePKCS1PrivateKey(raw); err == nil {
		return k, nil
	}
	key, err := x509.ParsePKCS8PrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid rsa private key: %w", err)
	}
	k, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("rsa private key required: PKCS#8 key is type %T", key)
	}
	return k, nil
}

func signingInput(header, claims map[string]any) (string, error) {
	if claims == nil {
		claims = map[string]any{}
	}
	hb, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb), nil
}

var _ hash.Hash
