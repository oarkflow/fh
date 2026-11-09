package gateway

// Fail-closed JWKS rotation for deployments with a pinned OIDC issuer and
// signing-key endpoint. Keys are never taken from JWT headers or redirects.

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrJWKS = errors.New("gateway: JWKS refresh or policy failure")

const maxJWKSBytes = 128 << 10

type JWKSOptions struct {
	Issuer          string
	Audience        string
	URL             string
	RefreshInterval time.Duration
	MaxStaleness    time.Duration
	ClockSkew       time.Duration
	HTTPClient      *http.Client
	Clock           func() time.Time
}

type jwksSnapshot struct {
	verifier *JWTVerifier
	loaded   time.Time
}

type RotatingJWKS struct {
	options    JWKSOptions
	httpClient *http.Client
	snapshot   atomic.Pointer[jwksSnapshot]
	refreshMu  sync.Mutex
}

func validPinnedEndpoint(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && u.Opaque == "" && u.Port() != "0" && !strings.Contains(u.Host, "@")
}

func secureHTTPClient(c *http.Client) *http.Client {
	if c == nil {
		c = &http.Client{Timeout: 5 * time.Second}
	}
	clone := *c
	if clone.Timeout == 0 || clone.Timeout > 15*time.Second {
		clone.Timeout = 5 * time.Second
	}
	// Never follow a redirect, even to another HTTPS origin.
	clone.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &clone
}

func NewRotatingJWKS(opts JWKSOptions) (*RotatingJWKS, error) {
	if opts.Issuer == "" || opts.Audience == "" || !validPinnedEndpoint(opts.URL) {
		return nil, ErrJWKS
	}
	if opts.RefreshInterval == 0 {
		opts.RefreshInterval = 5 * time.Minute
	}
	if opts.MaxStaleness == 0 {
		opts.MaxStaleness = 30 * time.Minute
	}
	if opts.RefreshInterval < time.Second || opts.RefreshInterval > time.Hour || opts.MaxStaleness < opts.RefreshInterval || opts.MaxStaleness > 24*time.Hour || opts.ClockSkew < 0 || opts.ClockSkew > time.Minute {
		return nil, ErrJWKS
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &RotatingJWKS{options: opts, httpClient: secureHTTPClient(opts.HTTPClient)}, nil
}

func parseJWKS(body []byte) (map[string]JWTKey, error) {
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Alg string `json:"alg"`
			Use string `json:"use"`
			Crv string `json:"crv"`
			X   string `json:"x"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &set); err != nil || len(set.Keys) == 0 || len(set.Keys) > 32 {
		return nil, ErrJWKS
	}
	keys := make(map[string]JWTKey, len(set.Keys))
	for _, k := range set.Keys {
		if len(k.Kid) == 0 || len(k.Kid) > 128 || (k.Use != "" && k.Use != "sig") {
			return nil, ErrJWKS
		}
		if _, dup := keys[k.Kid]; dup {
			return nil, ErrJWKS
		}
		var pub crypto.PublicKey
		switch {
		case k.Kty == "OKP" && k.Alg == "EdDSA" && k.Crv == "Ed25519":
			raw, err := base64.RawURLEncoding.DecodeString(k.X)
			if err != nil || len(raw) != ed25519.PublicKeySize {
				return nil, ErrJWKS
			}
			pub = ed25519.PublicKey(raw)
		case k.Kty == "RSA" && k.Alg == "RS256":
			modulus, err := base64.RawURLEncoding.DecodeString(k.N)
			if err != nil || len(modulus) == 0 || len(modulus) > 1024 {
				return nil, ErrJWKS
			}
			exponent, err := base64.RawURLEncoding.DecodeString(k.E)
			if err != nil || len(exponent) == 0 || len(exponent) > 4 {
				return nil, ErrJWKS
			}
			e := new(big.Int).SetBytes(exponent)
			if !e.IsInt64() || e.Int64() < 3 || e.Int64() > 1<<31-1 || e.Int64()%2 == 0 {
				return nil, ErrJWKS
			}
			n := new(big.Int).SetBytes(modulus)
			if n.BitLen() < 2048 || n.BitLen() > 8192 || n.Bit(0) == 0 {
				return nil, ErrJWKS
			}
			pub = &rsa.PublicKey{N: n, E: int(e.Int64())}
		default:
			return nil, fmt.Errorf("%w: unsupported signing key", ErrJWKS)
		}
		keys[k.Kid] = JWTKey{KeyID: k.Kid, Algorithm: k.Alg, PublicKey: pub}
	}
	return keys, nil
}

// Refresh atomically replaces the complete trusted keyset only after full
// validation. Any fetch or parse failure leaves the previous snapshot intact;
// Authenticate nevertheless rejects it after MaxStaleness.
func (r *RotatingJWKS) Refresh(ctx context.Context) error {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.options.URL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := r.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", ErrJWKS, response.StatusCode)
	}
	if response.ContentLength > maxJWKSBytes {
		return ErrJWKS
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxJWKSBytes+1))
	if err != nil || len(body) > maxJWKSBytes {
		return ErrJWKS
	}
	keys, err := parseJWKS(body)
	if err != nil {
		return err
	}
	verifier := &JWTVerifier{Issuer: r.options.Issuer, Audience: r.options.Audience, Keys: keys, Clock: r.options.Clock, ClockSkew: r.options.ClockSkew}
	if err := verifier.Validate(); err != nil {
		return err
	}
	r.snapshot.Store(&jwksSnapshot{verifier: verifier, loaded: r.options.Clock()})
	return nil
}

// Authenticate never uses stale or missing keys, including during IdP outages.
func (r *RotatingJWKS) Authenticate(req *http.Request) (string, error) {
	snap := r.snapshot.Load()
	if snap == nil || r.options.Clock().Before(snap.loaded) || r.options.Clock().Sub(snap.loaded) > r.options.MaxStaleness {
		return "", ErrJWKS
	}
	return snap.verifier.Authenticate(req)
}

// Run refreshes keys until cancellation. Call Refresh once successfully before
// exposing the gateway and then start Run in a goroutine.
func (r *RotatingJWKS) Run(ctx context.Context) {
	tick := time.NewTicker(r.options.RefreshInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_ = r.Refresh(ctx)
		}
	}
}

// DiscoverOIDCJWKS verifies the pinned issuer and refuses redirects or cross-
// origin JWKS references. For third-party key CDNs configure an independently
// pinned JWKS URL and restrict its outbound network path at deployment.
func DiscoverOIDCJWKS(ctx context.Context, issuer string, client *http.Client) (string, error) {
	if !validPinnedEndpoint(issuer) {
		return "", ErrJWKS
	}
	base, err := url.Parse(issuer)
	if err != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", ErrJWKS
	}
	endpoint := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := secureHTTPClient(client).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength > 16384 {
		return "", ErrJWKS
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16385))
	if err != nil || len(b) > 16384 {
		return "", ErrJWKS
	}
	var doc struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if json.Unmarshal(b, &doc) != nil || doc.Issuer != issuer || !validPinnedEndpoint(doc.JWKSURI) {
		return "", ErrJWKS
	}
	target, err := url.Parse(doc.JWKSURI)
	if err != nil || !strings.EqualFold(target.Host, base.Host) {
		return "", ErrJWKS
	}
	return doc.JWKSURI, nil
}
