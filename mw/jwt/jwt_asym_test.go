package jwt

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"testing"
	"time"
)

func TestRS256SignVerifyAndRequiredClaims(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	tok, err := SignRS256(map[string]any{"sub": "u1", "exp": time.Now().Add(time.Hour).Unix()}, privPEM, "k1")
	if err != nil {
		t.Fatal(err)
	}
	_, claims, err := Verify(nil, tok, Config{PublicKeys: map[string][]byte{"k1": pubPEM}, Algorithms: []string{"RS256"}, RequiredClaims: []string{"sub"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if claims["sub"] != "u1" {
		t.Fatalf("unexpected sub: %#v", claims["sub"])
	}
	_, _, err = Verify(nil, tok, Config{PublicKeys: map[string][]byte{"k1": pubPEM}, Algorithms: []string{"RS256"}, RequiredClaims: []string{"tenant_id"}}, nil)
	if err == nil {
		t.Fatal("expected missing claim error")
	}
}

// TestAlgorithmConfusionRejected proves an attacker cannot forge an HS256
// token by HMAC-signing with the RSA public key bytes, even when a config
// (e.g. mid-migration from HS to RS) allows both HS256 and RS256.
func TestAlgorithmConfusionRejected(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	// Forge a token: alg=HS256, signed with the RSA public key bytes as the
	// HMAC secret (the public key is not secret; an attacker has it).
	header := map[string]any{"alg": "HS256", "typ": "JWT", "kid": "k1"}
	claims := map[string]any{"sub": "attacker", "roles": []string{"admin"}}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	signingInput := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	mac := hmac.New(sha256.New, pubPEM)
	mac.Write([]byte(signingInput))
	forged := signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	cfg := Config{
		PublicKeys: map[string][]byte{"k1": pubPEM},
		Algorithms: []string{"HS256", "RS256"}, // realistic HS->RS migration config
	}
	if _, _, err := Verify(nil, forged, cfg, nil); err == nil {
		t.Fatal("algorithm-confusion forged token was accepted, expected rejection")
	}
}
