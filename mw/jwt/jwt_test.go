package jwt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestJWTVerifyHS256(t *testing.T) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"u1","roles":["admin"],"exp":4102444800}`))
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte(header + "." + claims))
	tok := header + "." + claims + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	_, cl, err := Verify(nil, tok, Config{Secret: []byte("secret"), Algorithms: []string{"HS256"}}, map[string]bool{"HS256": true})
	if err != nil {
		t.Fatal(err)
	}
	if cl["sub"] != "u1" {
		t.Fatalf("bad sub: %#v", cl["sub"])
	}
}

func TestJWTSignAndVerify(t *testing.T) {
	tok, err := Sign(map[string]any{"sub": "u1", "tenant_id": "t1", "roles": []string{"admin"}, "exp": 4102444800}, []byte("secret"), "HS256")
	if err != nil {
		t.Fatal(err)
	}
	_, cl, err := Verify(nil, tok, Config{Secret: []byte("secret")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cl["tenant_id"] != "t1" {
		t.Fatalf("bad tenant: %#v", cl["tenant_id"])
	}
}
