package cryptokit

import (
	"bytes"
	"testing"
)

func TestDirectionSeparatedKeysAndAEAD(t *testing.T) {
	secret := bytes.Repeat([]byte{6}, 32)
	a, err := DeriveTrafficKeys(secret, []byte("v1 cid9 epoch0 client-to-server"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := DeriveTrafficKeys(secret, []byte("v1 cid9 epoch0 server-to-client"))
	if a == b {
		t.Fatal("direction collision")
	}
	enc, _ := NewProtector(a)
	plain, aad := []byte("hello"), []byte("immutable header")
	ciphertext := enc.Seal(nil, plain, aad, 4)
	decoded, err := enc.Open(nil, ciphertext, aad, 4)
	if err != nil || !bytes.Equal(plain, decoded) {
		t.Fatal("decrypt failed", err)
	}
	if _, err := enc.Open(nil, ciphertext, []byte("tampered"), 4); err == nil {
		t.Fatal("accepted invalid aad")
	}
	if _, err := enc.Open(nil, ciphertext, aad, 5); err == nil {
		t.Fatal("accepted wrong number")
	}
}
