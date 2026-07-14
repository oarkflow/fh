package fh

import "testing"

func TestSignedCookieRoundTrip(t *testing.T) {
	secret := []byte("s3cr3t-signing-key")
	c := &Cookie{Name: "role", Value: "user"}
	if err := c.Sign(secret); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.Verify(secret) {
		t.Fatal("expected freshly signed cookie to verify")
	}
}

func TestSignedCookieRejectsTamperedValue(t *testing.T) {
	secret := []byte("s3cr3t-signing-key")
	c := &Cookie{Name: "role", Value: "user"}
	if err := c.Sign(secret); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Attacker swaps the value but keeps the original signature.
	i := lastDot(c.Value)
	tampered := &Cookie{Name: "role", Value: "admin" + c.Value[i:]}
	if tampered.Verify(secret) {
		t.Fatal("expected tampered value to fail verification")
	}
}

func TestSignedCookieRejectsWrongSecret(t *testing.T) {
	c := &Cookie{Name: "role", Value: "user"}
	if err := c.Sign([]byte("short-secret-aaa")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Verify([]byte("short-secret-bbb")) {
		t.Fatal("expected verification with a different secret to fail")
	}
}

// TestSignedCookieRejectsCrossNameForgery proves a signature issued for one
// cookie name cannot be replayed under a different cookie name, even when
// the value is identical. Before binding Name into the MAC, an attacker
// who legitimately received a signed cookie with some value under one name
// (e.g. a low-privilege "session_flag=verified" cookie) could copy that
// exact "value.signature" string into a differently named,
// security-sensitive cookie (e.g. "role=verified") and have it verify as
// authentic there, since the old signature only covered the value.
func TestSignedCookieRejectsCrossNameForgery(t *testing.T) {
	secret := []byte("s3cr3t-signing-key")

	legit := &Cookie{Name: "session_flag", Value: "verified"}
	if err := legit.Sign(secret); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	forged := &Cookie{Name: "role", Value: legit.Value}
	if forged.Verify(secret) {
		t.Fatal("signed value from one cookie name was accepted under a different cookie name")
	}
}

func lastDot(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}
