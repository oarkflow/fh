package delegation

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

func TestDelegationSignatureReplaySessionChannelAndExpiry(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1800000000, 0)
	v := &Verifier{Audience: "mira-upstream", PublicKey: pub, Clock: func() time.Time { return now }}
	proof, err := Issue(priv, "mira-upstream", "alice", 100, 3, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = v.Verify(proof, 100, 4); err == nil {
		t.Fatal("cross-channel proof")
	}
	if _, err = v.Verify(proof, 101, 3); err == nil {
		t.Fatal("cross-session proof")
	}
	subject, err := v.Verify(proof, 100, 3)
	if err != nil || subject != "alice" {
		t.Fatalf("valid proof: %v %q", err, subject)
	}
	if _, err = v.Verify(proof, 100, 3); err == nil {
		t.Fatal("replay accepted")
	}
	v2 := &Verifier{Audience: "different", PublicKey: pub, Clock: func() time.Time { return now }}
	if _, err = v2.Verify(proof, 100, 3); err == nil {
		t.Fatal("wrong audience accepted")
	}
	v.Clock = func() time.Time { return now.Add(31 * time.Second) }
	if _, err = v.Verify(proof, 100, 3); err == nil {
		t.Fatal("expired proof accepted")
	}
	if _, err := Issue(priv, "mira-upstream", "alice\nadmin", 100, 3, now); err == nil {
		t.Fatal("untrusted newline identity accepted")
	}
}
