package delegation

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func writeRing(t *testing.T, dir string, active string, keys []KeyReference) string {
	t.Helper()
	path := filepath.Join(dir, "ring-"+active+".json")
	b, _ := json.Marshal(KeyringConfig{Active: active, Keys: keys})
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func keyPair(t *testing.T, dir, kid string) (KeyReference, KeyReference, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pk, _ := x509.MarshalPKCS8PrivateKey(priv)
	pb, _ := x509.MarshalPKIXPublicKey(pub)
	privPath := filepath.Join(dir, kid+".key")
	pubPath := filepath.Join(dir, kid+".pub")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pb}), 0644); err != nil {
		t.Fatal(err)
	}
	return KeyReference{ID: kid, File: privPath}, KeyReference{ID: kid, File: pubPath}, priv
}
func TestLiveKeyRotationAndFailureAtomicity(t *testing.T) {
	d := t.TempDir()
	privA, pubA, keyA := keyPair(t, d, "a-2026")
	privB, pubB, keyB := keyPair(t, d, "b-2026")
	sender := &RotatingSigner{}
	receiver := &Verifier{Audience: "backend", Clock: func() time.Time { return time.Unix(1800000000, 0) }}
	if err := sender.Reload(writeRing(t, d, privA.ID, []KeyReference{privA})); err != nil {
		t.Fatal(err)
	}
	if err := receiver.ReloadKeys(writeRing(t, d, "public-a", []KeyReference{pubA})); err != nil {
		t.Fatal(err)
	}
	a, err := IssueNamed(keyA, "a-2026", "backend", "alice", 90, 4, time.Unix(1800000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Verify(a, 90, 4); err != nil {
		t.Fatal(err)
	}
	if err := receiver.ReloadKeys(writeRing(t, d, "overlap", []KeyReference{pubA, pubB})); err != nil {
		t.Fatal(err)
	}
	// A valid but DIFFERENT trusted kid cannot be substituted into proof A:
	// the kid is included in the v2 signature transcript.
	var tampered Proof
	if err := json.Unmarshal(a, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.KeyID = pubB.ID
	changed, _ := json.Marshal(tampered)
	if _, err := receiver.Verify(changed, 90, 4); err == nil {
		t.Fatal("key-ID substitution accepted")
	}
	if err := sender.Reload(writeRing(t, d, privB.ID, []KeyReference{privB})); err != nil {
		t.Fatal(err)
	}
	b, err := IssueNamed(keyB, "b-2026", "backend", "alice", 90, 5, time.Unix(1800000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Verify(b, 90, 5); err != nil {
		t.Fatal(err)
	}
	if err := receiver.ReloadKeys(writeRing(t, d, "public-b", []KeyReference{pubB})); err != nil {
		t.Fatal(err)
	}
	proofA, err := IssueNamed(keyA, "a-2026", "backend", "alice", 91, 4, time.Unix(1800000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Verify(proofA, 91, 4); err == nil {
		t.Fatal("retired key accepted")
	}
	if _, err := receiver.Verify(b, 90, 5); err == nil {
		t.Fatal("replay accepted")
	}
	// A failed reload must retain its current valid keys.
	bad := writeRing(t, d, "bad", []KeyReference{{ID: "bad", File: filepath.Join(d, "missing.pub")}})
	if err := receiver.ReloadKeys(bad); err == nil {
		t.Fatal("bad trust accepted")
	}
	valid, _ := IssueNamed(keyB, "b-2026", "backend", "alice", 91, 6, time.Unix(1800000000, 0))
	if _, err := receiver.Verify(valid, 91, 6); err != nil {
		t.Fatalf("lost last good trust: %v", err)
	}
	if err := sender.Reload(bad); err == nil {
		t.Fatal("bad signer reload accepted")
	}
}

func TestConcurrentVerifierRotation(t *testing.T) {
	d := t.TempDir()
	_, pub, priv := keyPair(t, d, "current")
	v := &Verifier{Audience: "backend", Clock: func() time.Time { return time.Unix(1800000000, 0) }}
	path := writeRing(t, d, "pub", []KeyReference{pub})
	if err := v.ReloadKeys(path); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for n := 0; n < 5; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if n == 0 {
					if err := v.ReloadKeys(path); err != nil {
						t.Error(err)
					}
					continue
				}
				b, err := IssueNamed(priv, "current", "backend", "alice", uint64(100+n), uint64(i+1), time.Unix(1800000000, 0))
				if err != nil {
					t.Error(err)
					return
				}
				if _, err = v.Verify(b, uint64(100+n), uint64(i+1)); err != nil {
					t.Error(err)
					return
				}
			}
		}(n)
	}
	wg.Wait()
}
