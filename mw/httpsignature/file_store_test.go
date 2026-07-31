package httpsignature

import (
	"testing"
	"time"
)

func TestHTTPSigFileStoreCheckAndStoreBasic(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	accepted, err := s.CheckAndStore("nonce-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected first use to be accepted")
	}
	accepted, err = s.CheckAndStore("nonce-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accepted {
		t.Fatal("expected replayed nonce to be rejected")
	}
}

func TestHTTPSigFileStoreExpiry(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	accepted, err := s.CheckAndStore("nonce-expiring", time.Now().Add(20*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected first use to be accepted")
	}
	time.Sleep(50 * time.Millisecond)
	accepted, err = s.CheckAndStore("nonce-expiring", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected nonce to be accepted again after expiry")
	}
}

func TestHTTPSigFileStoreDifferentKeysDoNotCollide(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	accepted, err := s.CheckAndStore("nonce-one", time.Now().Add(time.Minute))
	if err != nil || !accepted {
		t.Fatalf("nonce-one: accepted=%v err=%v", accepted, err)
	}
	accepted, err = s.CheckAndStore("nonce-two", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected nonce-two to be accepted despite nonce-one being recorded")
	}
	accepted, err = s.CheckAndStore("nonce-one", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accepted {
		t.Fatal("expected nonce-one to still be rejected as a replay")
	}
}

func TestHTTPSigFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	if _, err := s.CheckAndStore("gc-nonce", time.Now().Add(10*time.Millisecond)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	s.GC()
	accepted, err := s.CheckAndStore("gc-nonce", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected expired nonce removed by GC to be accepted again")
	}
}

func TestHTTPSigFileStoreStopGC(t *testing.T) {
	s := NewFileStore(t.TempDir(), 5*time.Millisecond)
	s.StopGC()
	s.StopGC()
}
