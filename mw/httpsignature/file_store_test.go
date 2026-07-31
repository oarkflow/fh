package httpsignature

import (
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestHTTPSigFileStoreCheckAndStoreBasic(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex
	accepted, err := CheckAndStore(s, &mu, "nonce-a", time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected first use to be accepted")
	}
	accepted, err = CheckAndStore(s, &mu, "nonce-a", time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accepted {
		t.Fatal("expected replayed nonce to be rejected")
	}
}

func TestHTTPSigFileStoreExpiry(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex
	accepted, err := CheckAndStore(s, &mu, "nonce-expiring", time.Now().Add(20*time.Millisecond), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected first use to be accepted")
	}
	time.Sleep(50 * time.Millisecond)
	accepted, err = CheckAndStore(s, &mu, "nonce-expiring", time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected nonce to be accepted again after expiry")
	}
}

func TestHTTPSigFileStoreDifferentKeysDoNotCollide(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex
	accepted, err := CheckAndStore(s, &mu, "nonce-one", time.Now().Add(time.Minute), 100)
	if err != nil || !accepted {
		t.Fatalf("nonce-one: accepted=%v err=%v", accepted, err)
	}
	accepted, err = CheckAndStore(s, &mu, "nonce-two", time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected nonce-two to be accepted despite nonce-one being recorded")
	}
	accepted, err = CheckAndStore(s, &mu, "nonce-one", time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accepted {
		t.Fatal("expected nonce-one to still be rejected as a replay")
	}
}

func TestHTTPSigFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex
	if _, err := CheckAndStore(s, &mu, "gc-nonce", time.Now().Add(10*time.Millisecond), 100); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	s.GC()
	accepted, err := CheckAndStore(s, &mu, "gc-nonce", time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !accepted {
		t.Fatal("expected expired nonce removed by GC to be accepted again")
	}
}
