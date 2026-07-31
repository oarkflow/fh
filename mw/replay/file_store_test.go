package replay

import (
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestFileStoreSeenBasic(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex

	seen, err := Seen(store, &mu, "nonce-1", time.Minute, 100)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("first sighting should not be seen")
	}

	seen, err = Seen(store, &mu, "nonce-1", time.Minute, 100)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatalf("replay of nonce-1 should be detected")
	}
}

func TestFileStoreTTLExpiry(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex

	ttl := 20 * time.Millisecond

	seen, err := Seen(store, &mu, "short-lived", ttl, 100)
	if err != nil || seen {
		t.Fatalf("first sighting: seen=%v err=%v", seen, err)
	}

	time.Sleep(ttl + 15*time.Millisecond)

	seen, err = Seen(store, &mu, "short-lived", ttl, 100)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("expired key should not be reported as seen")
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var mu sync.Mutex

	seenA, err := Seen(store, &mu, "keyA", time.Minute, 100)
	if err != nil || seenA {
		t.Fatalf("keyA first sighting: seen=%v err=%v", seenA, err)
	}
	seenB, err := Seen(store, &mu, "keyB", time.Minute, 100)
	if err != nil || seenB {
		t.Fatalf("keyB first sighting: seen=%v err=%v", seenB, err)
	}

	seenA2, err := Seen(store, &mu, "keyA", time.Minute, 100)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seenA2 {
		t.Fatalf("keyA should now be reported as seen")
	}
}
