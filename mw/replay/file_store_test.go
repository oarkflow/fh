package replay

import (
	"testing"
	"time"
)

func TestFileStoreSeenBasic(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	seen, err := store.Seen("nonce-1", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("first sighting should not be seen")
	}

	seen, err = store.Seen("nonce-1", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatalf("replay of nonce-1 should be detected")
	}
}

func TestFileStoreTTLExpiry(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	ttl := 20 * time.Millisecond

	seen, err := store.Seen("short-lived", ttl)
	if err != nil || seen {
		t.Fatalf("first sighting: seen=%v err=%v", seen, err)
	}

	time.Sleep(ttl + 15*time.Millisecond)

	seen, err = store.Seen("short-lived", ttl)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("expired key should not be reported as seen")
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	seenA, err := store.Seen("keyA", time.Minute)
	if err != nil || seenA {
		t.Fatalf("keyA first sighting: seen=%v err=%v", seenA, err)
	}
	seenB, err := store.Seen("keyB", time.Minute)
	if err != nil || seenB {
		t.Fatalf("keyB first sighting: seen=%v err=%v", seenB, err)
	}

	seenA2, err := store.Seen("keyA", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seenA2 {
		t.Fatalf("keyA should now be reported as seen")
	}
}
