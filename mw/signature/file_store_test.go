package signature

import (
	"os"
	"testing"
	"time"
)

func TestFileStoreSeenBasic(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	if store.Seen("sig-1", time.Minute) {
		t.Fatalf("first sighting should not be seen")
	}
	if !store.Seen("sig-1", time.Minute) {
		t.Fatalf("replay of sig-1 should be detected")
	}
}

func TestFileStoreTTLExpiry(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	ttl := 20 * time.Millisecond

	if store.Seen("short-lived", ttl) {
		t.Fatalf("first sighting should not be seen")
	}

	time.Sleep(ttl + 15*time.Millisecond)

	if store.Seen("short-lived", ttl) {
		t.Fatalf("expired key should not be reported as seen")
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	if store.Seen("keyA", time.Minute) {
		t.Fatalf("keyA first sighting should not be seen")
	}
	if store.Seen("keyB", time.Minute) {
		t.Fatalf("keyB first sighting should not be seen")
	}
	if !store.Seen("keyA", time.Minute) {
		t.Fatalf("keyA should now be reported as seen")
	}
}

func TestFileStoreFailsSafeOnBadDir(t *testing.T) {
	// Point the store at a path that cannot be created as a directory
	// (its parent is a regular file), so NewFileStore records initErr and
	// Seen must fail safe by treating the key as already seen.
	dir := t.TempDir()
	blocker := dir + "/blocker"
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	store := NewFileStore(blocker+"/sub", 0)
	defer store.StopGC()

	if !store.Seen("anything", time.Minute) {
		t.Fatalf("expected fail-safe seen=true when store dir is unusable")
	}
}
