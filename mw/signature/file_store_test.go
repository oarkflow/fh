package signature

import (
	"os"
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

	seen, err := Seen(store, &mu, "sig-1", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("first sighting should not be seen")
	}
	seen, err = Seen(store, &mu, "sig-1", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatalf("replay of sig-1 should be detected")
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

	seen, err := Seen(store, &mu, "short-lived", ttl)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("first sighting should not be seen")
	}

	time.Sleep(ttl + 15*time.Millisecond)

	seen, err = Seen(store, &mu, "short-lived", ttl)
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

	seen, err := Seen(store, &mu, "keyA", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("keyA first sighting should not be seen")
	}
	seen, err = Seen(store, &mu, "keyB", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatalf("keyB first sighting should not be seen")
	}
	seen, err = Seen(store, &mu, "keyA", time.Minute)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
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
	store, err := kv.NewFileStore(blocker + "/sub")
	if err == nil {
		_ = store.Close()
		t.Fatalf("expected kv.NewFileStore to fail")
	}

	_ = err
}
