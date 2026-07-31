package timestamp

import (
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestFileStoreSeenBasic(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	seen, err := Seen(s, "key-a", time.Minute, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected first sighting to report not seen")
	}
	seen, err = Seen(s, "key-a", time.Minute, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !seen {
		t.Fatal("expected second sighting to report seen")
	}
}

func TestFileStoreExpiry(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	seen, err := Seen(s, "key-expiring", 20*time.Millisecond, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected first sighting to report not seen")
	}
	time.Sleep(50 * time.Millisecond)
	seen, err = Seen(s, "key-expiring", time.Minute, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected key to be treated as unseen after expiry")
	}
}

func TestFileStoreDifferentKeysDoNotCollide(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if seen, err := Seen(s, "key-one", time.Minute, 100); err != nil || seen {
		t.Fatalf("key-one: seen=%v err=%v", seen, err)
	}
	seen, err := Seen(s, "key-two", time.Minute, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected key-two to be unseen despite key-one being recorded")
	}
	// key-one should still be recognized as seen.
	seen, err = Seen(s, "key-one", time.Minute, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !seen {
		t.Fatal("expected key-one to still be recorded as seen")
	}
}

func TestFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if _, err := Seen(s, "gc-key", 10*time.Millisecond, 100); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	_, _ = s.Len()
	seen, err := Seen(s, "gc-key", time.Minute, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected expired key removed by GC to report unseen")
	}
}
