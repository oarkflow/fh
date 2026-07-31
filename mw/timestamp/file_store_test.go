package timestamp

import (
	"testing"
	"time"
)

func TestFileStoreSeenBasic(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	seen, err := s.Seen("key-a", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected first sighting to report not seen")
	}
	seen, err = s.Seen("key-a", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !seen {
		t.Fatal("expected second sighting to report seen")
	}
}

func TestFileStoreExpiry(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	seen, err := s.Seen("key-expiring", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected first sighting to report not seen")
	}
	time.Sleep(50 * time.Millisecond)
	seen, err = s.Seen("key-expiring", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected key to be treated as unseen after expiry")
	}
}

func TestFileStoreDifferentKeysDoNotCollide(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	if seen, err := s.Seen("key-one", time.Minute); err != nil || seen {
		t.Fatalf("key-one: seen=%v err=%v", seen, err)
	}
	seen, err := s.Seen("key-two", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected key-two to be unseen despite key-one being recorded")
	}
	// key-one should still be recognized as seen.
	seen, err = s.Seen("key-one", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !seen {
		t.Fatal("expected key-one to still be recorded as seen")
	}
}

func TestFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	if _, err := s.Seen("gc-key", 10*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	s.GC()
	seen, err := s.Seen("gc-key", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatal("expected expired key removed by GC to report unseen")
	}
}

func TestFileStoreStopGC(t *testing.T) {
	s := NewFileStore(t.TempDir(), 5*time.Millisecond)
	s.StopGC()
	// Calling StopGC twice must not panic.
	s.StopGC()
}
