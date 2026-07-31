package webhook

import (
	"os"
	"testing"
	"time"
)

func TestWebhookFileStoreSeenBasic(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	if s.Seen("sig-a:1000", time.Minute) {
		t.Fatal("expected first sighting to report not seen")
	}
	if !s.Seen("sig-a:1000", time.Minute) {
		t.Fatal("expected second sighting to report seen")
	}
}

func TestWebhookFileStoreExpiry(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	if s.Seen("sig-expiring", 20*time.Millisecond) {
		t.Fatal("expected first sighting to report not seen")
	}
	time.Sleep(50 * time.Millisecond)
	if s.Seen("sig-expiring", time.Minute) {
		t.Fatal("expected key to be treated as unseen after expiry")
	}
}

func TestWebhookFileStoreDifferentKeysDoNotCollide(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	if s.Seen("sig-one", time.Minute) {
		t.Fatal("expected sig-one to be unseen on first sight")
	}
	if s.Seen("sig-two", time.Minute) {
		t.Fatal("expected sig-two to be unseen despite sig-one being recorded")
	}
	if !s.Seen("sig-one", time.Minute) {
		t.Fatal("expected sig-one to still be recorded as seen")
	}
}

func TestWebhookFileStoreFailsSafeOnInitError(t *testing.T) {
	// Point the store at a path that cannot be a directory (a regular
	// file), forcing MkdirAll to fail so initErr is set. Seen must then
	// fail closed (report seen=true) rather than silently allowing replay.
	dir := t.TempDir()
	blocked := dir + "/blocked"
	if err := os.WriteFile(blocked, []byte("not a directory"), 0600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s := NewFileStore(blocked+"/store", 0)
	if !s.Seen("any-key", time.Minute) {
		t.Fatal("expected fail-safe seen=true when store failed to initialize")
	}
}

func TestWebhookFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	s.Seen("gc-key", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	s.GC()
	if s.Seen("gc-key", time.Minute) {
		t.Fatal("expected expired key removed by GC to report unseen")
	}
}

func TestWebhookFileStoreStopGC(t *testing.T) {
	s := NewFileStore(t.TempDir(), 5*time.Millisecond)
	s.StopGC()
	s.StopGC()
}
