package webhook

import (
	"os"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestWebhookFileStoreSeenBasic(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if Seen(s, nil, "sig-a:1000", time.Minute) {
		t.Fatal("expected first sighting to report not seen")
	}
	if !Seen(s, nil, "sig-a:1000", time.Minute) {
		t.Fatal("expected second sighting to report seen")
	}
}

func TestWebhookFileStoreExpiry(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if Seen(s, nil, "sig-expiring", 20*time.Millisecond) {
		t.Fatal("expected first sighting to report not seen")
	}
	time.Sleep(50 * time.Millisecond)
	if Seen(s, nil, "sig-expiring", time.Minute) {
		t.Fatal("expected key to be treated as unseen after expiry")
	}
}

func TestWebhookFileStoreDifferentKeysDoNotCollide(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if Seen(s, nil, "sig-one", time.Minute) {
		t.Fatal("expected sig-one to be unseen on first sight")
	}
	if Seen(s, nil, "sig-two", time.Minute) {
		t.Fatal("expected sig-two to be unseen despite sig-one being recorded")
	}
	if !Seen(s, nil, "sig-one", time.Minute) {
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
	s, err := kv.NewFileStore(blocked + "/store")
	if err == nil {
		_ = s.Close()
		t.Fatal("expected kv.NewFileStore to fail")
	}
}

func TestWebhookFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	Seen(s, nil, "gc-key", 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	s.GC()
	if Seen(s, nil, "gc-key", time.Minute) {
		t.Fatal("expected expired key removed by GC to report unseen")
	}
}
