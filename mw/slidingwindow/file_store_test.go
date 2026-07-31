package slidingwindow

import (
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestFileStoreUnderLimitAllowed(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	for i := 0; i < 3; i++ {
		allowed, _, _, err := Allow(s, "k", 3, 0, time.Hour)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d: allowed = false, want true", i)
		}
	}
}

func TestFileStoreOverLimitRejected(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	var allowed, denied int
	for i := 0; i < 10; i++ {
		ok, _, _, err := Allow(s, "k", 3, 0, time.Hour)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if ok {
			allowed++
		} else {
			denied++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed = %d, want 3", allowed)
	}
	if denied != 7 {
		t.Fatalf("denied = %d, want 7", denied)
	}
}

func TestFileStoreWindowResetAfterExpiry(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	ok, _, _, err := Allow(s, "k", 1, 0, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !ok {
		t.Fatal("first request should be allowed")
	}
	ok, _, _, err = Allow(s, "k", 1, 0, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Fatal("second immediate request should be denied")
	}

	time.Sleep(50 * time.Millisecond)

	ok, _, _, err = Allow(s, "k", 1, 0, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !ok {
		t.Fatal("request after window expiry should be allowed")
	}
}

// TestFileStorePersistsAcrossInstances proves state written by one
// FileStore is visible to a second FileStore instance pointed at the same
// directory (simulating a process restart), once a flush has happened.
func TestFileStorePersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()

	s1, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	ok, _, _, err := Allow(s1, "k", 1, 0, time.Hour)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !ok {
		t.Fatal("first request should be allowed")
	}
	_ = s1.Close()

	s2, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	ok, _, _, err = Allow(s2, "k", 1, 0, time.Hour)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if ok {
		t.Fatal("second instance should see the persisted request and deny (rate=1 already used)")
	}
}

// TestFileStoreMatchesMemoryStoreBehavior runs the same scenario against
// both stores and asserts identical allow/deny decisions.
func TestFileStoreMatchesMemoryStoreBehavior(t *testing.T) {
	mem := NewLimiter(Config{Rate: 5, Burst: 2, Window: time.Hour, MaxKeys: 100, CleanupInterval: time.Hour})
	file, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	for i := 0; i < 12; i++ {
		memOK, _, _ := mem.Allow("k")
		fileOK, _, _, err := Allow(file, "k", 5, 2, time.Hour)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if memOK != fileOK {
			t.Fatalf("iteration %d: mem allowed=%v file allowed=%v", i, memOK, fileOK)
		}
	}
}
