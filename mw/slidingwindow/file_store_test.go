package slidingwindow

import (
	"testing"
	"time"
)

func TestFileStoreUnderLimitAllowed(t *testing.T) {
	s := NewFileStore(t.TempDir(), FileStoreConfig{Rate: 3, Burst: 0, Window: time.Hour})
	defer s.StopGC()

	for i := 0; i < 3; i++ {
		allowed, _, _ := s.Allow("k")
		if !allowed {
			t.Fatalf("request %d: allowed = false, want true", i)
		}
	}
}

func TestFileStoreOverLimitRejected(t *testing.T) {
	s := NewFileStore(t.TempDir(), FileStoreConfig{Rate: 3, Burst: 0, Window: time.Hour})
	defer s.StopGC()

	var allowed, denied int
	for i := 0; i < 10; i++ {
		ok, _, _ := s.Allow("k")
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
	s := NewFileStore(t.TempDir(), FileStoreConfig{Rate: 1, Burst: 0, Window: 30 * time.Millisecond})
	defer s.StopGC()

	ok, _, _ := s.Allow("k")
	if !ok {
		t.Fatal("first request should be allowed")
	}
	ok, _, _ = s.Allow("k")
	if ok {
		t.Fatal("second immediate request should be denied")
	}

	time.Sleep(50 * time.Millisecond)

	ok, _, _ = s.Allow("k")
	if !ok {
		t.Fatal("request after window expiry should be allowed")
	}
}

// TestFileStorePersistsAcrossInstances proves state written by one
// FileStore is visible to a second FileStore instance pointed at the same
// directory (simulating a process restart), once a flush has happened.
func TestFileStorePersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()

	s1 := NewFileStore(dir, FileStoreConfig{Rate: 1, Burst: 0, Window: time.Hour, FlushEvery: 1})
	ok, _, _ := s1.Allow("k")
	if !ok {
		t.Fatal("first request should be allowed")
	}
	if err := s1.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	s1.StopGC()

	s2 := NewFileStore(dir, FileStoreConfig{Rate: 1, Burst: 0, Window: time.Hour, FlushEvery: 1})
	defer s2.StopGC()
	ok, _, _ = s2.Allow("k")
	if ok {
		t.Fatal("second instance should see the persisted request and deny (rate=1 already used)")
	}
}

// TestFileStoreMatchesMemoryStoreBehavior runs the same scenario against
// both stores and asserts identical allow/deny decisions.
func TestFileStoreMatchesMemoryStoreBehavior(t *testing.T) {
	mem := NewLimiter(Config{Rate: 5, Burst: 2, Window: time.Hour, MaxKeys: 100, CleanupInterval: time.Hour})
	file := NewFileStore(t.TempDir(), FileStoreConfig{Rate: 5, Burst: 2, Window: time.Hour})
	defer file.StopGC()

	for i := 0; i < 12; i++ {
		memOK, _, _ := mem.Allow("k")
		fileOK, _, _ := file.Allow("k")
		if memOK != fileOK {
			t.Fatalf("iteration %d: mem allowed=%v file allowed=%v", i, memOK, fileOK)
		}
	}
}
