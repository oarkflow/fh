package ipthrottle

import (
	"errors"
	"testing"
	"time"
)

func TestFileStoreUnderLimitAllowed(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	defer s.StopGC()

	now := time.Now()
	for i := 1; i <= 3; i++ {
		count, err := s.Increment("1.2.3.4", time.Minute, 0, now)
		if err != nil {
			t.Fatalf("Increment() error = %v", err)
		}
		if count != i {
			t.Fatalf("count = %d, want %d", count, i)
		}
	}
}

func TestFileStoreOverLimitRejected(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	defer s.StopGC()

	now := time.Now()
	// MaxPerIP enforcement lives in the middleware, not the Store, but the
	// MaxIPs cardinality cap does live in the Store: prove it fails closed
	// exactly like MemoryStore does.
	if _, err := s.Increment("1.2.3.4", time.Minute, 1, now); err != nil {
		t.Fatalf("first Increment() error = %v", err)
	}
	_, err := s.Increment("5.6.7.8", time.Minute, 1, now)
	if !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("second key: err = %v, want ErrCapacityExceeded", err)
	}
}

func TestFileStoreWindowResetAfterExpiry(t *testing.T) {
	s := NewFileStore(t.TempDir(), 0)
	defer s.StopGC()

	now := time.Now()
	count, err := s.Increment("1.2.3.4", time.Minute, 0, now)
	if err != nil {
		t.Fatalf("Increment() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}

	later := now.Add(2 * time.Minute)
	count, err = s.Increment("1.2.3.4", time.Minute, 0, later)
	if err != nil {
		t.Fatalf("Increment() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("count after window reset = %d, want 1", count)
	}
}

// TestFileStoreMatchesMemoryStoreBehavior runs the same scenario against
// both stores and asserts identical outcomes.
func TestFileStoreMatchesMemoryStoreBehavior(t *testing.T) {
	mem := NewMemoryStore()
	file := NewFileStore(t.TempDir(), 0)
	defer file.StopGC()

	now := time.Now()
	for i := 0; i < 5; i++ {
		memCount, memErr := mem.Increment("k", time.Minute, 0, now)
		fileCount, fileErr := file.Increment("k", time.Minute, 0, now)
		if memErr != nil || fileErr != nil {
			t.Fatalf("unexpected error: mem=%v file=%v", memErr, fileErr)
		}
		if memCount != fileCount {
			t.Fatalf("iteration %d: mem count=%d file count=%d", i, memCount, fileCount)
		}
	}
}
