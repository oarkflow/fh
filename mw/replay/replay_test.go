package replay

import (
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreFailsClosedAtCapacity(t *testing.T) {
	store := NewMemoryStore(1)
	if _, err := store.Seen("first", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Seen("second", time.Minute); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected ErrStoreFull, got %v", err)
	}
	if seen, err := store.Seen("first", time.Minute); err != nil || !seen {
		t.Fatalf("valid replay marker was evicted: seen=%v err=%v", seen, err)
	}
}
