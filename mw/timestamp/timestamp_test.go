package timestamp

import (
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreDoesNotEvictLiveReplayMarkers(t *testing.T) {
	store := NewMemoryStore(1)
	if _, err := store.Seen("first", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Seen("second", time.Minute); !errors.Is(err, ErrReplayStoreFull) {
		t.Fatalf("expected ErrReplayStoreFull, got %v", err)
	}
	if seen, err := store.Seen("first", time.Minute); err != nil || !seen {
		t.Fatalf("valid replay marker was evicted: seen=%v err=%v", seen, err)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	_, shutdown := New(Config{})
	shutdown()
	shutdown()
}
