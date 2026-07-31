package timestamp

import (
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestMemoryStoreDoesNotEvictLiveReplayMarkers(t *testing.T) {
	store := kv.NewMemoryStore()
	if _, err := Seen(store, "first", time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := Seen(store, "second", time.Minute, 1); !errors.Is(err, ErrReplayStoreFull) {
		t.Fatalf("expected ErrReplayStoreFull, got %v", err)
	}
	if seen, err := Seen(store, "first", time.Minute, 1); err != nil || !seen {
		t.Fatalf("valid replay marker was evicted: seen=%v err=%v", seen, err)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	_, shutdown := New(Config{})
	shutdown()
	shutdown()
}
