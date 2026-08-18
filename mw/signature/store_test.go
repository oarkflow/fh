package signature

import (
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestReplayStoreFailsClosedAtCapacity(t *testing.T) {
	store := kv.NewMemoryStore()
	var mu sync.Mutex
	seen, err := Seen(store, &mu, "first", time.Minute, 1)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Fatal("first signature was treated as replay")
	}
	seen, err = Seen(store, &mu, "second", time.Minute, 1)
	if err == nil {
		t.Fatal("store capacity exhaustion should return error")
	}
	_ = seen
	seen, err = Seen(store, &mu, "first", time.Minute, 1)
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Fatal("live replay marker was evicted")
	}
}
