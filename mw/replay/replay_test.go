package replay

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestMemoryStoreFailsClosedAtCapacity(t *testing.T) {
	store := kv.NewMemoryStore()
	var mu sync.Mutex
	if _, err := Seen(store, &mu, "first", time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := Seen(store, &mu, "second", time.Minute, 1); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected ErrStoreFull, got %v", err)
	}
	if seen, err := Seen(store, &mu, "first", time.Minute, 1); err != nil || !seen {
		t.Fatalf("valid replay marker was evicted: seen=%v err=%v", seen, err)
	}
}
