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
	if Seen(store, &mu, "first", time.Minute, 1) {
		t.Fatal("first signature was treated as replay")
	}
	if !Seen(store, &mu, "second", time.Minute, 1) {
		t.Fatal("store capacity exhaustion did not fail closed")
	}
	if !Seen(store, &mu, "first", time.Minute, 1) {
		t.Fatal("live replay marker was evicted")
	}
}
