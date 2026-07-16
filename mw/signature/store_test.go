package signature

import (
	"testing"
	"time"
)

func TestReplayStoreFailsClosedAtCapacity(t *testing.T) {
	store := newMemoryReplayStore(1)
	if store.Seen("first", time.Minute) {
		t.Fatal("first signature was treated as replay")
	}
	if !store.Seen("second", time.Minute) {
		t.Fatal("store capacity exhaustion did not fail closed")
	}
	if !store.Seen("first", time.Minute) {
		t.Fatal("live replay marker was evicted")
	}
}
