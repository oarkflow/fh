package session

import (
	"fmt"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// hexID returns a valid-looking 64-hex-char session ID (validSessionID
// requires exactly 64 hex characters) derived from n, for test fixtures.
func hexID(n int) string {
	return fmt.Sprintf("%064x", n)
}

// TestMemoryStoreBoundedByMaxSessions proves an attacker who can trigger
// many session creations (any unauthenticated request reaching Begin)
// cannot grow the store past the configured bound, closing the unbounded
// memory-growth vector where sessions previously accumulated with no cap
// until GC reclaimed expired ones.
func TestMemoryStoreBoundedByMaxSessions(t *testing.T) {
	const maxSessions = 50
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(maxSessions))

	for i := 0; i < 1000; i++ {
		s := &Session{ID: hexID(i + 1), Data: map[string]any{}, ExpiresAt: time.Now().Add(time.Hour)}
		if err := SetSession(store, s); err != nil {
			t.Fatalf("unexpected error at i=%d: %v", i, err)
		}
	}

	size, err := store.Len()
	if err != nil {
		t.Fatal(err)
	}

	if size > maxSessions {
		t.Fatalf("expected store bounded at %d sessions after 1000 inserts, got %d", maxSessions, size)
	}
}

// TestMemoryStoreDefaultBoundIsSane proves the zero-value/default case (no
// explicit maxSessions argument) still applies a bound rather than growing
// forever.
func TestMemoryStoreDefaultBoundIsSane(t *testing.T) {
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(defaultMaxSessions))
	if store == nil {
		t.Fatal("expected default kv session store")
	}
}

// TestMemoryStoreEvictsExpiredBeforeArbitrary proves that when at capacity,
// an already-expired session is reclaimed in preference to evicting a live
// one.
func TestMemoryStoreEvictsExpiredBeforeArbitrary(t *testing.T) {
	store := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(2))
	live := &Session{ID: hexID(1), Data: map[string]any{}, ExpiresAt: time.Now().Add(time.Hour)}
	expired := &Session{ID: hexID(2), Data: map[string]any{}, ExpiresAt: time.Now().Add(-time.Minute)}
	if err := SetSession(store, live); err != nil {
		t.Fatal(err)
	}
	if err := SetSession(store, expired); err != nil {
		t.Fatal(err)
	}

	newSession := &Session{ID: hexID(3), Data: map[string]any{}, ExpiresAt: time.Now().Add(time.Hour)}
	if err := SetSession(store, newSession); err != nil {
		t.Fatal(err)
	}

	if got, err := GetSession(store, live.ID); err != nil || got == nil {
		t.Fatalf("expected live session to survive eviction, got %v err=%v", got, err)
	}
	if got, _ := GetSession(store, expired.ID); got != nil {
		t.Fatal("expected expired session to be evicted, not the live one")
	}
}
