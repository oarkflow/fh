package cache

import (
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func newTestFileStore(t *testing.T, maxEntries int, opts ...kv.FileOption) kv.Store {
	t.Helper()
	_ = maxEntries
	opts = append(opts, kv.WithMaxEntrySize(maxStoredEntrySize))
	store, err := kv.NewFileStore(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	return store
}

func TestFileStoreGetSetDelete(t *testing.T) {
	fs := newTestFileStore(t, 0)

	if _, ok := Get(fs, "GET /foo"); ok {
		t.Fatalf("expected miss for unknown key")
	}

	now := time.Now()
	e := Entry{Status: 200, ContentType: "text/plain", Body: []byte("hello"), Created: now, Expires: now.Add(time.Minute)}
	Set(fs, "GET /foo", e)

	got, ok := Get(fs, "GET /foo")
	if !ok {
		t.Fatalf("expected hit after Set")
	}
	if got.Status != 200 || string(got.Body) != "hello" || got.ContentType != "text/plain" {
		t.Fatalf("unexpected entry after Set: %+v", got)
	}

	Delete(fs, "GET /foo")
	if _, ok := Get(fs, "GET /foo"); ok {
		t.Fatalf("expected miss after Delete")
	}
}

// TestFileStoreExpiry was previously flaky under CPU contention (e.g. a full
// `go test ./... -race` run): it used a single fixed time.Sleep(40ms) after a
// 20ms TTL and asserted on the outcome of exactly one Get afterward, so any
// scheduling delay of the test goroutine itself (plausible under a big
// parallel -race run, not just delay inside the store) could make the sleep
// elapse before or after the TTL relative to when the *assertion* actually
// ran, flipping the result either way. Polling for the eventual state within
// a generous deadline tests the real invariant (self-eviction happens, and
// happens only after the TTL, not before) without depending on the test
// goroutine being scheduled promptly at any single instant.
func TestFileStoreExpiry(t *testing.T) {
	fs := newTestFileStore(t, 0)

	const ttl = 20 * time.Millisecond
	now := time.Now()
	e := Entry{Status: 200, Body: []byte("x"), Created: now, Expires: now.Add(ttl)}
	Set(fs, "GET /expiring", e)

	if _, ok := Get(fs, "GET /expiring"); !ok {
		t.Fatalf("expected hit immediately after Set, before the TTL has any chance to elapse")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, ok := Get(fs, "GET /expiring")
		if !ok {
			break // self-evicted, as expected.
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected miss after expiry (self-eviction) within %s of a %s TTL, entry is still present", 2*time.Second, ttl)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	fs := newTestFileStore(t, 0)

	future := time.Now().Add(time.Minute)
	Set(fs, "GET /a", Entry{Status: 200, Body: []byte("a"), Expires: future})
	Set(fs, "../../etc/passwd", Entry{Status: 200, Body: []byte("traversal"), Expires: future})
	Set(fs, "GET /b", Entry{Status: 200, Body: []byte("b"), Expires: future})

	a, ok := Get(fs, "GET /a")
	if !ok || string(a.Body) != "a" {
		t.Fatalf("unexpected entry for key a: %+v ok=%v", a, ok)
	}
	trav, ok := Get(fs, "../../etc/passwd")
	if !ok || string(trav.Body) != "traversal" {
		t.Fatalf("unexpected entry for traversal key: %+v ok=%v", trav, ok)
	}
	b, ok := Get(fs, "GET /b")
	if !ok || string(b.Body) != "b" {
		t.Fatalf("unexpected entry for key b: %+v ok=%v", b, ok)
	}
}

func TestFileStoreMaxEntriesEviction(t *testing.T) {
	fs := kv.NewMemoryStore(kv.WithShardCount(1), kv.WithMaxEntries(2))

	future := time.Now().Add(time.Minute)
	Set(fs, "k1", Entry{Status: 200, Body: []byte("1"), Created: time.Now(), Expires: future})
	time.Sleep(2 * time.Millisecond)
	Set(fs, "k2", Entry{Status: 200, Body: []byte("2"), Created: time.Now(), Expires: future})
	time.Sleep(2 * time.Millisecond)
	Set(fs, "k3", Entry{Status: 200, Body: []byte("3"), Created: time.Now(), Expires: future})

	if n, err := fs.Len(); err != nil || n > 2 {
		t.Fatalf("expected store bounded at 2 entries, got n=%d err=%v", n, err)
	}
}

func TestFileStoreRejectsOversizedBody(t *testing.T) {
	fs := newTestFileStore(t, 0)

	big := make([]byte, maxCachedBodySize+1)
	Set(fs, "big", Entry{Status: 200, Body: big, Expires: time.Now().Add(time.Minute)})

	if _, ok := Get(fs, "big"); ok {
		t.Fatalf("expected oversized body to be rejected, not persisted")
	}
}

func TestFileStoreGC(t *testing.T) {
	fs := newTestFileStore(t, 0, kv.WithFileGCInterval(20*time.Millisecond))
	defer fs.Close()

	now := time.Now()
	Set(fs, "expiring", Entry{Status: 200, Body: []byte("x"), Created: now, Expires: now.Add(10 * time.Millisecond)})
	Set(fs, "fresh", Entry{Status: 200, Body: []byte("y"), Created: now, Expires: now.Add(time.Hour)})

	time.Sleep(80 * time.Millisecond)

	if _, ok := Get(fs, "fresh"); !ok {
		t.Fatalf("expected fresh entry to survive GC")
	}
}
