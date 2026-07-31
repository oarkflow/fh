package smartcache

import (
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func newTestFileStore(t *testing.T, gcInterval time.Duration) *kv.FileStore {
	t.Helper()
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir, kv.WithMaxEntrySize(maxCacheEntrySize), kv.WithFileGCInterval(gcInterval))
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestFileBackedGetSetDeleteLen(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir(), kv.WithMaxEntrySize(maxCacheEntrySize))
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	defer s.Close()

	if n := Len(s); n != 0 {
		t.Fatalf("expected empty store, got Len()=%d", n)
	}

	resp := &Response{StatusCode: 200, Body: []byte("hello"), Headers: map[string][]string{"X-Test": {"1"}}}
	Set(s, "key1", resp, time.Minute)

	got, ok := Get(s, "key1")
	if !ok {
		t.Fatalf("expected hit for key1")
	}
	if got.StatusCode != 200 || string(got.Body) != "hello" {
		t.Fatalf("unexpected response: %+v", got)
	}

	if _, ok := Get(s, "missing"); ok {
		t.Fatalf("expected miss for missing key")
	}

	if n := Len(s); n != 1 {
		t.Fatalf("expected Len()=1, got %d", n)
	}

	Set(s, "key2", &Response{StatusCode: 200, Body: []byte("world")}, time.Minute)
	if n := Len(s); n != 2 {
		t.Fatalf("expected Len()=2, got %d", n)
	}

	Delete(s, "key1")
	if _, ok := Get(s, "key1"); ok {
		t.Fatalf("expected key1 deleted")
	}
	if n := Len(s); n != 1 {
		t.Fatalf("expected Len()=1 after delete, got %d", n)
	}
}

func TestFileBackedTTLExpiry(t *testing.T) {
	s := newTestFileStore(t, 0)

	resp := &Response{StatusCode: 200, Body: []byte("expiring")}
	Set(s, "ttl-key", resp, 20*time.Millisecond)

	if _, ok := Get(s, "ttl-key"); !ok {
		t.Fatalf("expected hit immediately after Set")
	}

	time.Sleep(60 * time.Millisecond)

	if _, ok := Get(s, "ttl-key"); ok {
		t.Fatalf("expected miss after TTL expiry")
	}
}

func TestFileBackedNoTTLNeverExpires(t *testing.T) {
	s := newTestFileStore(t, 0)

	Set(s, "permanent", &Response{StatusCode: 200, Body: []byte("x")}, 0)
	time.Sleep(20 * time.Millisecond)
	if _, ok := Get(s, "permanent"); !ok {
		t.Fatalf("expected entry with zero TTL to never expire")
	}
}

func TestFileBackedOversizedBodySkipped(t *testing.T) {
	s, err := kv.NewFileStore(t.TempDir(), kv.WithMaxEntrySize(1024))
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	defer s.Close()

	big := make([]byte, 2048)
	Set(s, "big", &Response{StatusCode: 200, Body: big}, time.Minute)

	if _, ok := Get(s, "big"); ok {
		t.Fatalf("expected store-rejected oversized entry to not be persisted")
	}
	if n := Len(s); n != 0 {
		t.Fatalf("expected Len()=0 for skipped oversized entry, got %d", n)
	}
}

func TestFileBackedGC(t *testing.T) {
	s := newTestFileStore(t, 30*time.Millisecond)

	Set(s, "short", &Response{StatusCode: 200, Body: []byte("x")}, 10*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	if n := Len(s); n != 0 {
		t.Fatalf("expected background GC to remove expired entry, Len()=%d", n)
	}
}

func TestFileBackedKeyPathTraversalSafe(t *testing.T) {
	s := newTestFileStore(t, 0)

	// Arbitrary, non-identifier cache keys (as produced by defaultKey: method
	// + full URL + query + vary headers) must never be used to build a path
	// directly.
	key := "GET\x00/../../etc/passwd?x=../../y"
	Set(s, key, &Response{StatusCode: 200, Body: []byte("safe")}, time.Minute)

	got, ok := Get(s, key)
	if !ok || string(got.Body) != "safe" {
		t.Fatalf("expected hashed-path lookup to work for traversal-like key")
	}
}
