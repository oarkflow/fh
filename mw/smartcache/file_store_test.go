package smartcache

import (
	"testing"
	"time"
)

func TestFileStoreGetSetDeletePurgeLen(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	defer s.StopGC()

	if s.Len() != 0 {
		t.Fatalf("expected empty store, got Len()=%d", s.Len())
	}

	resp := &Response{StatusCode: 200, Body: []byte("hello"), Headers: map[string][]string{"X-Test": {"1"}}}
	s.Set("key1", resp, time.Minute)

	got, ok := s.Get("key1")
	if !ok {
		t.Fatalf("expected hit for key1")
	}
	if got.StatusCode != 200 || string(got.Body) != "hello" {
		t.Fatalf("unexpected response: %+v", got)
	}

	if _, ok := s.Get("missing"); ok {
		t.Fatalf("expected miss for missing key")
	}

	if n := s.Len(); n != 1 {
		t.Fatalf("expected Len()=1, got %d", n)
	}

	s.Set("key2", &Response{StatusCode: 200, Body: []byte("world")}, time.Minute)
	if n := s.Len(); n != 2 {
		t.Fatalf("expected Len()=2, got %d", n)
	}

	s.Delete("key1")
	if _, ok := s.Get("key1"); ok {
		t.Fatalf("expected key1 deleted")
	}
	if n := s.Len(); n != 1 {
		t.Fatalf("expected Len()=1 after delete, got %d", n)
	}

	s.Purge()
	if n := s.Len(); n != 0 {
		t.Fatalf("expected Len()=0 after purge, got %d", n)
	}
}

func TestFileStoreTTLExpiry(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	defer s.StopGC()

	resp := &Response{StatusCode: 200, Body: []byte("expiring")}
	s.Set("ttl-key", resp, 20*time.Millisecond)

	if _, ok := s.Get("ttl-key"); !ok {
		t.Fatalf("expected hit immediately after Set")
	}

	time.Sleep(60 * time.Millisecond)

	if _, ok := s.Get("ttl-key"); ok {
		t.Fatalf("expected miss after TTL expiry")
	}
}

func TestFileStoreNoTTLNeverExpires(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	defer s.StopGC()

	s.Set("permanent", &Response{StatusCode: 200, Body: []byte("x")}, 0)
	time.Sleep(20 * time.Millisecond)
	if _, ok := s.Get("permanent"); !ok {
		t.Fatalf("expected entry with zero TTL to never expire")
	}
}

func TestFileStoreOversizedBodySkipped(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	defer s.StopGC()

	big := make([]byte, maxCachedBodySize+1)
	s.Set("big", &Response{StatusCode: 200, Body: big}, time.Minute)

	if _, ok := s.Get("big"); ok {
		t.Fatalf("expected oversized body to not be persisted")
	}
	if n := s.Len(); n != 0 {
		t.Fatalf("expected Len()=0 for skipped oversized entry, got %d", n)
	}
}

func TestFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 30*time.Millisecond)
	defer s.StopGC()

	s.Set("short", &Response{StatusCode: 200, Body: []byte("x")}, 10*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	if n := s.Len(); n != 0 {
		t.Fatalf("expected background GC to remove expired entry, Len()=%d", n)
	}
}

func TestFileStoreKeyPathTraversalSafe(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStore(dir, 0)
	defer s.StopGC()

	// Arbitrary, non-identifier cache keys (as produced by defaultKey: method
	// + full URL + query + vary headers) must never be used to build a path
	// directly.
	key := "GET\x00/../../etc/passwd?x=../../y"
	s.Set(key, &Response{StatusCode: 200, Body: []byte("safe")}, time.Minute)

	got, ok := s.Get(key)
	if !ok || string(got.Body) != "safe" {
		t.Fatalf("expected hashed-path lookup to work for traversal-like key")
	}
}
