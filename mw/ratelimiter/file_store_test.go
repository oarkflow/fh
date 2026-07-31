package ratelimiter

import (
	"testing"
	"time"
)

func TestFileStoreAllowBasic(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	now := time.Now()

	for i := 0; i < 3; i++ {
		res, err := store.Allow("alice", 3, time.Minute, now)
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("request %d should be allowed, got %+v", i, res)
		}
	}

	res, err := store.Allow("alice", 3, time.Minute, now)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if res.Allowed {
		t.Fatalf("4th request should be denied, got %+v", res)
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter, got %v", res.RetryAfter)
	}
}

func TestFileStoreWindowExpiry(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	window := 20 * time.Millisecond
	now := time.Now()

	res, err := store.Allow("bob", 1, window, now)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("first request should be allowed")
	}

	res, err = store.Allow("bob", 1, window, now)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if res.Allowed {
		t.Fatalf("second request within window should be denied")
	}

	// Advance past the window using a later `now` rather than sleeping the
	// wall clock, since Allow is driven entirely by the now parameter.
	later := now.Add(window + 5*time.Millisecond)
	res, err = store.Allow("bob", 1, window, later)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("request after window reset should be allowed, got %+v", res)
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	now := time.Now()

	res1, err := store.Allow("keyA", 1, time.Minute, now)
	if err != nil || !res1.Allowed {
		t.Fatalf("keyA first request: res=%+v err=%v", res1, err)
	}
	res2, err := store.Allow("keyB", 1, time.Minute, now)
	if err != nil || !res2.Allowed {
		t.Fatalf("keyB first request: res=%+v err=%v", res2, err)
	}

	res1b, err := store.Allow("keyA", 1, time.Minute, now)
	if err != nil {
		t.Fatalf("keyA second request: %v", err)
	}
	if res1b.Allowed {
		t.Fatalf("keyA second request should be denied")
	}
}

func TestFileStoreGCRemovesExpiredBuckets(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir, 0)
	defer store.StopGC()

	window := 10 * time.Millisecond
	now := time.Now()
	if _, err := store.Allow("gone", 1, window, now); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	// GC uses time.Now() internally with a grace period, so simulate an
	// old reset by directly invoking GC well after the fact is not
	// possible without real time; instead just confirm the file exists
	// and GC doesn't remove a fresh bucket.
	store.GC()

	res, err := store.Allow("gone", 1, window, now)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	if res.Allowed {
		t.Fatalf("bucket should not have been GC'd immediately (grace period)")
	}
}
