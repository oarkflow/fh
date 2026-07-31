package cache

import (
	"testing"
	"time"
)

func TestFileStoreGetSetDelete(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	if _, ok := fs.Get("GET /foo"); ok {
		t.Fatalf("expected miss for unknown key")
	}

	now := time.Now()
	e := Entry{Status: 200, ContentType: "text/plain", Body: []byte("hello"), Created: now, Expires: now.Add(time.Minute)}
	fs.Set("GET /foo", e)

	got, ok := fs.Get("GET /foo")
	if !ok {
		t.Fatalf("expected hit after Set")
	}
	if got.Status != 200 || string(got.Body) != "hello" || got.ContentType != "text/plain" {
		t.Fatalf("unexpected entry after Set: %+v", got)
	}

	fs.Delete("GET /foo")
	if _, ok := fs.Get("GET /foo"); ok {
		t.Fatalf("expected miss after Delete")
	}
}

func TestFileStoreExpiry(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	now := time.Now()
	e := Entry{Status: 200, Body: []byte("x"), Created: now, Expires: now.Add(20 * time.Millisecond)}
	fs.Set("GET /expiring", e)

	if _, ok := fs.Get("GET /expiring"); !ok {
		t.Fatalf("expected hit before expiry")
	}

	time.Sleep(40 * time.Millisecond)

	if _, ok := fs.Get("GET /expiring"); ok {
		t.Fatalf("expected miss after expiry (self-eviction)")
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	future := time.Now().Add(time.Minute)
	fs.Set("GET /a", Entry{Status: 200, Body: []byte("a"), Expires: future})
	fs.Set("../../etc/passwd", Entry{Status: 200, Body: []byte("traversal"), Expires: future})
	fs.Set("GET /b", Entry{Status: 200, Body: []byte("b"), Expires: future})

	a, ok := fs.Get("GET /a")
	if !ok || string(a.Body) != "a" {
		t.Fatalf("unexpected entry for key a: %+v ok=%v", a, ok)
	}
	trav, ok := fs.Get("../../etc/passwd")
	if !ok || string(trav.Body) != "traversal" {
		t.Fatalf("unexpected entry for traversal key: %+v ok=%v", trav, ok)
	}
	b, ok := fs.Get("GET /b")
	if !ok || string(b.Body) != "b" {
		t.Fatalf("unexpected entry for key b: %+v ok=%v", b, ok)
	}
}

func TestFileStoreMaxEntriesEviction(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 2)

	future := time.Now().Add(time.Minute)
	fs.Set("k1", Entry{Status: 200, Body: []byte("1"), Created: time.Now(), Expires: future})
	time.Sleep(2 * time.Millisecond)
	fs.Set("k2", Entry{Status: 200, Body: []byte("2"), Created: time.Now(), Expires: future})
	time.Sleep(2 * time.Millisecond)
	fs.Set("k3", Entry{Status: 200, Body: []byte("3"), Created: time.Now(), Expires: future})

	// k1 was oldest, should have been evicted to make room for k3.
	if _, ok := fs.Get("k1"); ok {
		t.Fatalf("expected k1 to be evicted")
	}
	if _, ok := fs.Get("k3"); !ok {
		t.Fatalf("expected k3 present")
	}
}

func TestFileStoreRejectsOversizedBody(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	big := make([]byte, maxCachedBodySize+1)
	fs.Set("big", Entry{Status: 200, Body: big, Expires: time.Now().Add(time.Minute)})

	if _, ok := fs.Get("big"); ok {
		t.Fatalf("expected oversized body to be rejected, not persisted")
	}
}

func TestFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStoreWithGC(dir, 0, 20*time.Millisecond)
	defer fs.StopGC()

	now := time.Now()
	fs.Set("expiring", Entry{Status: 200, Body: []byte("x"), Created: now, Expires: now.Add(10 * time.Millisecond)})
	fs.Set("fresh", Entry{Status: 200, Body: []byte("y"), Created: now, Expires: now.Add(time.Hour)})

	time.Sleep(80 * time.Millisecond)

	if _, ok := fs.Get("fresh"); !ok {
		t.Fatalf("expected fresh entry to survive GC")
	}
}
