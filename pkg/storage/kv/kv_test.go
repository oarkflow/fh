package kv

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testStoreBasic(t *testing.T, s Store) {
	t.Helper()
	if _, ok, err := s.Get("missing"); err != nil || ok {
		t.Fatalf("Get(missing) = %v, %v, %v", ok, err, err)
	}
	if err := s.Set("a", []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("b", []byte("2"), 0); err != nil {
		t.Fatal(err)
	}
	v, ok, err := s.Get("a")
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(a) = %q, %v, %v", v, ok, err)
	}
	v, ok, err = s.Get("b")
	if err != nil || !ok || string(v) != "2" {
		t.Fatalf("Get(b) = %q, %v, %v", v, ok, err)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Get("a"); err != nil || ok {
		t.Fatalf("Get(a) after delete = %v, %v", ok, err)
	}
	if err := s.Delete("does-not-exist"); err != nil {
		t.Fatalf("Delete(missing) should not error, got %v", err)
	}
}

func testStoreTTL(t *testing.T, s Store) {
	t.Helper()
	if err := s.Set("expiring", []byte("x"), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := s.Get("expiring"); err != nil || !ok || string(v) != "x" {
		t.Fatalf("Get before expiry = %q, %v, %v", v, ok, err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok, err := s.Get("expiring"); err != nil || ok {
		t.Fatalf("Get after expiry = %v, %v, want miss", ok, err)
	}
}

func testStoreKeyCollisionSafety(t *testing.T, s Store) {
	t.Helper()
	keys := []string{"../../etc/passwd", "normal", "with space", "unicode-éè", ""}
	for i, k := range keys {
		if err := s.Set(k, []byte{byte(i)}, 0); err != nil {
			t.Fatalf("Set(%q) = %v", k, err)
		}
	}
	for i, k := range keys {
		v, ok, err := s.Get(k)
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("Get(%q) = %v, %v, %v; want %d", k, v, ok, err, i)
		}
	}
}

func testStoreMutateCounter(t *testing.T, s Store) {
	t.Helper()
	inc := func() (int, error) {
		var result int
		err := s.Mutate("counter", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
			n := 0
			if exists {
				n = int(current[0])
			}
			n++
			result = n
			return []byte{byte(n)}, 0, true, nil
		})
		return result, err
	}
	for i := 1; i <= 5; i++ {
		n, err := inc()
		if err != nil || n != i {
			t.Fatalf("inc() = %d, %v; want %d", n, err, i)
		}
	}
}

func testStoreMutateConcurrent(t *testing.T, s Store) {
	t.Helper()
	const goroutines = 50
	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			_ = s.Mutate("shared-counter", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
				n := 0
				if exists {
					n = int(current[0])
				}
				n++
				return []byte{byte(n)}, 0, true, nil
			})
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	v, ok, err := s.Get("shared-counter")
	if err != nil || !ok || int(v[0]) != goroutines {
		t.Fatalf("final counter = %v, %v, %v; want %d (no lost updates)", v, ok, err, goroutines)
	}
}

func testStoreMutateRejects(t *testing.T, s Store) {
	t.Helper()
	err := s.Mutate("rejected", func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		return nil, 0, false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get("rejected"); ok {
		t.Fatal("Mutate with ok=false should not have written a value")
	}
}

func TestMemoryStoreMutateCounter(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()
	testStoreMutateCounter(t, s)
}

func TestMemoryStoreMutateConcurrent(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()
	testStoreMutateConcurrent(t, s)
}

func TestMemoryStoreMutateRejects(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()
	testStoreMutateRejects(t, s)
}

func TestFileStoreMutateCounter(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStoreMutateCounter(t, s)
}

func TestFileStoreMutateConcurrent(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStoreMutateConcurrent(t, s)
}

func TestFileStoreMutateRejects(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStoreMutateRejects(t, s)
}

func TestMemoryStore(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()
	testStoreBasic(t, s)
}

func TestMemoryStoreTTL(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()
	testStoreTTL(t, s)
}

func TestMemoryStoreKeySafety(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()
	testStoreKeyCollisionSafety(t, s)
}

func TestMemoryStoreMaxEntries(t *testing.T) {
	s := NewMemoryStore(WithShardCount(1), WithMaxEntries(2))
	defer s.Close()
	if err := s.Set("a", []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("b", []byte("2"), 0); err != nil {
		t.Fatal(err)
	}
	// Third distinct key over capacity: with no expired entries to reclaim,
	// evictOne falls back to evicting an arbitrary existing entry, so this
	// should still succeed (capacity is a soft cap), not error.
	if err := s.Set("c", []byte("3"), 0); err != nil {
		t.Fatalf("Set over soft cap should evict, not fail: %v", err)
	}
	n, err := s.Len()
	if err != nil {
		t.Fatal(err)
	}
	if n > 2 {
		t.Fatalf("Len() = %d, want <= 2 after eviction", n)
	}
}

func TestMemoryStoreClosed(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got %v", err)
	}
	if _, _, err := s.Get("a"); err != ErrClosed {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	if err := s.Set("a", []byte("1"), 0); err != ErrClosed {
		t.Fatalf("Set after Close = %v, want ErrClosed", err)
	}
}

func TestFileStore(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStoreBasic(t, s)
}

func TestFileStoreTTL(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStoreTTL(t, s)
}

func TestFileStoreKeySafety(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStoreKeyCollisionSafety(t, s)
}

func TestFileStorePersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Set("k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	_ = s1.Close()

	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	v, ok, err := s2.Get("k")
	if err != nil || !ok || string(v) != "v" {
		t.Fatalf("Get after reopen = %q, %v, %v", v, ok, err)
	}
}

func TestFileStoreTooLarge(t *testing.T) {
	s, err := NewFileStore(t.TempDir(), WithMaxEntrySize(4))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Set("k", []byte("12345"), 0); err != ErrTooLarge {
		t.Fatalf("Set oversized = %v, want ErrTooLarge", err)
	}
}

func TestFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir, WithFileGCInterval(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Set("k", []byte("v"), 15*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	matches, _ := filepath.Glob(filepath.Join(dir, "*.kv"))
	if len(matches) != 0 {
		t.Fatalf("expected GC to remove expired entry files, found %v", matches)
	}
}

func TestFileStoreClosed(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got %v", err)
	}
	if _, _, err := s.Get("a"); err != ErrClosed {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
}

func TestFileStoreDirPermissions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub")
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Regression guard: MkdirAll silently no-ops on an existing dir without
	// correcting its mode, which is why NewFileStore also calls Chmod
	// explicitly (t.TempDir() and pre-existing dirs are not created 0700).
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0700 {
		t.Fatalf("dir perm = %v, want 0700", info.Mode().Perm())
	}
}
