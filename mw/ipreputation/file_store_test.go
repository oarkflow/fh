package ipreputation

import (
	"testing"
	"time"
)

func TestFileStoreGetSetUpdate(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	if _, ok := fs.Get("1.2.3.4"); ok {
		t.Fatalf("expected miss for unknown key")
	}

	fs.Set("1.2.3.4", &Score{Value: 10, Reasons: []string{"seed"}})
	sc, ok := fs.Get("1.2.3.4")
	if !ok {
		t.Fatalf("expected hit after Set")
	}
	if sc.Value != 10 || len(sc.Reasons) != 1 || sc.Reasons[0] != "seed" {
		t.Fatalf("unexpected score after Set: %+v", sc)
	}

	fs.Update("1.2.3.4", 5, "auth_failure")
	sc, ok = fs.Get("1.2.3.4")
	if !ok {
		t.Fatalf("expected hit after Update")
	}
	if sc.Value != 15 {
		t.Fatalf("expected value 15 after update, got %v", sc.Value)
	}
	if len(sc.Reasons) != 2 || sc.Reasons[1] != "auth_failure" {
		t.Fatalf("unexpected reasons after update: %v", sc.Reasons)
	}

	// Update never lets value go negative.
	fs.Update("1.2.3.4", -1000, "success")
	sc, _ = fs.Get("1.2.3.4")
	if sc.Value != 0 {
		t.Fatalf("expected value clamped to 0, got %v", sc.Value)
	}
}

func TestFileStoreUpdateOnMissingKey(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	fs.Update("9.9.9.9", 3, "bad_request")
	sc, ok := fs.Get("9.9.9.9")
	if !ok {
		t.Fatalf("expected entry to be created by Update")
	}
	if sc.Value != 3 {
		t.Fatalf("expected value 3, got %v", sc.Value)
	}
}

func TestFileStoreKeysDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir, 0)

	fs.Set("1.1.1.1", &Score{Value: 1})
	// Path-traversal-shaped key should be handled safely via hashing, not
	// interpreted as a path component.
	fs.Set("../../etc/passwd", &Score{Value: 2})
	fs.Set("2.2.2.2", &Score{Value: 3})

	sc1, ok := fs.Get("1.1.1.1")
	if !ok || sc1.Value != 1 {
		t.Fatalf("unexpected value for key1: %+v ok=%v", sc1, ok)
	}
	sc2, ok := fs.Get("../../etc/passwd")
	if !ok || sc2.Value != 2 {
		t.Fatalf("unexpected value for traversal key: %+v ok=%v", sc2, ok)
	}
	sc3, ok := fs.Get("2.2.2.2")
	if !ok || sc3.Value != 3 {
		t.Fatalf("unexpected value for key3: %+v ok=%v", sc3, ok)
	}
}

func TestFileStoreGC(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStoreWithGC(dir, 0, 20*time.Millisecond)
	defer fs.StopGC()

	fs.Set("3.3.3.3", &Score{Value: 0})
	fs.Set("4.4.4.4", &Score{Value: 50})

	time.Sleep(80 * time.Millisecond)

	if _, ok := fs.Get("3.3.3.3"); ok {
		t.Fatalf("expected zero-value score to be GC'd")
	}
	if _, ok := fs.Get("4.4.4.4"); !ok {
		t.Fatalf("expected non-zero score to survive GC")
	}
}
