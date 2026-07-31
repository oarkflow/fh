package cors

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeOriginFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}

func TestFileOriginStore_LoadAndLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "origins.json")
	writeOriginFile(t, path, `["https://example.com", "https://*.foo.com"]`)

	store, err := NewFileOriginStore(path, 0)
	if err != nil {
		t.Fatalf("NewFileOriginStore returned error: %v", err)
	}
	defer store.StopWatch()

	if !store.Allowed("https://example.com") {
		t.Errorf("expected https://example.com to be allowed")
	}
	if !store.Allowed("https://api.foo.com") {
		t.Errorf("expected https://api.foo.com to be allowed via wildcard")
	}
	if store.Allowed("https://other.com") {
		t.Errorf("expected https://other.com to be denied")
	}
}

func TestFileOriginStore_MalformedAtConstruction(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "missing.json")
	if _, err := NewFileOriginStore(path, 0); err == nil {
		t.Errorf("expected error for missing file")
	}

	badPath := filepath.Join(dir, "bad.json")
	writeOriginFile(t, badPath, `{not valid json`)
	if _, err := NewFileOriginStore(badPath, 0); err == nil {
		t.Errorf("expected error for malformed json")
	}
}

func TestFileOriginStore_Reload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "origins.json")
	writeOriginFile(t, path, `["https://example.com"]`)

	store, err := NewFileOriginStore(path, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileOriginStore returned error: %v", err)
	}
	defer store.StopWatch()

	if !store.Allowed("https://example.com") {
		t.Fatalf("expected https://example.com to be allowed initially")
	}
	if store.Allowed("https://new.com") {
		t.Fatalf("expected https://new.com to be denied initially")
	}

	writeOriginFile(t, path, `["https://new.com"]`)
	time.Sleep(80 * time.Millisecond)

	if !store.Allowed("https://new.com") {
		t.Errorf("expected https://new.com to be allowed after reload")
	}

	// A malformed edit should not break existing lookups: the last-good list
	// keeps serving.
	writeOriginFile(t, path, `{not valid json`)
	time.Sleep(80 * time.Millisecond)

	if !store.Allowed("https://new.com") {
		t.Errorf("expected https://new.com to remain allowed after a malformed edit")
	}
}

func TestFileOriginStore_StopWatchIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "origins.json")
	writeOriginFile(t, path, `["https://example.com"]`)

	store, err := NewFileOriginStore(path, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileOriginStore returned error: %v", err)
	}

	store.StopWatch()
	store.StopWatch() // must not panic

	// Also fine with reloadInterval disabled.
	store2, err := NewFileOriginStore(path, 0)
	if err != nil {
		t.Fatalf("NewFileOriginStore returned error: %v", err)
	}
	store2.StopWatch()
}
