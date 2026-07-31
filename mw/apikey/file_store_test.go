package apikey

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// writeRegistry persists records as the whole registry at dir, through a
// kv.FileStore the same way FileStore.SaveRegistry does, then closes it so
// the caller's own FileStore (or the next helper call) can open/reopen dir
// without contention. This stands in for what used to be a raw
// os.WriteFile of a hand-edited JSON array, now that persistence goes
// through kv.FileStore under a single fixed key.
func writeRegistry(t *testing.T, dir string, records []KeyRecord) {
	t.Helper()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	defer store.Close()
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatalf("marshal records: %v", err)
	}
	if err := store.Set(registryKey, data, 0); err != nil {
		t.Fatalf("set registry: %v", err)
	}
}

func TestFileStoreLookup(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, []KeyRecord{
		{ID: "key1", Name: "First", Hash: "h1", Scopes: []string{"read"}},
		{ID: "key2", Name: "Second", Hash: "h2", Revoked: true},
	})

	s, err := NewFileStore(dir, 0)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	defer s.StopWatch()

	rec, ok, err := s.Lookup(nil, "key1")
	if err != nil || !ok {
		t.Fatalf("expected key1 found, ok=%v err=%v", ok, err)
	}
	if rec.Name != "First" || len(rec.Scopes) != 1 || rec.Scopes[0] != "read" {
		t.Fatalf("unexpected record for key1: %+v", rec)
	}

	rec2, ok, err := s.Lookup(nil, "key2")
	if err != nil || !ok || !rec2.Revoked {
		t.Fatalf("expected key2 found and revoked, ok=%v err=%v rec=%+v", ok, err, rec2)
	}

	_, ok, err = s.Lookup(nil, "unknown")
	if err != nil || ok {
		t.Fatalf("expected unknown id to miss, ok=%v err=%v", ok, err)
	}
}

func TestFileStoreConstructorErrorsOnMalformedFile(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if err := store.Set(registryKey, []byte("not json"), 0); err != nil {
		t.Fatalf("set malformed registry: %v", err)
	}
	store.Close()

	if _, err := NewFileStore(dir, 0); err == nil {
		t.Fatalf("expected error constructing FileStore from malformed registry")
	}
}

func TestFileStoreConstructorErrorsOnMissingFile(t *testing.T) {
	dir := t.TempDir()

	if _, err := NewFileStore(dir, 0); err == nil {
		t.Fatalf("expected error constructing FileStore from a registry-less directory")
	}
}

func TestFileStoreReloadPicksUpChanges(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, []KeyRecord{
		{ID: "key1", Name: "Original"},
	})

	s, err := NewFileStore(dir, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	defer s.StopWatch()

	rec, ok, _ := s.Lookup(nil, "key1")
	if !ok || rec.Name != "Original" {
		t.Fatalf("expected original record, got ok=%v rec=%+v", ok, rec)
	}

	if err := s.SaveRegistry([]KeyRecord{
		{ID: "key1", Name: "Updated"},
		{ID: "key2", Name: "NewKey"},
	}); err != nil {
		t.Fatalf("SaveRegistry: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	rec, ok, _ = s.Lookup(nil, "key1")
	if !ok || rec.Name != "Updated" {
		t.Fatalf("expected updated record after reload, got ok=%v rec=%+v", ok, rec)
	}
	rec2, ok, _ := s.Lookup(nil, "key2")
	if !ok || rec2.Name != "NewKey" {
		t.Fatalf("expected new key2 after reload, got ok=%v rec=%+v", ok, rec2)
	}
}

func TestFileStoreReloadKeepsLastGoodOnMalformedUpdate(t *testing.T) {
	dir := t.TempDir()
	writeRegistry(t, dir, []KeyRecord{
		{ID: "key1", Name: "Good"},
	})

	s, err := NewFileStore(dir, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	defer s.StopWatch()

	// Corrupt the registry directly (bypassing SaveRegistry, which would
	// refuse to marshal/write anything malformed) to simulate an external
	// writer leaving a bad entry behind.
	badStore, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if err := badStore.Set(registryKey, []byte("{ broken"), 0); err != nil {
		t.Fatalf("set broken registry: %v", err)
	}
	badStore.Close()

	time.Sleep(100 * time.Millisecond)

	rec, ok, _ := s.Lookup(nil, "key1")
	if !ok || rec.Name != "Good" {
		t.Fatalf("expected last-good record retained after malformed reload, got ok=%v rec=%+v", ok, rec)
	}
}
