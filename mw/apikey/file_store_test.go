package apikey

import (
	"testing"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// writeRegistry persists records as the whole registry at dir, through a
func newFileRegistry(t *testing.T, records []KeyRecord) kv.Store {
	t.Helper()
	store, err := kv.NewFileStore(t.TempDir(), kv.WithMaxEntrySize(maxRegistryFileSize))
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if err := SaveRegistry(store, records); err != nil {
		t.Fatalf("SaveRegistry: %v", err)
	}
	return store
}

func TestFileStoreLookup(t *testing.T) {
	s := newFileRegistry(t, []KeyRecord{
		{ID: "key1", Name: "First", Hash: "h1", Scopes: []string{"read"}},
		{ID: "key2", Name: "Second", Hash: "h2", Revoked: true},
	})

	rec, ok, err := Lookup(s, "key1")
	if err != nil || !ok {
		t.Fatalf("expected key1 found, ok=%v err=%v", ok, err)
	}
	if rec.Name != "First" || len(rec.Scopes) != 1 || rec.Scopes[0] != "read" {
		t.Fatalf("unexpected record for key1: %+v", rec)
	}

	rec2, ok, err := Lookup(s, "key2")
	if err != nil || !ok || !rec2.Revoked {
		t.Fatalf("expected key2 found and revoked, ok=%v err=%v rec=%+v", ok, err, rec2)
	}

	_, ok, err = Lookup(s, "unknown")
	if err != nil || ok {
		t.Fatalf("expected unknown id to miss, ok=%v err=%v", ok, err)
	}
}

func TestFileStoreConstructorErrorsOnMalformedFile(t *testing.T) {
	store, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if err := store.Set(registryKey, []byte("not json"), 0); err != nil {
		t.Fatalf("set malformed registry: %v", err)
	}
	store.Close()

	if _, _, err := Lookup(store, "anything"); err == nil {
		t.Fatalf("expected lookup error from malformed registry")
	}
}

func TestFileStoreMissingRegistryMisses(t *testing.T) {
	store, err := kv.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	if _, ok, err := Lookup(store, "missing"); err != nil || ok {
		t.Fatalf("expected missing registry to miss without error, ok=%v err=%v", ok, err)
	}
}

func TestFileStorePicksUpChanges(t *testing.T) {
	s := newFileRegistry(t, []KeyRecord{
		{ID: "key1", Name: "Original"},
	})

	rec, ok, _ := Lookup(s, "key1")
	if !ok || rec.Name != "Original" {
		t.Fatalf("expected original record, got ok=%v rec=%+v", ok, rec)
	}

	if err := SaveRegistry(s, []KeyRecord{
		{ID: "key1", Name: "Updated"},
		{ID: "key2", Name: "NewKey"},
	}); err != nil {
		t.Fatalf("SaveRegistry: %v", err)
	}

	rec, ok, _ = Lookup(s, "key1")
	if !ok || rec.Name != "Updated" {
		t.Fatalf("expected updated record after reload, got ok=%v rec=%+v", ok, rec)
	}
	rec2, ok, _ := Lookup(s, "key2")
	if !ok || rec2.Name != "NewKey" {
		t.Fatalf("expected new key2 after reload, got ok=%v rec=%+v", ok, rec2)
	}
}

func TestFileStoreMalformedUpdateReturnsLookupError(t *testing.T) {
	s := newFileRegistry(t, []KeyRecord{
		{ID: "key1", Name: "Good"},
	})

	if err := s.Set(registryKey, []byte("{ broken"), 0); err != nil {
		t.Fatalf("set broken registry: %v", err)
	}

	if _, _, err := Lookup(s, "key1"); err == nil {
		t.Fatalf("expected malformed registry to return lookup error")
	}
}
