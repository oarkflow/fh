package ipwhitelist

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

func writeIPFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}

func TestFileStore_LoadAndLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ips.json")
	writeIPFile(t, path, `["10.0.0.1", "192.168.1.0/24"]`)

	store := kv.NewMemoryStore()
	if err := LoadFile(store, path); err != nil {
		t.Fatalf("LoadFile returned error: %v", err)
	}

	if !AllowedIP(store, net.ParseIP("10.0.0.1")) {
		t.Errorf("expected 10.0.0.1 to be allowed")
	}
	if !AllowedIP(store, net.ParseIP("192.168.1.50")) {
		t.Errorf("expected 192.168.1.50 to be allowed via CIDR")
	}
	if AllowedIP(store, net.ParseIP("8.8.8.8")) {
		t.Errorf("expected 8.8.8.8 to be denied")
	}
}

func TestFileStore_MalformedAtConstruction(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "missing.json")
	if err := LoadFile(kv.NewMemoryStore(), path); err == nil {
		t.Errorf("expected error for missing file")
	}

	badJSONPath := filepath.Join(dir, "bad.json")
	writeIPFile(t, badJSONPath, `{not valid json`)
	if err := LoadFile(kv.NewMemoryStore(), badJSONPath); err == nil {
		t.Errorf("expected error for malformed json")
	}

	badIPPath := filepath.Join(dir, "badip.json")
	writeIPFile(t, badIPPath, `["not-an-ip"]`)
	if err := LoadFile(kv.NewMemoryStore(), badIPPath); err == nil {
		t.Errorf("expected error for invalid IP entry")
	}
}

func TestFileStore_Reload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ips.json")
	writeIPFile(t, path, `["10.0.0.1"]`)

	store := kv.NewMemoryStore()
	stop, err := StartFileWatcher(store, path, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("StartFileWatcher returned error: %v", err)
	}
	defer stop()

	if !AllowedIP(store, net.ParseIP("10.0.0.1")) {
		t.Fatalf("expected 10.0.0.1 to be allowed initially")
	}
	if AllowedIP(store, net.ParseIP("10.0.0.2")) {
		t.Fatalf("expected 10.0.0.2 to be denied initially")
	}

	writeIPFile(t, path, `["10.0.0.2"]`)
	time.Sleep(80 * time.Millisecond)

	if !AllowedIP(store, net.ParseIP("10.0.0.2")) {
		t.Errorf("expected 10.0.0.2 to be allowed after reload")
	}

	// A malformed edit should not break existing lookups: the last-good list
	// keeps serving.
	writeIPFile(t, path, `{not valid json`)
	time.Sleep(80 * time.Millisecond)

	if !AllowedIP(store, net.ParseIP("10.0.0.2")) {
		t.Errorf("expected 10.0.0.2 to remain allowed after a malformed edit")
	}
}

func TestFileStore_StopWatchIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ips.json")
	writeIPFile(t, path, `["10.0.0.1"]`)

	store := kv.NewMemoryStore()
	stop, err := StartFileWatcher(store, path, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("StartFileWatcher returned error: %v", err)
	}

	stop()
	stop() // must not panic

	stop2, err := StartFileWatcher(kv.NewMemoryStore(), path, 0)
	if err != nil {
		t.Fatalf("StartFileWatcher returned error: %v", err)
	}
	stop2()
}
