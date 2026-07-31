package securetransport

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	protocol "github.com/oarkflow/fh/pkg/securetransport"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func randID16(t *testing.T) protocol.ID16 {
	t.Helper()
	id, err := protocol.NewID()
	if err != nil {
		t.Fatalf("NewID: %v", err)
	}
	return id
}

func TestFileDeviceStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	id := randID16(t)
	dev := Device{
		ID: id, PublicKey: [32]byte{1, 2, 3}, Name: "laptop", Principal: "user-1",
		CreatedAt: time.Now().Truncate(time.Second),
	}
	if err := registerDevice(store, dev); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := registerDevice(store, dev); err == nil {
		t.Fatalf("expected duplicate id error on second Register")
	}

	got, err := getDevice(store, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != dev.Name || got.Principal != dev.Principal {
		t.Fatalf("round trip mismatch: got %+v", got)
	}

	touchAt := time.Now().Add(time.Minute).Truncate(time.Second)
	if err := touchDevice(store, id, touchAt); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	got, err = getDevice(store, id)
	if err != nil {
		t.Fatalf("Get after touch: %v", err)
	}
	if !got.LastSeen.Equal(touchAt) {
		t.Fatalf("LastSeen not updated: got %v want %v", got.LastSeen, touchAt)
	}

	revokeAt := time.Now().Truncate(time.Second)
	if err := revokeDevice(store, id, revokeAt); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := getDevice(store, id); err != ErrDeviceRevoked {
		t.Fatalf("expected ErrDeviceRevoked, got %v", err)
	}

	unknown := randID16(t)
	if _, err := getDevice(store, unknown); err != ErrDeviceNotFound {
		t.Fatalf("expected ErrDeviceNotFound, got %v", err)
	}
	if err := touchDevice(store, unknown, time.Now()); err != ErrDeviceNotFound {
		t.Fatalf("expected ErrDeviceNotFound on Touch, got %v", err)
	}
	if err := revokeDevice(store, unknown, time.Now()); err != ErrDeviceNotFound {
		t.Fatalf("expected ErrDeviceNotFound on Revoke, got %v", err)
	}
}

func TestFileSessionStore_RoundTripAndPermissions(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	sessID := randID16(t)
	devID := randID16(t)
	sess := Session{
		ID: sessID, DeviceID: devID, Principal: "user-1",
		Keys:      protocol.SessionKeys{ClientToServer: [32]byte{9, 9, 9}, ServerToClient: [32]byte{8, 8, 8}},
		CreatedAt: time.Now().Truncate(time.Second),
		ExpiresAt: time.Now().Add(time.Hour).Truncate(time.Second),
		KeyID:     "server-v1",
	}
	if err := createSession(store, sess); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := createSession(store, sess); err == nil {
		t.Fatalf("expected duplicate session id error")
	}

	got, err := getSession(store, sessID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Keys.ClientToServer != sess.Keys.ClientToServer || got.Keys.ServerToClient != sess.Keys.ServerToClient {
		t.Fatalf("key round trip mismatch")
	}

	// Security-relevant assertions: directory 0700, session file 0600.
	if runtime.GOOS != "windows" {
		dirInfo, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat dir: %v", err)
		}
		if perm := dirInfo.Mode().Perm(); perm != 0700 {
			t.Fatalf("expected dir mode 0700, got %o", perm)
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		found := false
		for _, entry := range entries {
			// Entry files are now named by kv.FileStore's own convention
			// (hex-encoded SHA-256 of the logical key, ".kv" extension)
			// rather than the prior hand-rolled "<id>.session.json"; the
			// permission assertions below are what actually matter here.
			if filepath.Ext(entry.Name()) != ".kv" {
				continue
			}
			found = true
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("entry.Info: %v", err)
			}
			if perm := info.Mode().Perm(); perm != 0600 {
				t.Fatalf("expected session file mode 0600, got %o for %s", perm, entry.Name())
			}
		}
		if !found {
			t.Fatalf("expected at least one session file on disk")
		}
	}

	if err := deleteSession(store, sessID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := getSession(store, sessID); err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound after delete, got %v", err)
	}

	// DeleteByDevice
	sess2ID := randID16(t)
	sess2 := sess
	sess2.ID = sess2ID
	if err := createSession(store, sess2); err != nil {
		t.Fatalf("Create sess2: %v", err)
	}
	if err := deleteSessionsByDevice(store, devID); err != nil {
		t.Fatalf("DeleteByDevice: %v", err)
	}
	if _, err := getSession(store, sess2ID); err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound after DeleteByDevice, got %v", err)
	}
}

func TestFileSessionStore_ExpiryTriggersNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}

	id := randID16(t)
	sess := Session{
		ID: id, DeviceID: randID16(t), Principal: "user-1",
		Keys:      protocol.SessionKeys{ClientToServer: [32]byte{1}, ServerToClient: [32]byte{2}},
		CreatedAt: time.Now().Add(-time.Hour),
		ExpiresAt: time.Now().Add(-time.Minute), // already expired
	}
	if err := createSession(store, sess); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := getSession(store, id); err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound for expired session, got %v", err)
	}
	// File should be gone after lazy expiry on Get.
	if _, err := getSession(store, id); err != ErrSessionNotFound {
		t.Fatalf("expected ErrSessionNotFound on second Get, got %v", err)
	}
}

func TestFileReplayStore_AcceptRejectCapacity(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var replayMu sync.Mutex

	future := time.Now().Add(time.Hour)

	ok, err := checkAndStoreReplay(store, &replayMu, 2, "key-1", future)
	if err != nil || !ok {
		t.Fatalf("expected accept for key-1, got ok=%v err=%v", ok, err)
	}

	ok, err = checkAndStoreReplay(store, &replayMu, 2, "key-1", future)
	if err != nil || ok {
		t.Fatalf("expected reject (replay) for key-1, got ok=%v err=%v", ok, err)
	}

	ok, err = checkAndStoreReplay(store, &replayMu, 2, "key-2", future)
	if err != nil || !ok {
		t.Fatalf("expected accept for key-2, got ok=%v err=%v", ok, err)
	}

	// Capacity is now exhausted (maxEntries=2, both non-expired).
	_, err = checkAndStoreReplay(store, &replayMu, 2, "key-3", future)
	if err == nil {
		t.Fatalf("expected capacity exhausted error for key-3")
	}
}

func TestFileReplayStore_ExpiredEntriesEvicted(t *testing.T) {
	dir := t.TempDir()
	store, err := kv.NewFileStore(dir)
	if err != nil {
		t.Fatalf("kv.NewFileStore: %v", err)
	}
	var replayMu sync.Mutex

	past := time.Now().Add(-time.Minute)
	ok, err := checkAndStoreReplay(store, &replayMu, 1, "expiring-key", past)
	if err != nil || !ok {
		t.Fatalf("expected accept for expiring-key, got ok=%v err=%v", ok, err)
	}

	future := time.Now().Add(time.Hour)
	// Capacity is 1 and full, but the existing entry is expired so it should
	// be evicted opportunistically, making room for the new key.
	ok, err = checkAndStoreReplay(store, &replayMu, 1, "new-key", future)
	if err != nil || !ok {
		t.Fatalf("expected accept for new-key after eviction, got ok=%v err=%v", ok, err)
	}
}
