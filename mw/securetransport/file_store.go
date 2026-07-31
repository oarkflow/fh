package securetransport

import (
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// This file provides the durable (file-backed) counterparts to the Memory*
// stores in store.go. Both flavors now share the exact same mechanics layer
// -- kv.Store, via kv.NewMemoryStore/kv.NewFileStore -- so each File* type
// here is just a kvDeviceStore/kvSessionStore/kvReplayStore wearing a
// kv.FileStore instead of a kv.MemoryStore. Atomic file writes, key hashing,
// directory (0700) and file (0600) permissions, and background GC mechanics
// all live once in pkg/storage/kv/file.go; nothing here reimplements them.

// FileDeviceStore is a durable DeviceStore backed by kv.FileStore. Device
// records do not contain symmetric key material (only a public key), so
// there is no secure-erasure concern here beyond normal file permission
// hygiene, which kv.FileStore already provides (0700 dir / 0600 files).
type FileDeviceStore struct {
	*kvDeviceStore
}

// NewFileDeviceStore creates a FileDeviceStore rooted at dir. The directory
// is created with mode 0700 if it does not already exist.
func NewFileDeviceStore(dir string) (*FileDeviceStore, error) {
	fs, err := kv.NewFileStore(dir)
	if err != nil {
		return nil, err
	}
	return &FileDeviceStore{kvDeviceStore: newKVDeviceStore(fs)}, nil
}

// FileSessionStore is a durable SessionStore backed by kv.FileStore.
//
// SECURITY NOTE: Session.Keys holds live symmetric key material. Files
// written by the underlying kv.FileStore are created with mode 0600 inside
// a directory created with mode 0700, matching the discipline the in-memory
// store applies to heap memory (see zeroSession/wipe and wipeAndDelete in
// store.go). However, unlike the in-memory store, deleted key material may
// remain recoverable from disk/backups/journaling filesystems until
// overwritten by other data -- there is no portable, dependency-free way to
// securely erase already written disk blocks on Delete/DeleteByDevice. This
// store only removes (after first overwriting with zero bytes) the file; it
// does not and cannot guarantee the original bytes are gone from the
// physical medium. Encrypt the storage volume at rest if this matters for
// your threat model.
type FileSessionStore struct {
	*kvSessionStore
}

// NewFileSessionStore creates a FileSessionStore rooted at dir. The
// directory is created with mode 0700. Pass gcInterval <= 0 to disable the
// background expiry sweep; expired sessions are always removed lazily on
// Get regardless.
func NewFileSessionStore(dir string, gcInterval time.Duration) (*FileSessionStore, error) {
	fs, err := kv.NewFileStore(dir)
	if err != nil {
		return nil, err
	}
	s := &FileSessionStore{kvSessionStore: newKVSessionStore(fs)}
	s.startGC(gcInterval)
	return s, nil
}

// FileReplayStore is a durable ReplayStore backed by kv.FileStore. Arbitrary-
// length replay keys are hashed internally by kv.FileStore (SHA-256) before
// being used as a file name, exactly as the prior hand-rolled
// replayKeyFileName did.
type FileReplayStore struct {
	*kvReplayStore
}

// NewFileReplayStore creates a FileReplayStore rooted at dir. If maxEntries
// is <= 0 it defaults to 250_000, matching MemoryReplayStore.
func NewFileReplayStore(dir string, maxEntries int) (*FileReplayStore, error) {
	fs, err := kv.NewFileStore(dir)
	if err != nil {
		return nil, err
	}
	return &FileReplayStore{kvReplayStore: newKVReplayStore(fs, maxEntries)}, nil
}
