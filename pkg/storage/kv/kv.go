// Package kv provides the single low-level key-value storage primitive
// shared by fh's middleware packages (rate limiters, replay/nonce guards,
// response caches, reputation scores, allow-lists, admin registries, ...).
//
// Middleware packages should depend on Store directly and layer their
// domain-specific operations (rate-limit Allow, replay Seen, cache Get/Set,
// and so on) as package functions over a Store parameter. The concrete
// mechanics of "keep this durably, keyed by an arbitrary string, with an
// optional TTL" — sharded locking, atomic file writes, key-to-filename hashing,
// directory permissions, background expiry sweeps — live here once, in
// MemoryStore and FileStore.
package kv

import (
	"errors"
	"time"
)

// ErrClosed is returned by any operation on a Store after Close has run.
var ErrClosed = errors.New("kv: store is closed")

// ErrTooLarge is returned when a value exceeds a store's configured maximum
// entry size.
var ErrTooLarge = errors.New("kv: value exceeds maximum entry size")

// ErrCapacityExceeded is returned by Set when a store enforcing a maximum
// entry count is full and cannot admit a new key.
var ErrCapacityExceeded = errors.New("kv: capacity exceeded")

// Store is a generic byte-oriented key-value store with optional per-entry
// TTL. Keys are arbitrary strings; implementations must not assume they are
// safe to use directly as filesystem paths (FileStore hashes them).
//
// A zero ttl passed to Set means the entry never expires on its own (it can
// still be evicted under a capacity limit). All methods must be safe for
// concurrent use.
type Store interface {
	// Get returns the value for key and true if present and not expired.
	// A missing or expired key returns (nil, false, nil) — that is not an
	// error condition.
	Get(key string) ([]byte, bool, error)
	// Set stores value for key, replacing any previous value, with the given
	// time-to-live (<=0 means no expiry).
	Set(key string, value []byte, ttl time.Duration) error
	// Delete removes key. Deleting a missing key is not an error.
	Delete(key string) error
	// Len reports the number of live (non-expired, as of the last sweep for
	// implementations that only expire lazily) entries.
	Len() (int, error)
	// Close releases resources such as background GC goroutines. A closed
	// Store returns ErrClosed from every other method. Close is idempotent.
	Close() error
	// Mutate atomically loads the current value for key (nil, false if
	// absent or expired) and passes it to fn. fn returns the next value and
	// ttl to store, and ok=false to leave the entry untouched (e.g. a
	// capacity check that rejects the mutation). Mutate holds the same lock
	// Get/Set use for key for its entire duration, so callers implementing
	// counters (rate limiters, window counts, reputation scores) get a
	// race-free read-modify-write without needing their own locking or a
	// separate compare-and-swap dance.
	Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error
}

// Entry pairs a value with its absolute expiry so implementations can share
// a common on-disk/in-memory record shape.
type entry struct {
	Value     []byte    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (e entry) expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(now)
}
