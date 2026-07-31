package securetransport

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"

	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

var (
	ErrDeviceNotFound  = errors.New("secure transport: device not found")
	ErrDeviceRevoked   = errors.New("secure transport: device revoked")
	ErrSessionNotFound = errors.New("secure transport: session not found")

	errDuplicateDevice         = errors.New("secure transport: duplicate device id")
	errDuplicateSession        = errors.New("secure transport: duplicate session id")
	errReplayCapacityExhausted = errors.New("secure transport: replay store capacity exhausted")
)

type Device struct {
	ID        protocol.ID16
	PublicKey [32]byte
	Name      string
	Principal string
	CreatedAt time.Time
	LastSeen  time.Time
	RevokedAt time.Time
}

func (d Device) Revoked() bool { return !d.RevokedAt.IsZero() }

type Session struct {
	ID        protocol.ID16
	DeviceID  protocol.ID16
	Principal string
	Keys      protocol.SessionKeys
	CreatedAt time.Time
	ExpiresAt time.Time
	KeyID     string
}

// SessionInfo is the key-free session view exposed to application handlers.
type SessionInfo struct {
	ID        protocol.ID16
	DeviceID  protocol.ID16
	Principal string
	CreatedAt time.Time
	ExpiresAt time.Time
	KeyID     string
}

func publicSession(session Session) SessionInfo {
	return SessionInfo{
		ID: session.ID, DeviceID: session.DeviceID, Principal: session.Principal,
		CreatedAt: session.CreatedAt, ExpiresAt: session.ExpiresAt, KeyID: session.KeyID,
	}
}

func zeroSession(session *Session) {
	if session == nil {
		return
	}
	wipe(session.Keys.ClientToServer[:])
	wipe(session.Keys.ServerToClient[:])
}

// ---------------------------------------------------------------------------
// Device, session and replay state all live in a caller-supplied kv.Store
// (see pkg/storage/kv) -- in-memory (kv.NewMemoryStore) or durable
// (kv.NewFileStore), the operations below don't care which. Each domain just
// wears its own JSON-encoded record shape and key scheme on top of the same
// Get/Set/Delete/Len/Mutate primitives; there is no package-specific store
// interface or Memory/File wrapper type, just plain functions parameterized
// by kv.Store.
// ---------------------------------------------------------------------------

type fileDevice struct {
	ID        protocol.ID16 `json:"id"`
	PublicKey [32]byte      `json:"public_key"`
	Name      string        `json:"name"`
	Principal string        `json:"principal"`
	CreatedAt time.Time     `json:"created_at"`
	LastSeen  time.Time     `json:"last_seen"`
	RevokedAt time.Time     `json:"revoked_at"`
}

func toFileDevice(d Device) fileDevice {
	return fileDevice{
		ID: d.ID, PublicKey: d.PublicKey, Name: d.Name, Principal: d.Principal,
		CreatedAt: d.CreatedAt, LastSeen: d.LastSeen, RevokedAt: d.RevokedAt,
	}
}

func (f fileDevice) toDevice() Device {
	return Device{
		ID: f.ID, PublicKey: f.PublicKey, Name: f.Name, Principal: f.Principal,
		CreatedAt: f.CreatedAt, LastSeen: f.LastSeen, RevokedAt: f.RevokedAt,
	}
}

type fileSession struct {
	ID        protocol.ID16        `json:"id"`
	DeviceID  protocol.ID16        `json:"device_id"`
	Principal string               `json:"principal"`
	Keys      protocol.SessionKeys `json:"keys"`
	CreatedAt time.Time            `json:"created_at"`
	ExpiresAt time.Time            `json:"expires_at"`
	KeyID     string               `json:"key_id"`
}

func toFileSession(s Session) fileSession {
	return fileSession{
		ID: s.ID, DeviceID: s.DeviceID, Principal: s.Principal, Keys: s.Keys,
		CreatedAt: s.CreatedAt, ExpiresAt: s.ExpiresAt, KeyID: s.KeyID,
	}
}

func (f fileSession) toSession() Session {
	return Session{
		ID: f.ID, DeviceID: f.DeviceID, Principal: f.Principal, Keys: f.Keys,
		CreatedAt: f.CreatedAt, ExpiresAt: f.ExpiresAt, KeyID: f.KeyID,
	}
}

func deviceKey(id protocol.ID16) string { return "device:" + hex.EncodeToString(id[:]) }

// ---------------------------------------------------------------------------
// Device operations. Device records never expire and hold no secret key
// material (only a public key), so these are a straightforward JSON-over-kv
// wrapper with no locking beyond what kv.Store.Mutate already provides.
// ---------------------------------------------------------------------------

// registerDevice uses Mutate rather than Get-then-Set so that two concurrent
// registerDevice calls for the same device ID cannot both observe "not
// present" and both write: Mutate holds the per-key lock for the whole
// check+write.
func registerDevice(store kv.Store, device Device) error {
	return store.Mutate(deviceKey(device.ID), func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			return nil, 0, false, errDuplicateDevice
		}
		data, err := json.Marshal(toFileDevice(device))
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
}

func getDevice(store kv.Store, id protocol.ID16) (Device, error) {
	data, exists, err := store.Get(deviceKey(id))
	if err != nil {
		return Device{}, err
	}
	if !exists {
		return Device{}, ErrDeviceNotFound
	}
	var fd fileDevice
	if err := json.Unmarshal(data, &fd); err != nil {
		return Device{}, err
	}
	device := fd.toDevice()
	if device.Revoked() {
		return Device{}, ErrDeviceRevoked
	}
	return device, nil
}

func touchDevice(store kv.Store, id protocol.ID16, at time.Time) error {
	return store.Mutate(deviceKey(id), func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists {
			return nil, 0, false, ErrDeviceNotFound
		}
		var fd fileDevice
		if err := json.Unmarshal(current, &fd); err != nil {
			return nil, 0, false, err
		}
		fd.LastSeen = at
		data, err := json.Marshal(fd)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
}

func revokeDevice(store kv.Store, id protocol.ID16, at time.Time) error {
	return store.Mutate(deviceKey(id), func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists {
			return nil, 0, false, ErrDeviceNotFound
		}
		var fd fileDevice
		if err := json.Unmarshal(current, &fd); err != nil {
			return nil, 0, false, err
		}
		fd.RevokedAt = at
		data, err := json.Marshal(fd)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
}

// ---------------------------------------------------------------------------
// Session operations.
//
// SECURITY NOTE: Session.Keys holds live symmetric key material. Every
// removal path (deleteSession, deleteSessionsByDevice, lazy expiry on Get,
// and SweepExpiredSessions) overwrites the stored record with zero bytes via
// kv.Store.Mutate *before* calling kv.Store.Delete (see wipeAndDelete below),
// mirroring the discipline the original hand-rolled MemorySessionStore
// applied to its map values before dropping them, and giving
// kv.FileStore-backed instances an actual on-disk overwrite pass (via
// FileStore's atomic write) prior to unlink. That said, this is still not
// secure erasure: a deleted (even zero-overwritten) file's prior on-disk
// blocks may remain recoverable from backups, snapshots, or journaling
// filesystems until overwritten by unrelated data. Encrypt the storage
// volume at rest if that matters for your threat model.
//
// kv.Store has no query/iteration primitive (by design -- see pkg/storage/kv
// doc comment), so deleteSessionsByDevice cannot ask the store "which keys
// have this DeviceID". These functions therefore maintain their own
// secondary index: one kv entry per device (key "sessdevidx:<deviceHex>")
// holding the JSON list of session ID hex strings created for that device,
// kept in sync on create/delete/expiry, plus one global index entry
// ("sessidx:all") used by SweepExpiredSessions to find candidates to expire
// without a full store scan. Index updates are not transactionally atomic
// with the session record write (two separate kv calls), so there is a
// narrow window where a session exists but is not yet indexed (or vice versa
// mid-delete); deleteSessionsByDevice is documented/expected to be a rare
// admin operation, so this tradeoff (simplicity over perfect atomicity) was
// judged acceptable rather than adding a second kv.Store+cross-store
// transaction just for this.
// ---------------------------------------------------------------------------

const sessionAllIndexKey = "sessidx:all"

func sessionKey(id protocol.ID16) string { return "sess:" + hex.EncodeToString(id[:]) }

func sessionDeviceIndexKey(deviceID protocol.ID16) string {
	return "sessdevidx:" + hex.EncodeToString(deviceID[:])
}

// addIndexEntry appends sessionHex to the JSON string-list stored at idxKey,
// if not already present.
func addIndexEntry(store kv.Store, idxKey, sessionHex string) error {
	return store.Mutate(idxKey, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var ids []string
		if exists {
			if err := json.Unmarshal(current, &ids); err != nil {
				ids = nil
			}
		}
		for _, id := range ids {
			if id == sessionHex {
				return nil, 0, false, nil
			}
		}
		ids = append(ids, sessionHex)
		data, err := json.Marshal(ids)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
}

// removeIndexEntry removes sessionHex from the JSON string-list stored at
// idxKey, if present.
func removeIndexEntry(store kv.Store, idxKey, sessionHex string) error {
	return store.Mutate(idxKey, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists {
			return nil, 0, false, nil
		}
		var ids []string
		if err := json.Unmarshal(current, &ids); err != nil {
			return nil, 0, false, nil
		}
		out := make([]string, 0, len(ids))
		changed := false
		for _, id := range ids {
			if id == sessionHex {
				changed = true
				continue
			}
			out = append(out, id)
		}
		if !changed {
			return nil, 0, false, nil
		}
		data, err := json.Marshal(out)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
}

// wipeAndDelete overwrites the value stored at key with zero bytes of the
// same length (a defense-in-depth pass over the underlying storage) before
// removing the key outright.
func wipeAndDelete(store kv.Store, key string) error {
	_ = store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if !exists {
			return nil, 0, false, nil
		}
		return make([]byte, len(current)), time.Nanosecond, true, nil
	})
	return store.Delete(key)
}

func createSession(store kv.Store, session Session) error {
	key := sessionKey(session.ID)
	err := store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			return nil, 0, false, errDuplicateSession
		}
		data, err := json.Marshal(toFileSession(session))
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
	if err != nil {
		return err
	}
	idHex := hex.EncodeToString(session.ID[:])
	_ = addIndexEntry(store, sessionDeviceIndexKey(session.DeviceID), idHex)
	_ = addIndexEntry(store, sessionAllIndexKey, idHex)
	return nil
}

// getSession expires the session lazily: a record whose ExpiresAt has passed
// is wiped and removed on this read (rather than surviving until some
// background sweep runs) and ErrSessionNotFound is returned, exactly as if
// it had already been deleted.
func getSession(store kv.Store, id protocol.ID16) (Session, error) {
	key := sessionKey(id)
	data, exists, err := store.Get(key)
	if err != nil {
		return Session{}, err
	}
	if !exists {
		return Session{}, ErrSessionNotFound
	}
	var fs fileSession
	if err := json.Unmarshal(data, &fs); err != nil {
		return Session{}, err
	}
	session := fs.toSession()
	if !session.ExpiresAt.After(time.Now()) {
		idHex := hex.EncodeToString(id[:])
		_ = removeIndexEntry(store, sessionDeviceIndexKey(session.DeviceID), idHex)
		_ = removeIndexEntry(store, sessionAllIndexKey, idHex)
		_ = wipeAndDelete(store, key)
		zeroSession(&session)
		return Session{}, ErrSessionNotFound
	}
	return session, nil
}

func deleteSession(store kv.Store, id protocol.ID16) error {
	key := sessionKey(id)
	idHex := hex.EncodeToString(id[:])
	if data, exists, err := store.Get(key); err == nil && exists {
		var fs fileSession
		if json.Unmarshal(data, &fs) == nil {
			_ = removeIndexEntry(store, sessionDeviceIndexKey(fs.DeviceID), idHex)
		}
	}
	_ = removeIndexEntry(store, sessionAllIndexKey, idHex)
	return wipeAndDelete(store, key)
}

func deleteSessionsByDevice(store kv.Store, deviceID protocol.ID16) error {
	idxKey := sessionDeviceIndexKey(deviceID)
	data, exists, err := store.Get(idxKey)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil
	}
	for _, idHex := range ids {
		_ = removeIndexEntry(store, sessionAllIndexKey, idHex)
		_ = wipeAndDelete(store, "sess:"+idHex)
	}
	return store.Delete(idxKey)
}

// SweepExpiredSessions removes every session in store whose ExpiresAt has
// passed, using the "sessidx:all" secondary index rather than a full store
// scan (kv.Store exposes no iteration primitive to scan with anyway).
// Sessions also expire lazily on every getSession/Get call, so calling this
// is entirely optional; wire it into your own *time.Ticker loop (or call it
// from a cron-style job) if you want proactive expiry independent of read
// traffic, e.g. for a durable kv.FileStore holding many idle sessions.
func SweepExpiredSessions(store kv.Store) {
	data, exists, err := store.Get(sessionAllIndexKey)
	if err != nil || !exists {
		return
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil {
		return
	}
	now := time.Now()
	for _, idHex := range ids {
		raw, ok, err := store.Get("sess:" + idHex)
		if err != nil || !ok {
			continue
		}
		var fs fileSession
		if json.Unmarshal(raw, &fs) != nil {
			continue
		}
		if !fs.ExpiresAt.After(now) {
			_ = removeIndexEntry(store, sessionDeviceIndexKey(fs.DeviceID), idHex)
			_ = removeIndexEntry(store, sessionAllIndexKey, idHex)
			_ = wipeAndDelete(store, "sess:"+idHex)
		}
	}
}

// ---------------------------------------------------------------------------
// Replay operations.
//
// kv.Store's own capacity control (kv.WithMaxEntries) is a soft cap that
// evicts an arbitrary/expired entry to make room rather than ever failing, so
// it cannot reproduce the prior MemoryReplayStore/FileReplayStore behavior of
// hard-rejecting once a maxEntries ceiling is reached. checkAndStoreReplay
// therefore does not rely on kv.WithMaxEntries at all: it enforces
// maxEntries itself using kv.Store.Len() (which, for both MemoryStore and
// FileStore, also opportunistically prunes expired entries as a side effect
// of counting -- the same "sweep expired, then check capacity" sequence the
// original implementations performed by hand).
//
// Len() cannot be called from inside a Mutate callback (both kv
// implementations serialize Mutate under a lock that Len() would try to
// re-acquire, e.g. FileStore's single s.mu, deadlocking), so the
// Len-then-Mutate sequence below needs its own mutex to keep the whole
// check-and-store operation atomic across concurrent callers -- the same way
// the original implementations used a single mutex around their equivalent
// count-then-write sequence. Since checkAndStoreReplay is now a plain
// function rather than a struct method, that mutex has to be supplied by the
// caller (Transport keeps one, scoped 1:1 with its ReplayStore) instead of
// being embedded in a per-store wrapper type.
// ---------------------------------------------------------------------------

const defaultMaxReplayEntries = 250_000

func checkAndStoreReplay(store kv.Store, mu *sync.Mutex, maxEntries int, key string, expiresAt time.Time) (bool, error) {
	if maxEntries <= 0 {
		maxEntries = defaultMaxReplayEntries
	}

	mu.Lock()
	defer mu.Unlock()

	n, err := store.Len()
	if err != nil {
		return false, err
	}

	accepted := false
	capExceeded := false
	mErr := store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		if exists {
			// Live (non-expired) entry already present: replay.
			return nil, 0, false, nil
		}
		ttl := time.Until(expiresAt)
		if ttl <= 0 {
			// Already-expired marker: it would provide no future replay
			// protection, so there is no point persisting it (and no point
			// counting it against capacity either). Still accept, matching
			// the prior stores which stored it but had it immediately
			// superseded on the very next lookup.
			accepted = true
			return nil, 0, false, nil
		}
		if n >= maxEntries {
			capExceeded = true
			return nil, 0, false, nil
		}
		accepted = true
		return []byte{1}, ttl, true, nil
	})
	if mErr != nil {
		return false, mErr
	}
	if capExceeded {
		return false, errReplayCapacityExhausted
	}
	return accepted, nil
}
