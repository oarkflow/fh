package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// FileStore is a Store implementation backed by a single JSON snapshot file
// on the local filesystem. Unlike MemoryStore, its state survives process
// restarts, and unlike a purely in-process store, it lets multiple OS
// processes on the SAME host (e.g. this package's typical use with fh's
// prefork multi-process model, see prefork.go/prefork_unix.go at the repo
// root) coordinate node heartbeats and leader-election leases through a
// shared directory.
//
// Locking mechanism and its limits.
//
// Cross-process mutual exclusion is implemented with only the stdlib: a
// sibling lock file (state.json.lock) is created with
// os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600). O_EXCL makes
// file creation atomic - if the file already exists the call fails, and
// that failure is the mutual-exclusion signal. The winner writes its own
// PID and an acquisition timestamp into the lock file, performs its
// read-modify-write of the JSON snapshot, and removes the lock file in a
// defer once done. A loser backs off with short sleeps and retries for a
// bounded total wait; if that wait is exceeded it inspects the existing
// lock file's timestamp, and if it is older than the configured staleness
// threshold (meaning the previous holder most likely crashed without
// cleaning up) it forcibly removes the stale lock file and retries once.
//
// This scheme assumes a shared LOCAL filesystem with correct O_EXCL
// semantics - it is appropriate for multiple processes on the same host
// (e.g. fh's prefork workers sharing a directory). It does NOT work
// reliably over NFS or other network filesystems, where O_EXCL is not
// guaranteed to be atomic across clients; do not use FileStore for
// coordination across machines or over a network mount.
type FileStore struct {
	dir            string
	lockTimeout    time.Duration // bounded total wait while contending for the lock
	lockRetryDelay time.Duration // sleep between O_EXCL retries
	staleThreshold time.Duration // lock files older than this are considered abandoned
	now            func() time.Time
}

// FileStoreOption configures a FileStore constructed via NewFileStore.
type FileStoreOption func(*FileStore)

// WithLockTimeout sets the bounded total wait time while contending for the
// on-disk lock before a staleness check is attempted. Default 2s.
func WithLockTimeout(d time.Duration) FileStoreOption {
	return func(s *FileStore) {
		if d > 0 {
			s.lockTimeout = d
		}
	}
}

// WithStaleThreshold sets how old an existing lock file must be before it is
// considered abandoned (e.g. the previous holder crashed) and force-removed.
// Default 10s.
func WithStaleThreshold(d time.Duration) FileStoreOption {
	return func(s *FileStore) {
		if d > 0 {
			s.staleThreshold = d
		}
	}
}

// WithLockRetryDelay sets the sleep between lock-acquisition retries.
// Default 20ms.
func WithLockRetryDelay(d time.Duration) FileStoreOption {
	return func(s *FileStore) {
		if d > 0 {
			s.lockRetryDelay = d
		}
	}
}

// fileState is the full on-disk snapshot shared by every process
// coordinating through the same directory.
type fileState struct {
	Nodes  map[string]fileNodeEntry `json:"nodes"`
	Leases map[string]Lease         `json:"leases"`
}

// fileNodeEntry pairs a Node with the absolute expiry time computed from its
// last heartbeat + TTL, mirroring MemoryStore's separate nodeTTL map.
type fileNodeEntry struct {
	Node      Node      `json:"node"`
	ExpiresAt time.Time `json:"expires_at"`
}

// NewFileStore creates a Store backed by a JSON snapshot file inside dir.
// dir is created (mode 0700) if it does not already exist. Multiple
// FileStore instances (in the same or different OS processes) pointed at
// the same dir coordinate safely via the locking scheme documented on
// FileStore.
func NewFileStore(dir string, opts ...FileStoreOption) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("fh/cluster: dir required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s := &FileStore{
		dir:            dir,
		lockTimeout:    2 * time.Second,
		lockRetryDelay: 20 * time.Millisecond,
		staleThreshold: 10 * time.Second,
		now:            time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *FileStore) statePath() string { return filepath.Join(s.dir, "state.json") }
func (s *FileStore) lockPath() string  { return filepath.Join(s.dir, "state.json.lock") }

// lockPayload is written into the lock file so a stale lock can be
// identified by age; the PID is informational only (best-effort, no
// liveness check is performed against it since that would require
// platform-specific syscalls this package intentionally avoids).
type lockPayload struct {
	PID      int       `json:"pid"`
	Acquired time.Time `json:"acquired_at"`
}

// acquireLock implements the O_EXCL-based advisory lock described in the
// FileStore doc comment. It returns a release function that must be called
// (typically via defer) once the caller's read-modify-write is complete.
func (s *FileStore) acquireLock() (func(), error) {
	path := s.lockPath()
	deadline := s.now().Add(s.lockTimeout)
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			payload, _ := json.Marshal(lockPayload{PID: os.Getpid(), Acquired: s.now().UTC()})
			_, werr := f.Write(payload)
			cerr := f.Close()
			if werr != nil || cerr != nil {
				os.Remove(path)
				if werr != nil {
					return nil, werr
				}
				return nil, cerr
			}
			return func() { os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if s.now().Before(deadline) {
			time.Sleep(s.lockRetryDelay)
			continue
		}
		// Bounded wait exceeded: check whether the existing lock looks
		// abandoned, and if so, forcibly remove it and retry once.
		if s.isStale(path) {
			os.Remove(path)
			deadline = s.now().Add(s.lockTimeout)
			continue
		}
		return nil, fmt.Errorf("fh/cluster: timed out acquiring lock %s", path)
	}
}

// isStale reports whether the lock file at path is older than the
// configured staleness threshold, based on its recorded acquisition time
// (falling back to the file's mtime if the payload can't be parsed).
func (s *FileStore) isStale(path string) bool {
	data, err := os.ReadFile(path)
	if err == nil {
		var p lockPayload
		if json.Unmarshal(data, &p) == nil && !p.Acquired.IsZero() {
			return s.now().Sub(p.Acquired) > s.staleThreshold
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		// Lock file vanished between our failed create and this check;
		// treat as not stale so the caller simply retries acquisition.
		return false
	}
	return s.now().Sub(info.ModTime()) > s.staleThreshold
}

// readState loads the snapshot, treating a missing file as empty state.
func (s *FileStore) readState() (fileState, error) {
	st := fileState{Nodes: map[string]fileNodeEntry{}, Leases: map[string]Lease{}}
	data, err := os.ReadFile(s.statePath())
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return st, err
	}
	if len(data) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, err
	}
	if st.Nodes == nil {
		st.Nodes = map[string]fileNodeEntry{}
	}
	if st.Leases == nil {
		st.Leases = map[string]Lease{}
	}
	return st, nil
}

// writeState atomically persists the snapshot: write to a temp file in the
// same directory, then rename over the target (following the pattern used
// by mw/session.FileStore).
func (s *FileStore) writeState(st fileState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.statePath()); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (s *FileStore) Heartbeat(ctx context.Context, node Node, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	release, err := s.acquireLock()
	if err != nil {
		return err
	}
	defer release()

	st, err := s.readState()
	if err != nil {
		return err
	}
	now := s.now()
	if node.LastSeen.IsZero() {
		node.LastSeen = now.UTC()
	}
	st.Nodes[node.ID] = fileNodeEntry{Node: cloneNode(node), ExpiresAt: now.Add(ttl)}
	return s.writeState(st)
}

func (s *FileStore) Nodes(ctx context.Context, now time.Time) ([]Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	release, err := s.acquireLock()
	if err != nil {
		return nil, err
	}
	defer release()

	st, err := s.readState()
	if err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = s.now()
	}
	out := make([]Node, 0, len(st.Nodes))
	for _, entry := range st.Nodes {
		if entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(now) {
			out = append(out, cloneNode(entry.Node))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *FileStore) TryAcquire(ctx context.Context, name, owner string, ttl time.Duration) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	release, err := s.acquireLock()
	if err != nil {
		return Lease{}, false, err
	}
	defer release()

	st, err := s.readState()
	if err != nil {
		return Lease{}, false, err
	}
	now := s.now()
	if l, ok := st.Leases[name]; ok && l.ExpiresAt.After(now) && l.Owner != "" && l.Owner != owner {
		return l, false, nil
	}
	l := Lease{Name: name, Owner: owner, ExpiresAt: now.Add(ttl), Token: generateToken()}
	st.Leases[name] = l
	if err := s.writeState(st); err != nil {
		return Lease{}, false, err
	}
	return l, true, nil
}

func (s *FileStore) Renew(ctx context.Context, name, owner string, ttl time.Duration) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	release, err := s.acquireLock()
	if err != nil {
		return Lease{}, false, err
	}
	defer release()

	st, err := s.readState()
	if err != nil {
		return Lease{}, false, err
	}
	now := s.now()
	l := st.Leases[name]
	if l.Owner != "" && l.Owner != owner && l.ExpiresAt.After(now) {
		return l, false, nil
	}
	l = Lease{Name: name, Owner: owner, ExpiresAt: now.Add(ttl), Token: generateToken()}
	st.Leases[name] = l
	if err := s.writeState(st); err != nil {
		return Lease{}, false, err
	}
	return l, true, nil
}

func (s *FileStore) Release(ctx context.Context, name, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	release, err := s.acquireLock()
	if err != nil {
		return err
	}
	defer release()

	st, err := s.readState()
	if err != nil {
		return err
	}
	if l, ok := st.Leases[name]; ok && l.Owner == owner {
		delete(st.Leases, name)
		return s.writeState(st)
	}
	return nil
}
