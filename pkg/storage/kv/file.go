package kv

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// defaultMaxFileEntrySize bounds how large a single stored value may be
// before FileStore refuses to persist it, as a defensive cap against
// unbounded disk usage from a single key (e.g. a cached response body).
const defaultMaxFileEntrySize = 8 << 20 // 8 MiB

// FileStore is a durable-across-restart implementation of Store. It persists
// one file per key under a root directory, named by the hex-encoded SHA-256
// digest of the key — keys handed to a Store are arbitrary strings (IPs,
// tokens, cache keys, URLs) and must never be used to build a filesystem
// path directly, since they are not validated identifiers. Hashing closes
// that off entirely and keeps filenames a fixed, bounded length.
//
// Writes are atomic: each Set writes to a temp file in the same directory,
// fsyncs it, renames it over the target path, then fsyncs the directory —
// a crash never leaves a partially-written entry. The store directory is
// created with 0700 and every entry file with 0600.
//
// FileStore trades throughput for durability: every Set and every expiring
// Get does real disk I/O. Middleware packages backing a hot request path
// with FileStore should size call volume accordingly; MemoryStore remains
// the default for exactly this reason.
type FileStore struct {
	dir           string
	maxEntrySize  int64
	mu            sync.Mutex
	closeOnce     sync.Once
	stopGC        chan struct{}
	closed        bool
	closedGuardMu sync.Mutex
}

// FileOption configures a FileStore.
type FileOption func(*fileConfig)

type fileConfig struct {
	maxEntrySize int64
	gcInterval   time.Duration
}

// WithMaxEntrySize overrides the default 8 MiB cap on a single stored value.
// Set attempts exceeding this return ErrTooLarge rather than partially
// persisting data.
func WithMaxEntrySize(n int64) FileOption {
	return func(c *fileConfig) {
		if n > 0 {
			c.maxEntrySize = n
		}
	}
}

// WithFileGCInterval starts a background goroutine that sweeps expired entry
// files every interval. 0 (default) disables background sweeping.
func WithFileGCInterval(d time.Duration) FileOption {
	return func(c *fileConfig) { c.gcInterval = d }
}

// NewFileStore creates a FileStore rooted at dir, creating it (mode 0700) if
// necessary.
func NewFileStore(dir string, opts ...FileOption) (*FileStore, error) {
	cfg := fileConfig{maxEntrySize: defaultMaxFileEntrySize}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, maxEntrySize: cfg.maxEntrySize}
	if cfg.gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(cfg.gcInterval)
	}
	return s, nil
}

func (s *FileStore) isClosed() bool {
	s.closedGuardMu.Lock()
	defer s.closedGuardMu.Unlock()
	return s.closed
}

func (s *FileStore) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:])+".kv")
}

func (s *FileStore) Get(key string) ([]byte, bool, error) {
	if s.isClosed() {
		return nil, false, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.path(key)
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > s.maxEntrySize+4096 {
		return nil, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var e entry
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, false, nil
	}
	if e.expired(time.Now()) {
		_ = os.Remove(path)
		return nil, false, nil
	}
	return e.Value, true, nil
}

func (s *FileStore) Set(key string, value []byte, ttl time.Duration) error {
	if s.isClosed() {
		return ErrClosed
	}
	if int64(len(value)) > s.maxEntrySize {
		return ErrTooLarge
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	data, err := json.Marshal(entry{Value: value, ExpiresAt: expiresAt})
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeAtomic(s.path(key), data)
}

func (s *FileStore) writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(s.dir, ".kv-*")
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
	if err := os.Rename(tmpName, path); err != nil {
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

func (s *FileStore) Mutate(key string, fn func(current []byte, exists bool) (next []byte, ttl time.Duration, ok bool, err error)) error {
	if s.isClosed() {
		return ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.path(key)
	var current []byte
	var exists bool
	if info, err := os.Lstat(path); err == nil {
		if info.Mode().IsRegular() && info.Size() <= s.maxEntrySize+4096 {
			if data, err := os.ReadFile(path); err == nil {
				var e entry
				if json.Unmarshal(data, &e) == nil && !e.expired(time.Now()) {
					current, exists = e.Value, true
				}
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	next, ttl, ok, err := fn(current, exists)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if int64(len(next)) > s.maxEntrySize {
		return ErrTooLarge
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	data, err := json.Marshal(entry{Value: next, ExpiresAt: expiresAt})
	if err != nil {
		return err
	}
	return s.writeAtomic(path, data)
}

func (s *FileStore) Delete(key string) error {
	if s.isClosed() {
		return ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.path(key))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *FileStore) Len() (int, error) {
	if s.isClosed() {
		return 0, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	count := 0
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".kv" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, de.Name()))
		if err != nil {
			continue
		}
		var e entry
		if json.Unmarshal(data, &e) != nil {
			continue
		}
		if e.expired(now) {
			_ = os.Remove(filepath.Join(s.dir, de.Name()))
			continue
		}
		count++
	}
	return count, nil
}

// GC removes all expired entry files. It is called automatically on
// WithFileGCInterval's schedule but may also be invoked directly.
func (s *FileStore) GC() {
	if s.isClosed() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	now := time.Now()
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".kv" {
			continue
		}
		p := filepath.Join(s.dir, de.Name())
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var e entry
		if json.Unmarshal(data, &e) != nil {
			continue
		}
		if e.expired(now) {
			_ = os.Remove(p)
		}
	}
}

func (s *FileStore) gcLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.GC()
		case <-s.stopGC:
			return
		}
	}
}

// Close stops any background GC goroutine. Safe to call multiple times.
func (s *FileStore) Close() error {
	s.closedGuardMu.Lock()
	s.closed = true
	s.closedGuardMu.Unlock()
	s.closeOnce.Do(func() {
		if s.stopGC != nil {
			close(s.stopGC)
		}
	})
	return nil
}
