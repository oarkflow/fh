package kv

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// defaultMaxFileEntrySize bounds how large a single stored value may be
// before FileStore refuses to persist it, as a defensive cap against
// unbounded disk usage from a single key (e.g. a cached response body).
const defaultMaxFileEntrySize = 8 << 20 // 8 MiB
const defaultMaxFileKeyBytes = 4096
const defaultMaxFileEntries = 100_000
const defaultMaxFileBytes int64 = 256 << 20

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
// Get does real disk I/O. It defaults to 100,000 entries, 256 MiB total stored
// .kv file bytes, 4 KiB per key, and 8 MiB per value. Its accounting is
// process-local; use one FileStore instance per directory and do not share a
// directory between processes. Account for disk I/O when using FileStore in a
// hot request path; MemoryStore remains the default for exactly this reason.
type FileStore struct {
	dir            string
	maxEntrySize   int64
	maxKeyBytes    int
	maxEntries     int
	maxBytes       int64
	entryCount     int
	diskBytes      int64
	lastLimitSweep time.Time
	mu             sync.Mutex
	closeOnce      sync.Once
	stopGC         chan struct{}
	gcDone         chan struct{}
	closed         bool
	closedGuardMu  sync.Mutex
}

// FileOption configures a FileStore.
type FileOption func(*fileConfig)

type fileConfig struct {
	maxEntrySize int64
	maxKeyBytes  int
	maxEntries   int
	maxBytes     int64
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

// WithMaxFileKeyBytes sets the maximum key length. The default is 4 KiB; larger
// keys require an explicit override.
func WithMaxFileKeyBytes(n int) FileOption {
	return func(c *fileConfig) {
		if n > 0 {
			c.maxKeyBytes = n
		}
	}
}

// WithMaxFileEntries sets the maximum number of stored .kv files. The default
// is 100,000; set n to 0 to disable the entry-count limit.
func WithMaxFileEntries(n int) FileOption {
	return func(c *fileConfig) {
		if n >= 0 {
			c.maxEntries = n
		}
	}
}

// WithMaxFileBytes sets the maximum total on-disk size of stored .kv files.
// The default is 256 MiB; set n to 0 to disable the total-size limit.
func WithMaxFileBytes(n int64) FileOption {
	return func(c *fileConfig) {
		if n >= 0 {
			c.maxBytes = n
		}
	}
}

// WithFileGCInterval starts a background goroutine that sweeps expired entry
// files every interval. 0 (default) disables background sweeping.
func WithFileGCInterval(d time.Duration) FileOption {
	return func(c *fileConfig) { c.gcInterval = d }
}

// NewFileStore creates a FileStore rooted at dir, creating it (mode 0700) if
// necessary. It scans existing .kv files once to initialize capacity accounting.
func NewFileStore(dir string, opts ...FileOption) (*FileStore, error) {
	cfg := fileConfig{maxEntrySize: defaultMaxFileEntrySize, maxKeyBytes: defaultMaxFileKeyBytes, maxEntries: defaultMaxFileEntries, maxBytes: defaultMaxFileBytes}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := ensurePrivateDirectory(dir); err != nil {
		return nil, err
	}
	s := &FileStore{dir: dir, maxEntrySize: cfg.maxEntrySize, maxKeyBytes: cfg.maxKeyBytes, maxEntries: cfg.maxEntries, maxBytes: cfg.maxBytes}
	if err := s.scanUsage(); err != nil {
		return nil, err
	}
	if cfg.gcInterval > 0 {
		s.stopGC = make(chan struct{})
		s.gcDone = make(chan struct{})
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

// maxEntryFileSize bounds the JSON representation of one value. encoding/json
// base64-encodes []byte, so the on-disk representation can be roughly 4/3 the
// configured value limit. The extra room covers the fixed JSON envelope and
// timestamp without rejecting values accepted by Set.
func (s *FileStore) maxEntryFileSize() int64 {
	const overhead = int64(4096)
	n := s.maxEntrySize
	if n <= 0 {
		return overhead
	}
	groups := n / 3
	if groups > (int64(^uint64(0)>>1)-overhead-4)/4 {
		return int64(^uint64(0) >> 1)
	}
	encoded := groups * 4
	if n%3 != 0 {
		encoded += 4
	}
	if encoded > int64(^uint64(0)>>1)-overhead {
		return int64(^uint64(0) >> 1)
	}
	return encoded + overhead
}

func (s *FileStore) readEntry(path string) (entry, bool, error) {
	limit := s.maxEntryFileSize()
	linkInfo, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return entry{}, false, nil
		}
		return entry{}, false, err
	}
	if !linkInfo.Mode().IsRegular() || linkInfo.Size() > limit {
		return entry{}, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return entry{}, false, nil
		}
		return entry{}, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return entry{}, false, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(linkInfo, info) || info.Size() > limit {
		return entry{}, false, nil
	}
	readLimit := limit
	if maxInt64 := int64(^uint64(0) >> 1); readLimit < maxInt64 {
		readLimit++
	}
	data, err := io.ReadAll(io.LimitReader(file, readLimit))
	if err != nil {
		return entry{}, false, err
	}
	if int64(len(data)) > limit {
		return entry{}, false, nil
	}
	var value entry
	if json.Unmarshal(data, &value) != nil || int64(len(value.Value)) > s.maxEntrySize {
		return entry{}, false, nil
	}
	return value, true, nil
}

func (s *FileStore) scanEntries(fn func(os.DirEntry) error) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(128)
		for _, de := range entries {
			if err := fn(de); err != nil {
				return err
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return nil
			}
			return readErr
		}
	}
}

// scanUsage recovers the store's accounting from disk once at startup. All
// later updates are made while mu is held, so Set and Mutate avoid directory
// scans on the hot path.
func (s *FileStore) scanUsage() error {
	err := s.scanEntries(func(de os.DirEntry) error {
		if strings.HasPrefix(de.Name(), ".kv-") {
			path := filepath.Join(s.dir, de.Name())
			info, err := os.Lstat(path)
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			return nil
		}
		if de.IsDir() || filepath.Ext(de.Name()) != ".kv" {
			return nil
		}
		info, err := os.Lstat(filepath.Join(s.dir, de.Name()))
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if s.entryCount == int(^uint(0)>>1) || info.Size() > int64(^uint64(0)>>1)-s.diskBytes {
			return errors.New("kv: file store usage counter overflow")
		}
		s.entryCount++
		s.diskBytes += info.Size()
		return nil
	})
	return err
}

// removeEntry updates usage only after a regular entry file was removed.
// Callers hold mu, which serializes this store's operations.
func (s *FileStore) removeEntry(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode().IsRegular() {
		if s.entryCount > 0 {
			s.entryCount--
		}
		if s.diskBytes >= info.Size() {
			s.diskBytes -= info.Size()
		} else {
			s.diskBytes = 0
		}
	}
	return nil
}

// sweepExpiredLocked removes expired files and updates usage. Callers hold mu.
func (s *FileStore) sweepExpiredLocked(now time.Time) {
	_ = s.scanEntries(func(de os.DirEntry) error {
		if de.IsDir() || filepath.Ext(de.Name()) != ".kv" {
			return nil
		}
		path := filepath.Join(s.dir, de.Name())
		item, ok, err := s.readEntry(path)
		if err == nil && !ok {
			// Invalid or oversized entries are not observable through Get. Remove
			// them during the existing sweep so corrupt files cannot pin the
			// process-local disk-capacity counters indefinitely.
			_ = s.removeEntry(path)
		} else if err == nil && item.expired(now) {
			_ = s.removeEntry(path)
		}
		return nil
	})
}

func (s *FileStore) Get(key string) ([]byte, bool, error) {
	if s.isClosed() {
		return nil, false, ErrClosed
	}
	if len(key) > s.maxKeyBytes {
		return nil, false, ErrKeyTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosed() {
		return nil, false, ErrClosed
	}
	path := s.path(key)
	e, ok, err := s.readEntry(path)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		_ = s.removeEntry(path)
		return nil, false, nil
	}
	if e.expired(time.Now()) {
		_ = s.removeEntry(path)
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
	if len(key) > s.maxKeyBytes {
		return ErrKeyTooLarge
	}
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	// Serialize under the store lock. File writes are serialized below anyway;
	// encoding here also bounds concurrent temporary buffers to one per store
	// instead of one per caller for values up to the configured entry limit.
	data, err := json.Marshal(entry{Value: value, ExpiresAt: expiresAt})
	if err != nil {
		return err
	}
	return s.writeAtomic(s.path(key), data)
}

func (s *FileStore) writeAtomic(path string, data []byte) error {
	newSize := int64(len(data))
	capacity := func() (int, int64, bool, error) {
		oldSize := int64(0)
		exists := false
		if info, err := os.Lstat(path); err == nil {
			if info.Mode().IsRegular() {
				exists = true
				oldSize = info.Size()
			}
		} else if !os.IsNotExist(err) {
			return 0, 0, false, err
		}
		newCount := s.entryCount
		if !exists {
			if newCount == int(^uint(0)>>1) {
				return 0, 0, false, nil
			}
			newCount++
		}
		if s.diskBytes < oldSize {
			return 0, 0, false, nil
		}
		baseBytes := s.diskBytes - oldSize
		if s.maxEntries > 0 && newCount > s.maxEntries {
			return 0, 0, false, nil
		}
		if s.maxBytes > 0 && (baseBytes > s.maxBytes || newSize > s.maxBytes-baseBytes) {
			return 0, 0, false, nil
		}
		return newCount, baseBytes, true, nil
	}
	newCount, baseBytes, allowed, err := capacity()
	if err != nil {
		return err
	}
	if !allowed && (s.lastLimitSweep.IsZero() || time.Since(s.lastLimitSweep) >= time.Second) {
		s.lastLimitSweep = time.Now()
		s.sweepExpiredLocked(s.lastLimitSweep)
		newCount, baseBytes, allowed, err = capacity()
		if err != nil {
			return err
		}
	}
	if !allowed {
		return ErrCapacityExceeded
	}
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
	s.entryCount = newCount
	if newSize > int64(^uint64(0)>>1)-baseBytes {
		s.diskBytes = int64(^uint64(0) >> 1)
	} else {
		s.diskBytes = baseBytes + newSize
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
	if len(key) > s.maxKeyBytes {
		return ErrKeyTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}

	path := s.path(key)
	var current []byte
	var exists bool
	if e, ok, err := s.readEntry(path); err == nil && ok && !e.expired(time.Now()) {
		current, exists = e.Value, true
	} else if err != nil && !os.IsNotExist(err) {
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
	if len(key) > s.maxKeyBytes {
		return ErrKeyTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	return s.removeEntry(s.path(key))
}

func (s *FileStore) Len() (int, error) {
	if s.isClosed() {
		return 0, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosed() {
		return 0, ErrClosed
	}
	now := time.Now()
	count := 0
	err := s.scanEntries(func(de os.DirEntry) error {
		if de.IsDir() || filepath.Ext(de.Name()) != ".kv" {
			return nil
		}
		p := filepath.Join(s.dir, de.Name())
		e, ok, err := s.readEntry(p)
		if err != nil {
			return nil
		}
		if !ok {
			_ = s.removeEntry(p)
			return nil
		}
		if e.expired(now) {
			_ = s.removeEntry(p)
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return count, err
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
	if s.isClosed() {
		return
	}
	s.sweepExpiredLocked(time.Now())
}

func (s *FileStore) gcLoop(interval time.Duration) {
	defer close(s.gcDone)
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
	if s.gcDone != nil {
		<-s.gcDone
	}
	// Drain operations that passed their final closed check before Close set
	// the flag. New operations recheck closed while holding mu and will not
	// touch disk after this point.
	s.mu.Lock()
	//lint:ignore SA2001 this unlock pairs with the drain barrier lock above.
	s.mu.Unlock()
	return nil
}
