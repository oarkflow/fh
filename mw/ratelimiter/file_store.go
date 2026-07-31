package ratelimiter

import (
	"encoding/json"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// fileWindowState is the on-disk representation of a single key's
// fixed-window counter state, persisted via kv.FileStore.
type fileWindowState struct {
	Count       int       `json:"count"`
	WindowStart time.Time `json:"window_start"`
	ResetAt     time.Time `json:"reset_at"`
}

// FileStore is a file-backed, fixed-window rate limiter Store. Each key's
// counter state is persisted by the shared kv.FileStore primitive (one JSON
// file per key, named after a hash of the key, written atomically), so
// restarts do not reset limits and arbitrary key bytes never touch the
// filesystem path directly.
type FileStore struct {
	kv      *kv.FileStore
	initErr error
}

// NewFileStore creates a FileStore rooted at dir. Pass 0 for gcInterval to
// disable automatic background GC.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	var opts []kv.FileOption
	if gcInterval > 0 {
		opts = append(opts, kv.WithFileGCInterval(gcInterval))
	}
	store, err := kv.NewFileStore(dir, opts...)
	return &FileStore{kv: store, initErr: err}
}

// StopGC stops the background garbage collection goroutine and releases the
// underlying store's resources. Safe to call multiple times.
func (s *FileStore) StopGC() {
	if s.kv != nil {
		_ = s.kv.Close()
	}
}

// GC removes bucket files whose window has already expired.
func (s *FileStore) GC() {
	if s.kv != nil {
		s.kv.GC()
	}
}

// Allow implements Store using a simple fixed-window-per-file scheme: one
// file per hashed key holds the current window's count and bounds. When the
// window has elapsed, the counter resets.
func (s *FileStore) Allow(key string, limit int, window time.Duration, now time.Time) (Result, error) {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Minute
	}
	if s.initErr != nil {
		return Result{}, s.initErr
	}

	var result Result
	err := s.kv.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var st fileWindowState
		if exists {
			if jerr := json.Unmarshal(current, &st); jerr != nil {
				exists = false
			}
		}
		if !exists || !now.Before(st.ResetAt) {
			st = fileWindowState{WindowStart: now, ResetAt: now.Add(window)}
		}

		st.Count++
		used := st.Count
		resetAt := st.ResetAt

		remaining := limit - used
		if remaining < 0 {
			remaining = 0
		}

		allowed := used <= limit
		retryAfter := time.Duration(0)
		if !allowed {
			retryAfter = resetAt.Sub(now)
			if retryAfter < time.Second {
				retryAfter = time.Second
			}
		}

		result = Result{
			Allowed:    allowed,
			Limit:      limit,
			Remaining:  remaining,
			Used:       used,
			ResetAt:    resetAt,
			RetryAfter: retryAfter,
		}

		data, merr := json.Marshal(st)
		if merr != nil {
			return nil, 0, false, merr
		}
		ttl := resetAt.Sub(now)
		if ttl <= 0 {
			ttl = window
		}
		return data, ttl, true, nil
	})
	if err != nil {
		return Result{}, err
	}
	return result, nil
}
