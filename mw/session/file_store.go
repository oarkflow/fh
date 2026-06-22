package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStore stores sessions as JSON files on disk.
type FileStore struct {
	dir      string
	mu       sync.Mutex
	stopGC   chan struct{}
	stopOnce sync.Once
	initErr  error
}

// NewFileStore creates a FileStore rooted at dir.
// Pass 0 for gcInterval to disable automatic GC.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	err := os.MkdirAll(dir, 0700)
	if err == nil {
		err = os.Chmod(dir, 0700)
	}
	s := &FileStore{dir: dir, initErr: err}
	if gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(gcInterval)
	}
	return s
}

// StopGC stops the background garbage collection goroutine.
func (s *FileStore) StopGC() {
	if s.stopGC != nil {
		s.stopOnce.Do(func() { close(s.stopGC) })
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

func (s *FileStore) path(id string) string {
	if !validSessionID(id) {
		return ""
	}
	return filepath.Join(s.dir, id+".session")
}

// GC removes all expired session files.
func (s *FileStore) GC() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".session" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			continue
		}
		var sess Session
		if json.Unmarshal(data, &sess) != nil {
			continue
		}
		if !sess.ExpiresAt.IsZero() && now.After(sess.ExpiresAt) {
			os.Remove(filepath.Join(s.dir, entry.Name()))
		}
	}
}

func (s *FileStore) Get(id string) (*Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil {
		return nil, s.initErr
	}
	path := s.path(id)
	if path == "" {
		return nil, ErrInvalidSession
	}
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return nil, ErrInvalidSession
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var session Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, err
	}
	return &session, nil
}

func (s *FileStore) Set(session *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil {
		return s.initErr
	}
	if session == nil {
		return ErrInvalidSession
	}
	snapshot := session.snapshot()
	if !validSessionID(snapshot.ID) {
		return ErrInvalidSession
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".session-*")
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
	if err := os.Rename(tmpName, s.path(snapshot.ID)); err != nil {
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

func (s *FileStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initErr != nil {
		return s.initErr
	}
	path := s.path(id)
	if path == "" {
		return ErrInvalidSession
	}
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
