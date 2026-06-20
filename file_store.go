package fasthttp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStore stores sessions as JSON files on disk.
type FileStore struct {
	dir    string
	mu     sync.Mutex
	stopGC chan struct{}
}

// NewFileStore creates a FileStore rooted at dir.
// Pass 0 for gcInterval to disable automatic GC.
func NewFileStore(dir string, gcInterval time.Duration) *FileStore {
	os.MkdirAll(dir, 0700)
	s := &FileStore{dir: dir}
	if gcInterval > 0 {
		s.stopGC = make(chan struct{})
		go s.gcLoop(gcInterval)
	}
	return s
}

// StopGC stops the background garbage collection goroutine.
func (s *FileStore) StopGC() {
	if s.stopGC != nil {
		close(s.stopGC)
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
	data, err := os.ReadFile(s.path(id))
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
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}
	return os.WriteFile(s.path(session.ID), data, 0600)
}

func (s *FileStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(s.path(id))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
