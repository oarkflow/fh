package dagflow

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type ExtendedStore interface {
	TaskStore
	SaveOutbox(OutboxEvent) error
	ListOutbox() []OutboxEvent
	UpdateOutbox(OutboxEvent) error
	CreateLease(WorkerLease) error
	HeartbeatLease(id string, extend time.Duration) error
	ExpireLeases(now time.Time) []WorkerLease
	ListLeases() []WorkerLease
	DeleteLease(id string) error
	SaveSnapshot(WorkflowSnapshot) error
	ListSnapshots(workflowID string) []WorkflowSnapshot
}

type durableState struct {
	Tasks       map[string]*Task              `json:"tasks"`
	Chains      map[string]*ChainRun          `json:"chains"`
	Idempotency map[string]IdempotencyRecord  `json:"idempotency"`
	DLQ         map[string]DLQItem            `json:"dlq"`
	Outbox      map[string]OutboxEvent        `json:"outbox"`
	Leases      map[string]WorkerLease        `json:"leases"`
	Snapshots   map[string][]WorkflowSnapshot `json:"snapshots"`
}

type FileStore struct {
	mu    sync.RWMutex
	path  string
	state durableState
}

func NewFileStore(path string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	s := &FileStore{path: path, state: durableState{Tasks: map[string]*Task{}, Chains: map[string]*ChainRun{}, Idempotency: map[string]IdempotencyRecord{}, DLQ: map[string]DLQItem{}, Outbox: map[string]OutboxEvent{}, Leases: map[string]WorkerLease{}, Snapshots: map[string][]WorkflowSnapshot{}}}
	if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
		_ = json.Unmarshal(b, &s.state)
		s.ensure()
	}
	return s, nil
}
func (s *FileStore) ensure() {
	if s.state.Tasks == nil {
		s.state.Tasks = map[string]*Task{}
	}
	if s.state.Chains == nil {
		s.state.Chains = map[string]*ChainRun{}
	}
	if s.state.Idempotency == nil {
		s.state.Idempotency = map[string]IdempotencyRecord{}
	}
	if s.state.DLQ == nil {
		s.state.DLQ = map[string]DLQItem{}
	}
	if s.state.Outbox == nil {
		s.state.Outbox = map[string]OutboxEvent{}
	}
	if s.state.Leases == nil {
		s.state.Leases = map[string]WorkerLease{}
	}
	if s.state.Snapshots == nil {
		s.state.Snapshots = map[string][]WorkflowSnapshot{}
	}
}
func (s *FileStore) persistLocked() error {
	tmp := s.path + ".tmp"
	b, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileStore) Create(t *Task) error { return s.Save(t) }
func (s *FileStore) Save(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Tasks[t.ID] = cloneTask(t)
	return s.persistLocked()
}
func (s *FileStore) Get(id string) (*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := s.state.Tasks[id]
	if t == nil {
		return nil, fmt.Errorf("task %s not found", id)
	}
	return cloneTask(t), nil
}
func (s *FileStore) List() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Task, 0, len(s.state.Tasks))
	for _, t := range s.state.Tasks {
		out = append(out, cloneTask(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (s *FileStore) GetIdempotency(key string) (*IdempotencyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.state.Idempotency[key]
	if !ok {
		return nil, fmt.Errorf("idempotency key %s not found", key)
	}
	return &r, nil
}
func (s *FileStore) PutIdempotency(rec IdempotencyRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Idempotency[rec.Key] = rec
	return s.persistLocked()
}
func (s *FileStore) AddDLQ(item DLQItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.DLQ[item.ID] = item
	return s.persistLocked()
}
func (s *FileStore) ListDLQ() []DLQItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DLQItem, 0, len(s.state.DLQ))
	for _, v := range s.state.DLQ {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (s *FileStore) DeleteDLQ(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.state.DLQ[id]; !ok {
		return fmt.Errorf("dlq item %s not found", id)
	}
	delete(s.state.DLQ, id)
	return s.persistLocked()
}

func (s *FileStore) SaveOutbox(ev OutboxEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if ev.ID == "" {
		ev.ID = newID("outbox")
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = now
	}
	ev.UpdatedAt = now
	if ev.Status == "" {
		ev.Status = "pending"
	}
	s.state.Outbox[ev.ID] = ev
	return s.persistLocked()
}
func (s *FileStore) UpdateOutbox(ev OutboxEvent) error { return s.SaveOutbox(ev) }
func (s *FileStore) ListOutbox() []OutboxEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]OutboxEvent, 0, len(s.state.Outbox))
	for _, v := range s.state.Outbox {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (s *FileStore) CreateLease(l WorkerLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.ID == "" {
		l.ID = newID("lease")
	}
	if l.BeatAt.IsZero() {
		l.BeatAt = time.Now()
	}
	s.state.Leases[l.ID] = l
	return s.persistLocked()
}
func (s *FileStore) HeartbeatLease(id string, extend time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.state.Leases[id]
	if !ok {
		return fmt.Errorf("lease %s not found", id)
	}
	l.BeatAt = time.Now()
	l.ExpiresAt = l.BeatAt.Add(extend)
	s.state.Leases[id] = l
	return s.persistLocked()
}
func (s *FileStore) ExpireLeases(now time.Time) []WorkerLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []WorkerLease
	for id, l := range s.state.Leases {
		if now.After(l.ExpiresAt) {
			out = append(out, l)
			delete(s.state.Leases, id)
		}
	}
	_ = s.persistLocked()
	return out
}
func (s *FileStore) DeleteLease(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state.Leases, id)
	return s.persistLocked()
}
func (s *FileStore) ListLeases() []WorkerLease {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]WorkerLease, 0, len(s.state.Leases))
	for _, v := range s.state.Leases {
		out = append(out, v)
	}
	return out
}
func (s *FileStore) SaveSnapshot(sn WorkflowSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Snapshots[sn.WorkflowID] = append(s.state.Snapshots[sn.WorkflowID], sn)
	return s.persistLocked()
}
func (s *FileStore) ListSnapshots(id string) []WorkflowSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := append([]WorkflowSnapshot(nil), s.state.Snapshots[id]...)
	return out
}

// ChainStore adapter wraps FileStore because TaskStore and ChainStore both use Create/Save with different argument types.

type FileChainStore struct{ fs *FileStore }

func (s FileChainStore) Create(r *ChainRun) error {
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	s.fs.state.Chains[r.ID] = cloneChainRun(r)
	return s.fs.persistLocked()
}
func (s FileChainStore) Save(r *ChainRun) error { return s.Create(r) }
func (s FileChainStore) Get(id string) (*ChainRun, error) {
	s.fs.mu.RLock()
	defer s.fs.mu.RUnlock()
	r := s.fs.state.Chains[id]
	if r == nil {
		return nil, fmt.Errorf("chain run %s not found", id)
	}
	return cloneChainRun(r), nil
}
func (s FileChainStore) List() []*ChainRun {
	s.fs.mu.RLock()
	defer s.fs.mu.RUnlock()
	out := make([]*ChainRun, 0, len(s.fs.state.Chains))
	for _, r := range s.fs.state.Chains {
		out = append(out, cloneChainRun(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (s *FileStore) ChainStore() ChainStore { return FileChainStore{fs: s} }
