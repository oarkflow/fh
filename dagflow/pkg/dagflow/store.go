package dagflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

type TaskStore interface {
	Create(*Task) error
	Save(*Task) error
	Get(taskID string) (*Task, error)
	List() []*Task
	GetIdempotency(key string) (*IdempotencyRecord, error)
	PutIdempotency(rec IdempotencyRecord) error
	AddDLQ(item DLQItem) error
	ListDLQ() []DLQItem
	DeleteDLQ(id string) error
}

type MemoryTaskStore struct {
	mu          sync.RWMutex
	tasks       map[string]*Task
	idempotency map[string]IdempotencyRecord
	dlq         map[string]DLQItem
}

func NewMemoryTaskStore() *MemoryTaskStore {
	return &MemoryTaskStore{tasks: map[string]*Task{}, idempotency: map[string]IdempotencyRecord{}, dlq: map[string]DLQItem{}}
}
func (s *MemoryTaskStore) Create(t *Task) error { return s.Save(t) }
func (s *MemoryTaskStore) Save(t *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[t.ID] = cloneTask(t)
	return nil
}
func (s *MemoryTaskStore) Get(id string) (*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("task %s not found", id)
	}
	return cloneTask(t), nil
}
func (s *MemoryTaskStore) List() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		out = append(out, cloneTask(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (s *MemoryTaskStore) GetIdempotency(key string) (*IdempotencyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.idempotency[key]
	if !ok {
		return nil, fmt.Errorf("idempotency key %s not found", key)
	}
	cp := r
	return &cp, nil
}
func (s *MemoryTaskStore) PutIdempotency(rec IdempotencyRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idempotency[rec.Key] = rec
	return nil
}
func (s *MemoryTaskStore) AddDLQ(item DLQItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlq[item.ID] = item
	return nil
}
func (s *MemoryTaskStore) ListDLQ() []DLQItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DLQItem, 0, len(s.dlq))
	for _, it := range s.dlq {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}
func (s *MemoryTaskStore) DeleteDLQ(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dlq[id]; !ok {
		return fmt.Errorf("dlq item %s not found", id)
	}
	delete(s.dlq, id)
	return nil
}

type ChainStore interface {
	Create(*ChainRun) error
	Save(*ChainRun) error
	Get(id string) (*ChainRun, error)
	List() []*ChainRun
}

type MemoryChainStore struct {
	mu   sync.RWMutex
	runs map[string]*ChainRun
}

func NewMemoryChainStore() *MemoryChainStore         { return &MemoryChainStore{runs: map[string]*ChainRun{}} }
func (s *MemoryChainStore) Create(r *ChainRun) error { return s.Save(r) }
func (s *MemoryChainStore) Save(r *ChainRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[r.ID] = cloneChainRun(r)
	return nil
}
func (s *MemoryChainStore) Get(id string) (*ChainRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.runs[id]
	if !ok {
		return nil, fmt.Errorf("chain run %s not found", id)
	}
	return cloneChainRun(r), nil
}
func (s *MemoryChainStore) List() []*ChainRun {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ChainRun, 0, len(s.runs))
	for _, r := range s.runs {
		out = append(out, cloneChainRun(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

func InputHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	var cp Task
	b, _ := json.Marshal(t)
	_ = json.Unmarshal(b, &cp)
	return &cp
}
func cloneChainRun(r *ChainRun) *ChainRun {
	if r == nil {
		return nil
	}
	var cp ChainRun
	b, _ := json.Marshal(r)
	_ = json.Unmarshal(b, &cp)
	return &cp
}
