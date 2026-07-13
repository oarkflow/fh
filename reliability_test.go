package fh

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdempotencyStoreReplayAndConflict(t *testing.T) {
	store, err := OpenIdempotencyStore(filepath.Join(t.TempDir(), "idem.jsonl"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	decision, _, err := store.Begin("key-1", "hash-a", "POST", "/orders")
	if err != nil {
		t.Fatal(err)
	}
	if decision != idemNew {
		t.Fatalf("expected new, got %v", decision)
	}
	if err := store.Complete("key-1", "hash-a", 201, "application/json", map[string][]string{"X-Test": {"ok"}}, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	decision, rec, err := store.Begin("key-1", "hash-a", "POST", "/orders")
	if err != nil {
		t.Fatal(err)
	}
	if decision != idemReplay {
		t.Fatalf("expected replay, got %v", decision)
	}
	if rec.StatusCode != 201 || string(rec.Response) != `{"ok":true}` {
		t.Fatalf("bad replay record: %#v", rec)
	}
	decision, _, err = store.Begin("key-1", "hash-b", "POST", "/orders")
	if err != nil {
		t.Fatal(err)
	}
	if decision != idemConflict {
		t.Fatalf("expected conflict, got %v", decision)
	}
}

func TestDurableQueueProcessesJobWithFileStorage(t *testing.T) {
	q, err := OpenDurableQueue(DurableQueueConfig{Dir: t.TempDir(), Workers: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var processed atomic.Int32
	q.Register("email", func(ctx context.Context, job *QueueJob) error {
		var payload map[string]string
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		if payload["to"] != "user@example.com" {
			t.Fatalf("unexpected payload: %#v", payload)
		}
		processed.Add(1)
		return nil
	})
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if _, err := q.Enqueue("email", map[string]string{"to": "user@example.com"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, _ := q.Stats()
	t.Fatalf("job was not processed; stats=%+v", st)
}

func TestDurableQueueEventLog(t *testing.T) {
	dir := t.TempDir()
	q, err := OpenDurableQueue(DurableQueueConfig{Dir: dir, Workers: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.Register("event_test", func(ctx context.Context, job *QueueJob) error { return nil })
	if _, err := q.Enqueue("event_test", map[string]string{"hello": "world"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
		if strings.Contains(string(b), "enqueued") && strings.Contains(string(b), "completed") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	t.Fatalf("queue event log was not updated: %s", b)
}

func TestRequestJournalAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	j, err := OpenRequestJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(RequestJournalEntry{RequestID: "req_test", Event: "received"}); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		t.Fatal("empty journal")
	}
}

type memoryQueueStorage struct {
	mu           sync.Mutex
	pending      []*QueueJob
	processing   map[string]*QueueJob
	done, failed int
}

func newMemoryQueueStorage() *memoryQueueStorage {
	return &memoryQueueStorage{processing: map[string]*QueueJob{}}
}
func (m *memoryQueueStorage) Enqueue(ctx context.Context, j *QueueJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *j
	cp.Payload = append([]byte(nil), j.Payload...)
	m.pending = append(m.pending, &cp)
	return nil
}
func (m *memoryQueueStorage) Claim(ctx context.Context, now time.Time) (*QueueJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) == 0 {
		return nil, ErrQueueEmpty
	}
	j := m.pending[0]
	m.pending = m.pending[1:]
	m.processing[j.ID] = j
	return j, nil
}
func (m *memoryQueueStorage) Complete(ctx context.Context, j *QueueJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.processing, j.ID)
	m.done++
	return nil
}
func (m *memoryQueueStorage) Retry(ctx context.Context, j *QueueJob, err error, d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j.Attempts++
	if j.Attempts >= j.MaxAttempts {
		delete(m.processing, j.ID)
		m.failed++
		return nil
	}
	delete(m.processing, j.ID)
	m.pending = append(m.pending, j)
	return nil
}
func (m *memoryQueueStorage) Fail(ctx context.Context, j *QueueJob, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.processing, j.ID)
	m.failed++
	return nil
}
func (m *memoryQueueStorage) Recover(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, j := range m.processing {
		delete(m.processing, id)
		m.pending = append(m.pending, j)
	}
	return nil
}
func (m *memoryQueueStorage) Stats(ctx context.Context) (QueueStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return QueueStats{Pending: len(m.pending), Processing: len(m.processing), Done: m.done, Failed: m.failed}, nil
}
func (m *memoryQueueStorage) Close() error { return nil }

func TestDurableQueueUsesQueueStorageInterface(t *testing.T) {
	store := newMemoryQueueStorage()
	q := NewDurableQueue(DurableQueueConfig{Workers: 1, PollInterval: 10 * time.Millisecond}, store)
	var processed atomic.Int32
	q.Register("job", func(context.Context, *QueueJob) error { processed.Add(1); return nil })
	if _, err := q.Enqueue("job", map[string]string{"x": "y"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if processed.Load() == 1 {
			st, _ := q.Stats()
			if st.Done != 1 {
				t.Fatalf("expected done=1 got %+v", st)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("interface-backed queue did not process")
}

func TestDurableQueueRetriesAndFailsThroughStorageInterface(t *testing.T) {
	store := newMemoryQueueStorage()
	q := NewDurableQueue(DurableQueueConfig{Workers: 1, MaxAttempts: 2, PollInterval: 10 * time.Millisecond, Backoff: time.Millisecond}, store)
	q.Register("fail", func(context.Context, *QueueJob) error { return errors.New("boom") })
	if _, err := q.Enqueue("fail", nil); err != nil {
		t.Fatal(err)
	}
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		st, _ := q.Stats()
		if st.Failed == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, _ := q.Stats()
	t.Fatalf("expected failed job, got %+v", st)
}
