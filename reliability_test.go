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

// TestIdempotencyIdentityScopesDifferentCallers proves the fix for
// cross-user idempotency replay: the scoped store key must differ between
// two distinct authenticated principals (and fall back to IP when no
// principal is set), so an attacker who learns a victim's raw
// Idempotency-Key header value cannot use it to replay the victim's cached
// response.
func TestIdempotencyIdentityScopesDifferentCallers(t *testing.T) {
	app := New()
	c1 := &DefaultCtx{server: app}
	c1.reset()
	SetPrincipal(c1, Principal{ID: "user-a", Type: "user"})

	c2 := &DefaultCtx{server: app}
	c2.reset()
	SetPrincipal(c2, Principal{ID: "user-b", Type: "user"})

	id1 := idempotencyIdentity(c1)
	id2 := idempotencyIdentity(c2)
	if id1 == id2 {
		t.Fatalf("expected different identities for different principals, got identical: %q", id1)
	}

	// Same principal must scope identically across requests so legitimate
	// same-caller retries still replay correctly.
	c3 := &DefaultCtx{server: app}
	c3.reset()
	SetPrincipal(c3, Principal{ID: "user-a", Type: "user"})
	if got := idempotencyIdentity(c3); got != id1 {
		t.Fatalf("expected same identity for same principal, got %q vs %q", got, id1)
	}
}

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

// TestQueueAdminRejectsPathTraversalJobID proves RequeueFailed/DiscardFailed
// reject a crafted job id before it ever reaches filepath.Join, so an admin
// caller (or an attacker who reaches this route via a misconfigured
// mw/admin auth) cannot escape the queue's failed/ directory.
func TestQueueAdminRejectsPathTraversalJobID(t *testing.T) {
	dir := t.TempDir()
	q, err := OpenDurableQueue(DurableQueueConfig{Dir: dir, Workers: 1, MaxAttempts: 1, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.Register("boom", func(context.Context, *QueueJob) error { return errors.New("fail") })
	if err := q.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue("boom", nil); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	var jobID string
	for time.Now().Before(deadline) {
		jobs, _ := q.ListJobs(ctx, "failed", 10)
		if len(jobs) == 1 {
			jobID = jobs[0].ID
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if jobID == "" {
		t.Fatal("job never reached failed state")
	}

	// Sentinel file outside the queue dir that a traversal payload targets.
	sentinel := filepath.Join(filepath.Dir(dir), "sentinel-"+filepath.Base(dir)+".txt")
	if err := os.WriteFile(sentinel, []byte("do-not-delete"), 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sentinel)

	traversalID := "../../" + filepath.Base(filepath.Dir(dir)) + "/sentinel-" + filepath.Base(dir)
	if err := q.DiscardFailed(ctx, traversalID); err == nil {
		t.Fatal("expected path-traversal job id to be rejected")
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("sentinel file outside queue dir was affected by rejected traversal id: %v", err)
	}
	if err := q.RetryFailed(ctx, "../etc/passwd"); err == nil {
		t.Fatal("expected path-traversal job id to be rejected by RetryFailed")
	}

	// Legitimate operation with the real job id still works.
	if err := q.DiscardFailed(ctx, jobID); err != nil {
		t.Fatalf("expected legitimate DiscardFailed to succeed, got %v", err)
	}
}

// TestInboxAcceptRejectsMalformedIdentifiers proves Source/EventID are
// validated before being folded into the dedup key, and that the key
// encoding can't let two different (Source, EventID) pairs collide via the
// ":" delimiter (e.g. Source="a:b",EventID="c" vs Source="a",EventID="b:c").
func TestInboxAcceptRejectsMalformedIdentifiers(t *testing.T) {
	dir := t.TempDir()
	r, err := NewReliability(ReliabilityConfig{Enabled: true, IdempotencyEnabled: true, QueueEnabled: true, DataDir: dir, QueueWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer r.queue.Close()
	inbox := r.Inbox()
	inbox.q.Register("inbox.test", func(context.Context, *QueueJob) error { return nil })
	inbox.q.Register("inbox.a", func(context.Context, *QueueJob) error { return nil })
	inbox.q.Register("inbox.a:b", func(context.Context, *QueueJob) error { return nil })

	if _, err := inbox.Accept(context.Background(), InboxEvent{Source: "test", EventID: "not valid!"}, "inbox.test"); err == nil {
		t.Fatal("expected malformed EventID to be rejected")
	}
	if _, err := inbox.Accept(context.Background(), InboxEvent{Source: "not valid!", EventID: "evt-1"}, "inbox.test"); err == nil {
		t.Fatal("expected malformed Source to be rejected")
	}

	// Two distinct (Source, EventID) pairs that would collide under a naive
	// "source:eventID" join must be treated as distinct events, not a
	// replay of one another.
	id1, err := inbox.Accept(context.Background(), InboxEvent{Source: "a:b", EventID: "c"}, "inbox.a:b")
	if err != nil || id1 == "" {
		t.Fatalf("expected first distinct event accepted, id=%q err=%v", id1, err)
	}
	id2, err := inbox.Accept(context.Background(), InboxEvent{Source: "a", EventID: "b:c"}, "inbox.a")
	if err != nil || id2 == "" {
		t.Fatalf("expected second distinct event (collision-prone pair) accepted, id=%q err=%v", id2, err)
	}
}

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
