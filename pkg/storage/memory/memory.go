package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/fh"
)

// Store groups process-local reliability adapters. Use New().Journal,
// New().Idempotency and New().Queue in fh.ReliabilityConfig. These adapters are
// non-distributed and intentionally simple for tests, benchmarks and embedded use.
type Store struct {
	Journal     *JournalStore
	Idempotency *IdempotencyStore
	Queue       *QueueStorage
}

func New() *Store {
	return &Store{Journal: &JournalStore{}, Idempotency: NewIdempotencyStore(24 * time.Hour), Queue: NewQueueStorage()}
}

type JournalStore struct {
	mu      sync.Mutex
	entries []fh.RequestJournalEntry
}

func (s *JournalStore) Append(e fh.RequestJournalEntry) error {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}
func (s *JournalStore) Entries() []fh.RequestJournalEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fh.RequestJournalEntry, len(s.entries))
	copy(out, s.entries)
	return out
}
func (s *JournalStore) Close() error { return nil }

type IdempotencyStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	records map[string]*fh.IdempotencyRecord
}

func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &IdempotencyStore{ttl: ttl, records: map[string]*fh.IdempotencyRecord{}}
}
func (s *IdempotencyStore) Begin(key, reqHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec := s.records[key]; rec != nil {
		if !rec.ExpiresAt.IsZero() && rec.ExpiresAt.Before(now) {
			delete(s.records, key)
		} else {
			cp := cloneIdem(rec)
			if rec.RequestHash != reqHash {
				return fh.IdempotencyConflict, cp, nil
			}
			if rec.State == "completed" {
				return fh.IdempotencyReplay, cp, nil
			}
			return fh.IdempotencyProcessing, cp, nil
		}
	}
	rec := &fh.IdempotencyRecord{Key: key, RequestHash: reqHash, Method: method, Path: path, State: "processing", CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(s.ttl)}
	s.records[key] = rec
	return fh.IdempotencyNew, cloneIdem(rec), nil
}
func (s *IdempotencyStore) Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.records[key]
	if rec == nil {
		return nil
	}
	if rec.RequestHash != reqHash {
		return errors.New("memory idempotency: hash mismatch")
	}
	rec.State = "completed"
	rec.StatusCode = status
	rec.ContentType = contentType
	rec.Headers = cloneHeaders(headers)
	rec.Response = append([]byte(nil), response...)
	rec.UpdatedAt = time.Now().UTC()
	return nil
}
func (s *IdempotencyStore) Close() error { return nil }

func (s *IdempotencyStore) PurgeExpired(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for key, rec := range s.records {
		if rec != nil && !rec.ExpiresAt.IsZero() && !rec.ExpiresAt.After(now) {
			delete(s.records, key)
			purged++
		}
	}
	return purged, nil
}

type QueueStorage struct {
	mu    sync.Mutex
	jobs  map[string]*fh.QueueJob
	state map[string]string
}

func NewQueueStorage() *QueueStorage {
	return &QueueStorage{jobs: map[string]*fh.QueueJob{}, state: map[string]string{}}
}
func (s *QueueStorage) Enqueue(ctx context.Context, job *fh.QueueJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return errors.New("memory queue: nil job")
	}
	cp := cloneJob(job)
	now := time.Now().UTC()
	if cp.ID == "" {
		cp.ID = "memjob_" + now.Format("20060102150405.000000000")
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = now
	}
	cp.UpdatedAt = now
	if cp.VisibleAt.IsZero() {
		cp.VisibleAt = now
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[cp.ID] = cp
	s.state[cp.ID] = "pending"
	job.ID = cp.ID
	return nil
}
func (s *QueueStorage) Claim(ctx context.Context, now time.Time) (*fh.QueueJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]*fh.QueueJob, 0)
	for id, st := range s.state {
		if st != "pending" {
			continue
		}
		j := s.jobs[id]
		vis := j.VisibleAt
		if vis.IsZero() {
			vis = j.RunAt
		}
		if !vis.IsZero() && vis.After(now) {
			continue
		}
		list = append(list, j)
	}
	if len(list) == 0 {
		return nil, fh.ErrQueueEmpty
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Priority != list[j].Priority {
			return list[i].Priority > list[j].Priority
		}
		return list[i].VisibleAt.Before(list[j].VisibleAt)
	})
	selected := list[0]
	s.state[selected.ID] = "processing"
	return cloneJob(selected), nil
}
func (s *QueueStorage) Complete(ctx context.Context, job *fh.QueueJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = cloneJob(job)
	s.state[job.ID] = "done"
	return nil
}
func (s *QueueStorage) Retry(ctx context.Context, job *fh.QueueJob, cause error, backoff time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return nil
	}
	cp := cloneJob(job)
	cp.Attempts++
	cp.UpdatedAt = time.Now().UTC()
	cp.VisibleAt = cp.UpdatedAt.Add(backoff * time.Duration(max(1, cp.Attempts)))
	if cause != nil {
		cp.LastError = cause.Error()
	}
	if cp.MaxAttempts <= 0 {
		cp.MaxAttempts = 5
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[cp.ID] = cp
	if cp.Attempts >= cp.MaxAttempts {
		s.state[cp.ID] = "failed"
	} else {
		s.state[cp.ID] = "pending"
	}
	return nil
}
func (s *QueueStorage) Fail(ctx context.Context, job *fh.QueueJob, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return nil
	}
	cp := cloneJob(job)
	cp.UpdatedAt = time.Now().UTC()
	if cause != nil {
		cp.LastError = cause.Error()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[cp.ID] = cp
	s.state[cp.ID] = "failed"
	return nil
}
func (s *QueueStorage) Recover(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, st := range s.state {
		if st == "processing" {
			s.state[id] = "pending"
		}
	}
	return nil
}
func (s *QueueStorage) Stats(ctx context.Context) (fh.QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return fh.QueueStats{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var st fh.QueueStats
	for _, state := range s.state {
		switch state {
		case "pending":
			st.Pending++
		case "processing":
			st.Processing++
		case "done":
			st.Done++
		case "failed":
			st.Failed++
		}
	}
	return st, nil
}
func (s *QueueStorage) Close() error { return nil }

func cloneJob(j *fh.QueueJob) *fh.QueueJob {
	if j == nil {
		return nil
	}
	cp := *j
	cp.Payload = append([]byte(nil), j.Payload...)
	if j.Headers != nil {
		cp.Headers = map[string]string{}
		for k, v := range j.Headers {
			cp.Headers[k] = v
		}
	}
	return &cp
}
func cloneIdem(r *fh.IdempotencyRecord) *fh.IdempotencyRecord {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Response = append([]byte(nil), r.Response...)
	cp.Headers = cloneHeaders(r.Headers)
	return &cp
}
func cloneHeaders(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, vals := range in {
		out[k] = append([]string(nil), vals...)
	}
	return out
}

func (s *QueueStorage) ListJobs(ctx context.Context, state string, limit int) ([]fh.QueueJobSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state != "" && state != "pending" && state != "processing" && state != "done" && state != "failed" {
		return nil, fmt.Errorf("memory queue: invalid state %q", state)
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]fh.QueueJobSnapshot, 0, min(limit, len(s.jobs)))
	for id, jobState := range s.state {
		if state != "" && jobState != state {
			continue
		}
		j := s.jobs[id]
		if j == nil {
			continue
		}
		out = append(out, queueSnapshot(jobState, j))
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}

func (s *QueueStorage) RequeueFailed(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state[id] != "failed" || s.jobs[id] == nil {
		return fmt.Errorf("memory queue: failed job %q not found", id)
	}
	j := cloneJob(s.jobs[id])
	j.UpdatedAt = time.Now().UTC()
	j.VisibleAt = j.UpdatedAt
	j.LastError = ""
	s.jobs[id] = j
	s.state[id] = "pending"
	return nil
}

func (s *QueueStorage) DiscardFailed(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state[id] != "failed" {
		return fmt.Errorf("memory queue: failed job %q not found", id)
	}
	delete(s.jobs, id)
	delete(s.state, id)
	return nil
}

func (s *QueueStorage) PurgeJobs(ctx context.Context, state string, before time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if state == "" {
		state = "done"
	}
	if state != "done" && state != "failed" {
		return 0, fmt.Errorf("memory queue: only done or failed jobs can be purged, got %q", state)
	}
	if before.IsZero() {
		before = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	purged := 0
	for id, st := range s.state {
		if st != state {
			continue
		}
		j := s.jobs[id]
		if j == nil || j.UpdatedAt.After(before) {
			continue
		}
		delete(s.jobs, id)
		delete(s.state, id)
		purged++
		if purged >= limit {
			break
		}
	}
	return purged, nil
}

func queueSnapshot(state string, job *fh.QueueJob) fh.QueueJobSnapshot {
	preview := string(job.Payload)
	if len(preview) > 512 {
		preview = preview[:512] + "..."
	}
	headers := map[string]string(nil)
	if len(job.Headers) > 0 {
		headers = make(map[string]string, len(job.Headers))
		for k, v := range job.Headers {
			headers[k] = v
		}
	}
	return fh.QueueJobSnapshot{ID: job.ID, Type: job.Type, State: state, Headers: headers, Attempts: job.Attempts, MaxAttempts: job.MaxAttempts, VisibleAt: job.VisibleAt, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, LastError: job.LastError, Priority: job.Priority, RunAt: job.RunAt, ConcurrencyKey: job.ConcurrencyKey, PayloadBytes: len(job.Payload), PayloadPreview: preview}
}
