package memory

import (
	"container/list"
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
	return NewWithConfig(StoreConfig{})
}

type StoreConfig struct {
	// MaxJournalEntries caps the append-only in-memory journal. At capacity,
	// Append returns ErrJournalCapacity instead of discarding prior entries.
	MaxJournalEntries int
	// MaxJournalBytes bounds retained journal strings; default 16 MiB.
	MaxJournalBytes int64
	// MaxIdempotencyEntries caps retained idempotency records.
	MaxIdempotencyEntries int
	// MaxIdempotencyBytes bounds retained records and cached responses; default 64 MiB.
	MaxIdempotencyBytes int64
	// MaxQueueEntries caps retained jobs; new jobs evict the oldest entry.
	MaxQueueEntries int
	// MaxQueueBytes caps estimated retained job memory; default 64 MiB.
	MaxQueueBytes int64
}

func NewWithConfig(cfg StoreConfig) *Store {
	return &Store{
		Journal:     NewJournalStoreWithLimits(cfg.MaxJournalEntries, cfg.MaxJournalBytes),
		Idempotency: NewIdempotencyStoreWithLimits(24*time.Hour, cfg.MaxIdempotencyEntries, cfg.MaxIdempotencyBytes),
		Queue:       NewQueueStorageWithLimits(cfg.MaxQueueEntries, cfg.MaxQueueBytes),
	}
}

var ErrJournalCapacity = errors.New("memory journal: entry capacity reached")
var ErrQueueCapacity = errors.New("memory queue: capacity reached")
var ErrIdempotencyCapacity = errors.New("memory idempotency: capacity reached")

const defaultMaxJournalEntries = 50000
const defaultMaxJournalBytes = 16 << 20

type JournalStore struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64
	usedBytes  int64
	entries    []fh.RequestJournalEntry
}

func NewJournalStoreWithLimit(maxEntries int) *JournalStore {
	return NewJournalStoreWithLimits(maxEntries, 0)
}

func NewJournalStoreWithLimits(maxEntries int, maxBytes int64) *JournalStore {
	if maxEntries <= 0 {
		maxEntries = defaultMaxJournalEntries
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxJournalBytes
	}
	return &JournalStore{maxEntries: maxEntries, maxBytes: maxBytes}
}

func (s *JournalStore) Append(e fh.RequestJournalEntry) error {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.maxEntries <= 0 {
		s.maxEntries = defaultMaxJournalEntries
	}
	if s.maxBytes <= 0 {
		s.maxBytes = defaultMaxJournalBytes
	}
	size := journalEntryBytes(e)
	if len(s.entries) >= s.maxEntries || s.usedBytes+size > s.maxBytes {
		return ErrJournalCapacity
	}
	s.entries = append(s.entries, e)
	s.usedBytes += size
	return nil
}

func journalEntryBytes(e fh.RequestJournalEntry) int64 {
	return int64(96 + len(e.RequestID) + len(e.Event) + len(e.Method) + len(e.Path) + len(e.BodyHash) + len(e.RemoteIP))
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
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	maxBytes   int64
	usedBytes  int64
	records    map[string]*idempotencyEntry
	order      list.List
}

type idempotencyEntry struct {
	key    string
	record *fh.IdempotencyRecord
	elem   *list.Element
	size   int64
}

func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	return NewIdempotencyStoreWithLimit(ttl, 0)
}

func NewIdempotencyStoreWithLimit(ttl time.Duration, maxEntries int) *IdempotencyStore {
	return NewIdempotencyStoreWithLimits(ttl, maxEntries, 0)
}

func NewIdempotencyStoreWithLimits(ttl time.Duration, maxEntries int, maxBytes int64) *IdempotencyStore {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if maxEntries <= 0 {
		maxEntries = 50000
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	return &IdempotencyStore{ttl: ttl, maxEntries: maxEntries, maxBytes: maxBytes, records: map[string]*idempotencyEntry{}}
}

func idempotencyRecordBytes(rec *fh.IdempotencyRecord) int64 {
	if rec == nil {
		return 0
	}
	size := int64(256 + len(rec.Key) + len(rec.RequestHash) + len(rec.Method) + len(rec.Path) + len(rec.ContentType) + len(rec.Response))
	for key, values := range rec.Headers {
		for _, value := range values {
			size += int64(48 + len(key) + len(value))
		}
	}
	return size
}

func (s *IdempotencyStore) removeEntryLocked(entry *idempotencyEntry) {
	if entry == nil {
		return
	}
	if s.records[entry.key] == entry {
		delete(s.records, entry.key)
		s.usedBytes -= entry.size
	}
	s.order.Remove(entry.elem)
}

func (s *IdempotencyStore) Begin(key, reqHash, method, path string) (fh.IdempotencyDecision, *fh.IdempotencyRecord, error) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.records[key]; entry != nil {
		rec := entry.record
		if !rec.ExpiresAt.IsZero() && rec.ExpiresAt.Before(now) {
			s.removeEntryLocked(entry)
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
	size := idempotencyRecordBytes(rec)
	if size > s.maxBytes {
		return fh.IdempotencyNew, nil, ErrIdempotencyCapacity
	}
	for len(s.records) >= s.maxEntries || s.usedBytes+size > s.maxBytes {
		if oldest := s.order.Front(); oldest != nil {
			entry := oldest.Value.(*idempotencyEntry)
			s.removeEntryLocked(entry)
		} else {
			return fh.IdempotencyNew, nil, ErrIdempotencyCapacity
		}
	}
	entry := &idempotencyEntry{key: key, record: rec, size: size}
	entry.elem = s.order.PushBack(entry)
	s.records[key] = entry
	s.usedBytes += size
	return fh.IdempotencyNew, cloneIdem(rec), nil
}
func (s *IdempotencyStore) Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.records[key]
	if entry == nil {
		return nil
	}
	rec := entry.record
	if rec.RequestHash != reqHash {
		return fmt.Errorf("memory idempotency: hash mismatch for key %q", key)
	}
	candidate := *rec
	candidate.ContentType = contentType
	candidate.Headers = headers
	candidate.Response = response
	candidate.State = "completed"
	candidate.UpdatedAt = time.Now().UTC()
	size := idempotencyRecordBytes(&candidate)
	if size > s.maxBytes {
		// A response larger than the configured cache budget is still returned
		// to its caller, but retaining it would violate the memory bound.
		s.removeEntryLocked(entry)
		return nil
	}
	for s.usedBytes-entry.size+size > s.maxBytes {
		var victim *idempotencyEntry
		for elem := s.order.Front(); elem != nil; elem = elem.Next() {
			other := elem.Value.(*idempotencyEntry)
			if other != entry {
				victim = other
				break
			}
		}
		if victim == nil {
			s.removeEntryLocked(entry)
			return nil
		}
		s.removeEntryLocked(victim)
	}
	rec.State = "completed"
	rec.StatusCode = status
	rec.ContentType = contentType
	rec.Headers = cloneHeaders(headers)
	rec.Response = append([]byte(nil), response...)
	rec.UpdatedAt = candidate.UpdatedAt
	s.usedBytes += size - entry.size
	entry.size = size
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
	for _, entry := range s.records {
		rec := entry.record
		if rec != nil && !rec.ExpiresAt.IsZero() && !rec.ExpiresAt.After(now) {
			s.removeEntryLocked(entry)
			purged++
		}
	}
	return purged, nil
}

type QueueStorage struct {
	mu         sync.Mutex
	jobs       map[string]*fh.QueueJob
	state      map[string]string
	maxEntries int
	maxBytes   int64
	usedBytes  int64
	jobBytes   map[string]int64
	order      list.List
	orderByID  map[string]*list.Element
}

type queueOrderEntry struct {
	id string
}

func NewQueueStorage() *QueueStorage {
	return NewQueueStorageWithLimit(0)
}

func NewQueueStorageWithLimit(maxEntries int) *QueueStorage {
	return NewQueueStorageWithLimits(maxEntries, 0)
}

func NewQueueStorageWithLimits(maxEntries int, maxBytes int64) *QueueStorage {
	if maxEntries <= 0 {
		maxEntries = 50000
	}
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	return &QueueStorage{jobs: map[string]*fh.QueueJob{}, state: map[string]string{}, maxEntries: maxEntries, maxBytes: maxBytes, jobBytes: map[string]int64{}, orderByID: map[string]*list.Element{}}
}

func (s *QueueStorage) evictOldestLocked() bool {
	oldest := s.order.Front()
	if oldest == nil {
		return false
	}
	entry := oldest.Value.(*queueOrderEntry)
	s.usedBytes -= s.jobBytes[entry.id]
	delete(s.jobBytes, entry.id)
	delete(s.jobs, entry.id)
	delete(s.state, entry.id)
	delete(s.orderByID, entry.id)
	s.order.Remove(oldest)
	return true
}

func (s *QueueStorage) removeJobLocked(id string) {
	s.usedBytes -= s.jobBytes[id]
	delete(s.jobBytes, id)
	delete(s.jobs, id)
	delete(s.state, id)
	if elem := s.orderByID[id]; elem != nil {
		s.order.Remove(elem)
		delete(s.orderByID, id)
	}
}

func (s *QueueStorage) storeLocked(job *fh.QueueJob, state string, moveToBack bool) error {
	size := queueJobBytes(job)
	if size > s.maxBytes {
		return ErrQueueCapacity
	}
	oldSize, exists := s.jobBytes[job.ID]
	if exists {
		if s.usedBytes-oldSize+size > s.maxBytes {
			return ErrQueueCapacity
		}
		if elem := s.orderByID[job.ID]; elem != nil && moveToBack {
			s.order.Remove(elem)
			delete(s.orderByID, job.ID)
		}
		s.usedBytes += size - oldSize
	} else {
		for len(s.jobs) >= s.maxEntries || s.usedBytes+size > s.maxBytes {
			if !s.evictOldestLocked() {
				return ErrQueueCapacity
			}
		}
		s.usedBytes += size
	}
	s.jobs[job.ID] = job
	s.state[job.ID] = state
	s.jobBytes[job.ID] = size
	if s.orderByID[job.ID] == nil {
		s.orderByID[job.ID] = s.order.PushBack(&queueOrderEntry{id: job.ID})
	}
	return nil
}

func queueJobBytes(job *fh.QueueJob) int64 {
	if job == nil {
		return 0
	}
	size := int64(256 + len(job.ID) + len(job.Type) + len(job.Payload) + len(job.LastError) + len(job.ConcurrencyKey))
	for key, value := range job.Headers {
		size += int64(48 + len(key) + len(value))
	}
	return size
}

func (s *QueueStorage) Enqueue(ctx context.Context, job *fh.QueueJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return errors.New("memory queue: nil job")
	}
	cp := *job
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
	if queueJobBytes(&cp) > s.maxBytes {
		return ErrQueueCapacity
	}
	stored := cloneJob(&cp)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.storeLocked(stored, "pending", true); err != nil {
		return err
	}
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
	if queueJobBytes(job) > s.maxBytes {
		return ErrQueueCapacity
	}
	return s.storeLocked(cloneJob(job), "done", false)
}
func (s *QueueStorage) Retry(ctx context.Context, job *fh.QueueJob, cause error, backoff time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return nil
	}
	cp := *job
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
	if cp.Attempts >= cp.MaxAttempts {
		if queueJobBytes(&cp) > s.maxBytes {
			return ErrQueueCapacity
		}
		return s.storeLocked(cloneJob(&cp), "failed", false)
	} else {
		if queueJobBytes(&cp) > s.maxBytes {
			return ErrQueueCapacity
		}
		return s.storeLocked(cloneJob(&cp), "pending", false)
	}
}

func (s *QueueStorage) Defer(ctx context.Context, job *fh.QueueJob, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return nil
	}
	cp := *job
	cp.UpdatedAt = time.Now().UTC()
	if delay < 0 {
		delay = 0
	}
	cp.VisibleAt = cp.UpdatedAt.Add(delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state[job.ID] != "processing" {
		return errors.New("memory queue: job is not processing")
	}
	return s.storeLocked(cloneJob(&cp), "pending", false)
}

func (s *QueueStorage) Fail(ctx context.Context, job *fh.QueueJob, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil {
		return nil
	}
	cp := *job
	cp.UpdatedAt = time.Now().UTC()
	if cause != nil {
		cp.LastError = cause.Error()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if queueJobBytes(&cp) > s.maxBytes {
		return ErrQueueCapacity
	}
	return s.storeLocked(cloneJob(&cp), "failed", false)
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
	return s.storeLocked(j, "pending", false)
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
	s.removeJobLocked(id)
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
		s.removeJobLocked(id)
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
	return fh.SanitizeQueueJobSnapshot(fh.QueueJobSnapshot{ID: job.ID, Type: job.Type, State: state, Headers: headers, Attempts: job.Attempts, MaxAttempts: job.MaxAttempts, VisibleAt: job.VisibleAt, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, LastError: job.LastError, Priority: job.Priority, RunAt: job.RunAt, ConcurrencyKey: job.ConcurrencyKey, PayloadBytes: len(job.Payload), PayloadPreview: preview})
}
