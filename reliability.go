package fh

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	HeaderRequestID      = "X-Request-ID"
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderReplayed       = "X-Idempotency-Replayed"
)

var ErrQueueEmpty = errors.New("fh: queue empty")

type ReliabilityConfig struct {
	Enabled bool
	DataDir string

	JournalEnabled     bool
	IdempotencyEnabled bool
	QueueEnabled       bool

	// JournalStore, IdempotencyRepository and QueueStorage allow applications to
	// replace the default file-backed persistence with SQLite, PostgreSQL, Redis,
	// cloud storage, or any other durable backend. When nil, fh uses the built-in
	// file/directory backend under DataDir.
	JournalStore          RequestJournalStore
	IdempotencyRepository IdempotencyRepository
	QueueStorage          QueueStorage

	RequestIDHeader string

	IdempotencyHeader            string
	RequireIdempotencyKey        bool
	IdempotencyTTL               time.Duration
	IdempotencyProcessingStatus  int
	IdempotencyReplayHeaderValue string

	QueueDir          string
	QueueWorkers      int
	QueueMaxAttempts  int
	QueuePollInterval time.Duration
	QueueBackoff      time.Duration
}

// RequestJournalStore is the durable append-only request lifecycle store.
// Implement this interface for DBMS-backed audit tables.
type RequestJournalStore interface {
	Append(RequestJournalEntry) error
	Close() error
}

// IdempotencyRepository stores request hashes and completed response snapshots.
// Implementations must make Begin atomic for a given key.
type IdempotencyRepository interface {
	Begin(key, reqHash, method, path string) (IdempotencyDecision, *IdempotencyRecord, error)
	Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error
	Close() error
}

// QueueStorage is the persistence contract used by DurableQueue. A DBMS backend
// should implement Claim atomically, for example using SELECT ... FOR UPDATE SKIP
// LOCKED or an UPDATE ... RETURNING lease pattern.
type QueueStorage interface {
	Enqueue(context.Context, *QueueJob) error
	Claim(context.Context, time.Time) (*QueueJob, error)
	Complete(context.Context, *QueueJob) error
	Retry(context.Context, *QueueJob, error, time.Duration) error
	Fail(context.Context, *QueueJob, error) error
	Recover(context.Context) error
	Stats(context.Context) (QueueStats, error)
	Close() error
}

// Queue is the application-facing async job queue contract.
type Queue interface {
	Register(jobType string, handler QueueHandler)
	Enqueue(jobType string, payload any, headers ...map[string]string) (string, error)
	Start() error
	Close() error
	Stats() (QueueStats, error)
}

type Reliability struct {
	cfg     ReliabilityConfig
	journal RequestJournalStore
	idem    IdempotencyRepository
	queue   *DurableQueue
}

func NewReliability(cfg ReliabilityConfig) (*Reliability, error) {
	cfg = normalizeReliabilityConfig(cfg)
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	r := &Reliability{cfg: cfg}
	var err error
	if cfg.JournalEnabled {
		if cfg.JournalStore != nil {
			r.journal = cfg.JournalStore
		} else {
			r.journal, err = OpenRequestJournal(filepath.Join(cfg.DataDir, "request-journal.jsonl"))
			if err != nil {
				return nil, err
			}
		}
	}
	if cfg.IdempotencyEnabled {
		if cfg.IdempotencyRepository != nil {
			r.idem = cfg.IdempotencyRepository
		} else {
			r.idem, err = OpenIdempotencyStore(filepath.Join(cfg.DataDir, "idempotency.jsonl"), cfg.IdempotencyTTL)
			if err != nil {
				r.Close()
				return nil, err
			}
		}
	}
	if cfg.QueueEnabled {
		storage := cfg.QueueStorage
		if storage == nil {
			qdir := cfg.QueueDir
			if qdir == "" {
				qdir = filepath.Join(cfg.DataDir, "queue")
			}
			storage, err = OpenFileQueueStorage(FileQueueStorageConfig{Dir: qdir})
			if err != nil {
				r.Close()
				return nil, err
			}
		}
		r.queue = NewDurableQueue(DurableQueueConfig{Workers: cfg.QueueWorkers, MaxAttempts: cfg.QueueMaxAttempts, PollInterval: cfg.QueuePollInterval, Backoff: cfg.QueueBackoff}, storage)
		if err := r.queue.Recover(); err != nil {
			r.Close()
			return nil, err
		}
	}
	return r, nil
}

func normalizeReliabilityConfig(cfg ReliabilityConfig) ReliabilityConfig {
	if cfg.DataDir == "" {
		cfg.DataDir = ".fh-reliability"
	}
	if !cfg.JournalEnabled && !cfg.IdempotencyEnabled && !cfg.QueueEnabled {
		cfg.JournalEnabled = true
		cfg.IdempotencyEnabled = true
		cfg.QueueEnabled = true
	}
	if cfg.RequestIDHeader == "" {
		cfg.RequestIDHeader = HeaderRequestID
	}
	if cfg.IdempotencyHeader == "" {
		cfg.IdempotencyHeader = HeaderIdempotencyKey
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	if cfg.IdempotencyProcessingStatus == 0 {
		cfg.IdempotencyProcessingStatus = StatusConflict
	}
	if cfg.IdempotencyReplayHeaderValue == "" {
		cfg.IdempotencyReplayHeaderValue = "true"
	}
	if cfg.QueueWorkers <= 0 {
		cfg.QueueWorkers = 1
	}
	if cfg.QueueMaxAttempts <= 0 {
		cfg.QueueMaxAttempts = 5
	}
	if cfg.QueuePollInterval <= 0 {
		cfg.QueuePollInterval = 250 * time.Millisecond
	}
	if cfg.QueueBackoff <= 0 {
		cfg.QueueBackoff = time.Second
	}
	return cfg
}

func (r *Reliability) Start() error {
	if r == nil || r.queue == nil {
		return nil
	}
	return r.queue.Start()
}
func (r *Reliability) Close() error {
	if r == nil {
		return nil
	}
	var first error
	if r.queue != nil {
		if err := r.queue.Close(); err != nil && first == nil {
			first = err
		}
	}
	if r.idem != nil {
		if err := r.idem.Close(); err != nil && first == nil {
			first = err
		}
	}
	if r.journal != nil {
		if err := r.journal.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
func (r *Reliability) Queue() *DurableQueue {
	if r == nil {
		return nil
	}
	return r.queue
}
func (r *Reliability) Journal() RequestJournalStore {
	if r == nil {
		return nil
	}
	return r.journal
}
func (r *Reliability) IdempotencyStore() IdempotencyRepository {
	if r == nil {
		return nil
	}
	return r.idem
}

func (r *Reliability) Middleware() HandlerFunc {
	return func(c *Ctx) error {
		requestID := c.Get(r.cfg.RequestIDHeader)
		if !validExternalID(requestID) {
			requestID = newRequestID()
		}
		c.Set(r.cfg.RequestIDHeader, requestID)
		c.Locals("request_id", requestID)

		if r.journal != nil {
			meta := RequestJournalEntry{RequestID: requestID, Event: "received", Method: c.Method(), Path: c.Path(), BodyHash: hashBody(c.body), RemoteIP: c.IP(), Time: time.Now().UTC()}
			_ = r.journal.Append(meta)
			c.OnBeforeResponse(func(ctx *Ctx) error {
				return r.journal.Append(RequestJournalEntry{RequestID: requestID, Event: "completed", Method: ctx.Method(), Path: ctx.Path(), Status: ctx.StatusCode(), BodyHash: hashBody(ctx.ResponseBody()), Time: time.Now().UTC()})
			})
		}

		if r.idem != nil && isUnsafeMethod(c.Header.Method) {
			key := c.Get(r.cfg.IdempotencyHeader)
			if key == "" {
				if r.cfg.RequireIdempotencyKey {
					return c.Status(StatusBadRequest).JSON(Map{"error": "missing_idempotency_key", "request_id": requestID})
				}
				return c.Next()
			}
			if !validExternalID(key) {
				return c.Status(StatusBadRequest).JSON(Map{"error": "invalid_idempotency_key", "request_id": requestID})
			}
			reqHash := hashRequest(c.Header.Method, c.path(), c.body)
			decision, rec, err := r.idem.Begin(key, reqHash, c.Method(), c.Path())
			if err != nil {
				return err
			}
			switch decision {
			case idemReplay:
				c.Set(HeaderReplayed, r.cfg.IdempotencyReplayHeaderValue)
				for k, values := range rec.Headers {
					for _, v := range values {
						setReplayHeader(c, k, v)
					}
				}
				if len(rec.ContentType) > 0 {
					c.Type(rec.ContentType)
				}
				return c.Status(rec.StatusCode).SendBytes(rec.Response)
			case idemConflict:
				return c.Status(StatusConflict).JSON(Map{"error": "idempotency_key_reused_with_different_payload", "request_id": requestID})
			case idemProcessing:
				return c.Status(r.cfg.IdempotencyProcessingStatus).JSON(Map{"error": "idempotency_key_processing", "request_id": requestID})
			}
			c.OnBeforeResponse(func(ctx *Ctx) error {
				return r.idem.Complete(key, reqHash, ctx.StatusCode(), string(ctx.contentType), ctx.GetRespHeaders(), ctx.ResponseBody())
			})
		}
		return c.Next()
	}
}

func isUnsafeMethod(m []byte) bool {
	return bytesEqualFold(m, MethodPOSTBytes) || bytesEqualFold(m, MethodPUTBytes) || bytesEqualFold(m, MethodPATCHBytes) || bytesEqualFold(m, MethodDELETEBytes)
}
func setReplayHeader(c *Ctx, k, v string) {
	if strings.EqualFold(k, HeaderContentLength) || strings.EqualFold(k, HeaderConnection) || strings.EqualFold(k, HeaderTransferEncoding) || strings.EqualFold(k, HeaderDate) || strings.EqualFold(k, "Trailer") {
		return
	}
	if strings.EqualFold(k, HeaderContentType) {
		c.Type(v)
		return
	}
	c.Set(k, v)
}
func hashBody(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }
func hashRequest(method, path, body []byte) string {
	h := sha256.New()
	h.Write(method)
	h.Write([]byte{' '})
	h.Write(path)
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
func validExternalID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == ':' {
			continue
		}
		return false
	}
	return true
}
func newRequestID() string {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "req_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "req_" + hex.EncodeToString(b[:])
}

type RequestJournalEntry struct {
	RequestID string    `json:"request_id"`
	Event     string    `json:"event"`
	Method    string    `json:"method,omitempty"`
	Path      string    `json:"path,omitempty"`
	Status    int       `json:"status,omitempty"`
	BodyHash  string    `json:"body_hash,omitempty"`
	RemoteIP  string    `json:"remote_ip,omitempty"`
	Time      time.Time `json:"time"`
}

type RequestJournal struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func OpenRequestJournal(path string) (*RequestJournal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &RequestJournal{file: f, path: path}, nil
}
func (j *RequestJournal) Append(e RequestJournalEntry) error {
	if j == nil {
		return nil
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err = j.file.Write(append(b, '\n')); err != nil {
		return err
	}
	return j.file.Sync()
}
func (j *RequestJournal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	return j.file.Close()
}
func (j *RequestJournal) Path() string {
	if j == nil {
		return ""
	}
	return j.path
}

// IdempotencyDecision describes how an idempotency repository resolved a request.
type IdempotencyDecision uint8

const (
	IdempotencyNew IdempotencyDecision = iota
	IdempotencyReplay
	IdempotencyConflict
	IdempotencyProcessing

	// Backward-compatible package-internal aliases.
	idemNew        = IdempotencyNew
	idemReplay     = IdempotencyReplay
	idemConflict   = IdempotencyConflict
	idemProcessing = IdempotencyProcessing
)

type IdempotencyRecord struct {
	Key         string              `json:"key"`
	RequestHash string              `json:"request_hash"`
	Method      string              `json:"method,omitempty"`
	Path        string              `json:"path,omitempty"`
	State       string              `json:"state"`
	StatusCode  int                 `json:"status_code,omitempty"`
	ContentType string              `json:"content_type,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Response    []byte              `json:"response,omitempty"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
	ExpiresAt   time.Time           `json:"expires_at"`
}

type IdempotencyStore struct {
	mu      sync.Mutex
	file    *os.File
	path    string
	ttl     time.Duration
	records map[string]*IdempotencyRecord
}

func OpenIdempotencyStore(path string, ttl time.Duration) (*IdempotencyStore, error) {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	s := &IdempotencyStore{path: path, ttl: ttl, records: make(map[string]*IdempotencyRecord)}
	_ = s.load()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	s.file = f
	return s, nil
}
func (s *IdempotencyStore) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		return nil
	}
	defer f.Close()
	now := time.Now()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 16*1024*1024)
	for sc.Scan() {
		var rec IdempotencyRecord
		if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Key != "" && (rec.ExpiresAt.IsZero() || rec.ExpiresAt.After(now)) {
			cp := rec
			s.records[rec.Key] = &cp
		}
	}
	return sc.Err()
}
func (s *IdempotencyStore) Begin(key, reqHash, method, path string) (IdempotencyDecision, *IdempotencyRecord, error) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec := s.records[key]; rec != nil {
		if !rec.ExpiresAt.IsZero() && rec.ExpiresAt.Before(now) {
			delete(s.records, key)
		} else {
			if rec.RequestHash != reqHash {
				return idemConflict, rec, nil
			}
			if rec.State == "completed" {
				return idemReplay, cloneIdem(rec), nil
			}
			return idemProcessing, cloneIdem(rec), nil
		}
	}
	rec := &IdempotencyRecord{Key: key, RequestHash: reqHash, Method: method, Path: path, State: "processing", CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(s.ttl)}
	s.records[key] = rec
	return idemNew, cloneIdem(rec), s.appendLocked(rec)
}
func (s *IdempotencyStore) Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.records[key]
	if rec == nil {
		return nil
	}
	if rec.RequestHash != reqHash {
		return errors.New("idempotency hash mismatch")
	}
	rec.State = "completed"
	rec.StatusCode = status
	rec.ContentType = contentType
	rec.Headers = cleanReplayHeaders(headers)
	rec.Response = append(rec.Response[:0], response...)
	rec.UpdatedAt = now
	return s.appendLocked(rec)
}
func cleanReplayHeaders(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, v := range in {
		if strings.EqualFold(k, HeaderContentLength) || strings.EqualFold(k, HeaderConnection) || strings.EqualFold(k, HeaderTransferEncoding) || strings.EqualFold(k, HeaderDate) {
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	return out
}
func (s *IdempotencyStore) appendLocked(rec *IdempotencyRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err = s.file.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.file.Sync()
}
func cloneIdem(r *IdempotencyRecord) *IdempotencyRecord {
	if r == nil {
		return nil
	}
	cp := *r
	cp.Response = append([]byte(nil), r.Response...)
	if r.Headers != nil {
		cp.Headers = cleanReplayHeaders(r.Headers)
	}
	return &cp
}
func (s *IdempotencyStore) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	return s.file.Close()
}
func (s *IdempotencyStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

type DurableQueueConfig struct {
	Dir          string
	Workers      int
	MaxAttempts  int
	PollInterval time.Duration
	Backoff      time.Duration
}
type QueueJob struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attempts    int               `json:"attempts"`
	MaxAttempts int               `json:"max_attempts"`
	VisibleAt   time.Time         `json:"visible_at"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	LastError   string            `json:"last_error,omitempty"`
}
type QueueHandler func(context.Context, *QueueJob) error
type QueueEvent struct {
	ID        string    `json:"id"`
	JobID     string    `json:"job_id"`
	Type      string    `json:"type"`
	State     string    `json:"state"`
	Event     string    `json:"event"`
	Attempts  int       `json:"attempts"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
type QueueStats struct{ Pending, Processing, Done, Failed int }

type DurableQueue struct {
	cfg      DurableQueueConfig
	store    QueueStorage
	mu       sync.RWMutex
	handlers map[string]QueueHandler
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	started  bool
}

func OpenDurableQueue(cfg DurableQueueConfig) (*DurableQueue, error) {
	if cfg.Dir == "" {
		cfg.Dir = ".fh-reliability/queue"
	}
	storage, err := OpenFileQueueStorage(FileQueueStorageConfig{Dir: cfg.Dir})
	if err != nil {
		return nil, err
	}
	q := NewDurableQueue(cfg, storage)
	return q, q.Recover()
}
func NewDurableQueue(cfg DurableQueueConfig, storage QueueStorage) *DurableQueue {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = time.Second
	}
	if storage == nil {
		panic("fh: durable queue requires storage")
	}
	return &DurableQueue{cfg: cfg, store: storage, handlers: make(map[string]QueueHandler)}
}
func (q *DurableQueue) Storage() QueueStorage {
	if q == nil {
		return nil
	}
	return q.store
}
func (q *DurableQueue) Recover() error {
	if q == nil || q.store == nil {
		return nil
	}
	return q.store.Recover(context.Background())
}
func (q *DurableQueue) Register(jobType string, handler QueueHandler) {
	if jobType == "" || handler == nil {
		panic("fh: queue handler requires type and handler")
	}
	q.mu.Lock()
	q.handlers[jobType] = handler
	q.mu.Unlock()
}
func (q *DurableQueue) Enqueue(jobType string, payload any, headers ...map[string]string) (string, error) {
	if q == nil {
		return "", errors.New("fh: durable queue is nil")
	}
	var raw []byte
	var err error
	switch v := payload.(type) {
	case nil:
		raw = nil
	case []byte:
		raw = append([]byte(nil), v...)
	case json.RawMessage:
		raw = append([]byte(nil), v...)
	default:
		raw, err = json.Marshal(v)
		if err != nil {
			return "", err
		}
	}
	now := time.Now().UTC()
	job := &QueueJob{ID: newQueueID(), Type: jobType, Payload: raw, Attempts: 0, MaxAttempts: q.cfg.MaxAttempts, CreatedAt: now, UpdatedAt: now, VisibleAt: now}
	if len(headers) > 0 {
		job.Headers = headers[0]
	}
	return job.ID, q.store.Enqueue(context.Background(), job)
}
func (q *DurableQueue) Start() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started {
		return nil
	}
	q.ctx, q.cancel = context.WithCancel(context.Background())
	q.started = true
	for i := 0; i < q.cfg.Workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return nil
}
func (q *DurableQueue) Close() error {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	cancel := q.cancel
	q.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	q.wg.Wait()
	if q.store != nil {
		return q.store.Close()
	}
	return nil
}
func (q *DurableQueue) worker() {
	defer q.wg.Done()
	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-q.ctx.Done():
			return
		default:
			q.processOne()
		}
		select {
		case <-q.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (q *DurableQueue) processOne() bool {
	job, err := q.store.Claim(q.ctx, time.Now().UTC())
	if err != nil {
		return false
	}
	q.mu.RLock()
	handler := q.handlers[job.Type]
	q.mu.RUnlock()
	if handler == nil {
		_ = q.store.Retry(q.ctx, job, fmt.Errorf("no handler registered for job type %q", job.Type), q.cfg.Backoff)
		return true
	}
	if err := handler(q.ctx, job); err != nil {
		_ = q.store.Retry(q.ctx, job, err, q.cfg.Backoff)
		return true
	}
	_ = q.store.Complete(q.ctx, job)
	return true
}
func (q *DurableQueue) Stats() (QueueStats, error) {
	if q == nil || q.store == nil {
		return QueueStats{}, nil
	}
	return q.store.Stats(context.Background())
}

// FileQueueStorage is the default file/directory based QueueStorage.
type FileQueueStorageConfig struct{ Dir string }
type FileQueueStorage struct {
	dir     string
	eventMu sync.Mutex
	events  *os.File
}

func OpenFileQueueStorage(cfg FileQueueStorageConfig) (*FileQueueStorage, error) {
	if cfg.Dir == "" {
		cfg.Dir = ".fh-reliability/queue"
	}
	for _, d := range []string{"pending", "processing", "done", "failed"} {
		if err := os.MkdirAll(filepath.Join(cfg.Dir, d), 0o700); err != nil {
			return nil, err
		}
	}
	events, err := os.OpenFile(filepath.Join(cfg.Dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileQueueStorage{dir: cfg.Dir, events: events}, nil
}
func (s *FileQueueStorage) Dir() string {
	if s == nil {
		return ""
	}
	return s.dir
}
func (s *FileQueueStorage) Enqueue(ctx context.Context, job *QueueJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.writeJobAtomic("pending", job); err != nil {
		return err
	}
	return s.appendEvent("enqueued", "pending", job, "")
}
func (s *FileQueueStorage) Claim(ctx context.Context, now time.Time) (*QueueJob, error) {
	files, err := os.ReadDir(filepath.Join(s.dir, "pending"))
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for _, ent := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		pending := filepath.Join(s.dir, "pending", ent.Name())
		job, err := readJob(pending)
		if err != nil {
			_ = os.Remove(pending)
			continue
		}
		if job.VisibleAt.After(now) {
			continue
		}
		processing := filepath.Join(s.dir, "processing", ent.Name())
		if os.Rename(pending, processing) == nil {
			_ = s.appendEvent("claimed", "processing", job, "")
			return job, nil
		}
	}
	return nil, ErrQueueEmpty
}
func (s *FileQueueStorage) Complete(ctx context.Context, job *QueueJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	job.UpdatedAt = time.Now().UTC()
	job.LastError = ""
	if err := s.writeJobAtomic("done", job); err != nil {
		return err
	}
	_ = s.appendEvent("completed", "done", job, "")
	return os.Remove(filepath.Join(s.dir, "processing", job.ID+".json"))
}
func (s *FileQueueStorage) Retry(ctx context.Context, job *QueueJob, cause error, backoff time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now().UTC()
	job.Attempts++
	job.UpdatedAt = now
	if cause != nil {
		job.LastError = cause.Error()
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}
	if job.Attempts >= job.MaxAttempts {
		return s.Fail(ctx, job, cause)
	}
	job.VisibleAt = now.Add(backoff * time.Duration(job.Attempts))
	if err := s.writeJobAtomic("pending", job); err != nil {
		return err
	}
	_ = s.appendEvent("retry_scheduled", "pending", job, job.LastError)
	return os.Remove(filepath.Join(s.dir, "processing", job.ID+".json"))
}
func (s *FileQueueStorage) Fail(ctx context.Context, job *QueueJob, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cause != nil {
		job.LastError = cause.Error()
	}
	job.UpdatedAt = time.Now().UTC()
	if err := s.writeJobAtomic("failed", job); err != nil {
		return err
	}
	_ = s.appendEvent("failed", "failed", job, job.LastError)
	return os.Remove(filepath.Join(s.dir, "processing", job.ID+".json"))
}
func (s *FileQueueStorage) Recover(ctx context.Context) error {
	files, err := os.ReadDir(filepath.Join(s.dir, "processing"))
	if err != nil {
		return err
	}
	for _, ent := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ent.IsDir() {
			continue
		}
		from := filepath.Join(s.dir, "processing", ent.Name())
		to := filepath.Join(s.dir, "pending", ent.Name())
		if job, err := readJob(from); err == nil {
			if os.Rename(from, to) == nil {
				_ = s.appendEvent("recovered", "pending", job, "")
			}
		} else {
			_ = os.Rename(from, to)
		}
	}
	return nil
}
func (s *FileQueueStorage) Stats(ctx context.Context) (QueueStats, error) {
	var st QueueStats
	for _, item := range []struct {
		name string
		dst  *int
	}{{"pending", &st.Pending}, {"processing", &st.Processing}, {"done", &st.Done}, {"failed", &st.Failed}} {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		ents, err := os.ReadDir(filepath.Join(s.dir, item.name))
		if err != nil {
			return st, err
		}
		for _, e := range ents {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
				*item.dst++
			}
		}
	}
	return st, nil
}
func (s *FileQueueStorage) Close() error {
	if s == nil || s.events == nil {
		return nil
	}
	return s.events.Close()
}
func (s *FileQueueStorage) writeJobAtomic(state string, job *QueueJob) error {
	if job.ID == "" {
		job.ID = newQueueID()
	}
	job.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Join(s.dir, state)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	final := filepath.Join(dir, job.ID+".json")
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(b)
	serr := f.Sync()
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	if serr != nil {
		return serr
	}
	if cerr != nil {
		return cerr
	}
	return os.Rename(tmp, final)
}
func (s *FileQueueStorage) appendEvent(event, state string, job *QueueJob, errText string) error {
	if s == nil || job == nil {
		return nil
	}
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.events == nil {
		return nil
	}
	e := QueueEvent{ID: newQueueEventID(), JobID: job.ID, Type: job.Type, State: state, Event: event, Attempts: job.Attempts, Error: errText, CreatedAt: time.Now().UTC()}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err = s.events.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.events.Sync()
}

func readJob(path string) (*QueueJob, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var j QueueJob
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, err
	}
	return &j, nil
}
func newQueueID() string {
	return "job_" + strconv.FormatInt(time.Now().UnixNano(), 36) + "_" + strings.TrimPrefix(newRequestID(), "req_")
}
func newQueueEventID() string {
	return "qev_" + strconv.FormatInt(time.Now().UnixNano(), 36) + "_" + strings.TrimPrefix(newRequestID(), "req_")
}
