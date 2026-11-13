package fh

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
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

const (
	maxJobFileSize    = 1 << 20 // 1MB
	maxScannerBufSize = 1 << 18 // 256KB
)

var (
	ErrQueueEmpty         = errors.New("fh: queue empty")
	ErrQueueJobIDConflict = errors.New("fh: queue job id already belongs to different work")
)

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
	IdempotencyProcessingTimeout time.Duration
	IdempotencyProcessingStatus  int
	IdempotencyReplayHeaderValue string

	QueueDir                   string
	QueueWorkers               int
	QueueMaxAttempts           int
	QueuePollInterval          time.Duration
	QueueBackoff               time.Duration
	QueueConcurrencyLimitByKey bool
	QueueLogError              func(msg string, args ...any)
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

// IdempotencyJanitor is an optional extension for repositories that can purge
// expired idempotency records. It is useful for long-running services where
// replay records should not grow without bounds.
type IdempotencyJanitor interface {
	PurgeExpired(context.Context, time.Time) (int, error)
}

// IdempotencyAborter is an optional extension for repositories that can cancel
// in-flight processing records when a request fails or panics before completion.
type IdempotencyAborter interface {
	Abort(key, reqHash string) error
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

// MultiReplicaQueueStorage is an optional capability for stores with atomic
// claims and fenced, expiring leases. Recover must not reclaim live work owned
// by another replica. The strict deployment gate requires this capability.
type MultiReplicaQueueStorage interface {
	QueueStorage
	SupportsMultiReplica() bool
}

// MultiReplicaQueueKeyConcurrencyStorage marks stores that enforce
// QueueConcurrencyLimitByKey across all replicas, rather than only within one
// DurableQueue process.
type MultiReplicaQueueKeyConcurrencyStorage interface {
	SupportsMultiReplicaKeyConcurrency() bool
}

// QueueConcurrencyLeaseStorage provides cross-process serialization for a
// concurrency key. The lease must be fenced by the job claim and expire if its
// owner stops renewing it.
type QueueConcurrencyLeaseStorage interface {
	AcquireConcurrencyLease(context.Context, *QueueJob) (bool, error)
	// ReleaseConcurrencyLease is called only after the job has successfully
	// transitioned out of processing. Failed transitions must retain the lease
	// until expiry, so an uncertain owner cannot overlap a new worker.
	ReleaseConcurrencyLease(context.Context, *QueueJob) error
}

// QueueJobDeferrer moves a claimed job back to pending after a temporary
// scheduling conflict without recording a handler failure or consuming an
// attempt.
type QueueJobDeferrer interface {
	Defer(context.Context, *QueueJob, time.Duration) error
}

// QueueLeaseRenewer is an optional extension for queue stores that assign
// expiring ownership leases when Claim succeeds. RenewLease must fail after a
// lease expires or a newer fence has been issued. DurableQueue renews the lease
// while the handler runs and cancels its context when ownership is lost.
type QueueLeaseRenewer interface {
	RenewLease(context.Context, *QueueJob) error
	LeaseTTL() time.Duration
}

// QueueJobLister is an optional extension for QueueStorage implementations that
// can expose jobs for admin/API tooling. The state value should be one of
// pending, processing, done or failed; an empty state means all states. Limit <=
// 0 lets the implementation choose a safe default.
type QueueJobLister interface {
	ListJobs(context.Context, string, int) ([]QueueJobSnapshot, error)
}

// QueueFailedOperator is an optional extension for QueueStorage implementations
// that can safely move failed jobs back to pending or discard them from the DLQ.
type QueueFailedOperator interface {
	RequeueFailed(context.Context, string) error
	DiscardFailed(context.Context, string) error
}

// QueueJanitor is an optional extension for QueueStorage implementations that
// can purge old terminal jobs. State should normally be done or failed. Before
// controls the UpdatedAt cutoff. Limit <= 0 lets implementations choose a safe
// default.
type QueueJanitor interface {
	PurgeJobs(context.Context, string, time.Time, int) (int, error)
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

// PurgeExpiredIdempotency removes expired replay records when the configured
// repository supports cleanup. File, memory and SQL adapters implement this.
func (r *Reliability) PurgeExpiredIdempotency(ctx context.Context, now time.Time) (int, error) {
	if r == nil || r.idem == nil {
		return 0, errors.New("fh: idempotency disabled")
	}
	janitor, ok := r.idem.(IdempotencyJanitor)
	if !ok {
		return 0, errors.New("fh: idempotency purge unsupported by repository")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return janitor.PurgeExpired(ctx, now)
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
			r.idem, err = OpenIdempotencyStore(filepath.Join(cfg.DataDir, "idempotency.jsonl"), cfg.IdempotencyTTL, cfg.IdempotencyProcessingTimeout)
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
			storage, err = OpenFileQueueStorage(FileQueueStorageConfig{Dir: qdir, LogError: cfg.QueueLogError})
			if err != nil {
				r.Close()
				return nil, err
			}
		}
		r.queue = NewDurableQueue(DurableQueueConfig{Workers: cfg.QueueWorkers, MaxAttempts: cfg.QueueMaxAttempts, PollInterval: cfg.QueuePollInterval, Backoff: cfg.QueueBackoff, ConcurrencyLimitByKey: cfg.QueueConcurrencyLimitByKey, LogError: cfg.QueueLogError}, storage)
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
	if cfg.IdempotencyProcessingTimeout <= 0 {
		cfg.IdempotencyProcessingTimeout = 60 * time.Second
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
		if err := r.queue.Close(); err != nil {
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
	return func(c Ctx) error {
		requestID := c.Get(r.cfg.RequestIDHeader)
		if !validExternalID(requestID) {
			requestID = newRequestID()
		}
		c.Set(r.cfg.RequestIDHeader, requestID)
		c.Locals("request_id", requestID)

		if r.journal != nil {
			c.CaptureResponseBody()
			meta := RequestJournalEntry{RequestID: requestID, Event: "received", Method: c.Method(), Path: c.Path(), BodyHash: hashBody(c.Body()), RemoteIP: c.IP(), Time: time.Now().UTC()}
			_ = r.journal.Append(meta)
			c.OnBeforeResponse(func(ctx Ctx) error {
				return r.journal.Append(RequestJournalEntry{RequestID: requestID, Event: "completed", Method: ctx.Method(), Path: ctx.Path(), Status: ctx.StatusCode(), BodyHash: hashBody(ctx.ResponseBody()), Time: time.Now().UTC()})
			})
		}

		if r.idem != nil && isUnsafeMethod(c.RequestHeader().Method) {
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
			// Scope the store key to the caller's identity so an attacker
			// who guesses or observes another caller's Idempotency-Key
			// cannot replay that caller's cached response: the raw header
			// value is global client input, not proof of who sent the
			// original request.
			scopedKey := idempotencyIdentity(c) + "\x00" + key
			reqHash := hashRequest(c.RequestHeader().Method, []byte(c.Path()), c.Body())
			decision, rec, err := r.idem.Begin(scopedKey, reqHash, c.Method(), c.Path())
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
			c.Locals("fh.idem_started", true)
			c.CaptureResponseBody()
			idemCompleted := false
			c.OnBeforeResponse(func(ctx Ctx) error {
				idemCompleted = true
				return r.idem.Complete(scopedKey, reqHash, ctx.StatusCode(), ctx.ResponseHeader(HeaderContentType), ctx.GetRespHeaders(), ctx.ResponseBody())
			})
			defer func() {
				if !idemCompleted {
					if aborter, ok := r.idem.(IdempotencyAborter); ok {
						_ = aborter.Abort(scopedKey, reqHash)
					}
				}
			}()
		}
		return c.Next()
	}
}

// idempotencyIdentity returns a best-effort caller identity used to scope
// idempotency keys so two different callers can never collide on the same
// Idempotency-Key value. Prefers the authenticated Principal (JWT/mTLS/API
// key/session); falls back to client IP for unauthenticated routes.
func idempotencyIdentity(c Ctx) string {
	if p, ok := PrincipalFrom(c); ok && p.ID != "" {
		return "principal:" + p.Type + ":" + p.ID
	}
	return "ip:" + c.IP()
}

func isUnsafeMethod(m []byte) bool {
	return bytesEqualFold(m, MethodPOSTBytes) || bytesEqualFold(m, MethodPUTBytes) || bytesEqualFold(m, MethodPATCHBytes) || bytesEqualFold(m, MethodDELETEBytes)
}
func setReplayHeader(c Ctx, k, v string) {
	if strings.EqualFold(k, HeaderContentLength) || strings.EqualFold(k, HeaderConnection) || strings.EqualFold(k, HeaderTransferEncoding) || strings.EqualFold(k, HeaderDate) || strings.EqualFold(k, HeaderSetCookie) || strings.EqualFold(k, "Trailer") {
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

var fallbackRandMu sync.Mutex

func newRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure must not introduce a data race in concurrent request
		// handling. The fallback is only a uniqueness aid for correlation IDs.
		fallbackRandMu.Lock()
		sum := sha256.Sum256([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
		copy(b, sum[:len(b)])
		fallbackRandMu.Unlock()
	}
	return fmt.Sprintf("req_%x", b)
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
	mu                sync.Mutex
	file              *os.File
	path              string
	ttl               time.Duration
	processingTimeout time.Duration
	records           map[string]*IdempotencyRecord
	failed            error
}

func OpenIdempotencyStore(path string, ttl time.Duration, processingTimeout ...time.Duration) (*IdempotencyStore, error) {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	pTimeout := 60 * time.Second
	if len(processingTimeout) > 0 && processingTimeout[0] > 0 {
		pTimeout = processingTimeout[0]
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	s := &IdempotencyStore{path: path, ttl: ttl, processingTimeout: pTimeout, records: make(map[string]*IdempotencyRecord)}
	if err := s.load(); err != nil {
		return nil, fmt.Errorf("fh: load idempotency store: %w", err)
	}
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
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	now := time.Now()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, maxScannerBufSize)
	for sc.Scan() {
		var rec IdempotencyRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return fmt.Errorf("fh: invalid idempotency record: %w", err)
		}
		if rec.Key == "" {
			return errors.New("fh: idempotency record missing key")
		}
		if rec.ExpiresAt.IsZero() || rec.ExpiresAt.After(now) {
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
	if s.failed != nil {
		return 0, nil, s.failed
	}
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
			if rec.State == "processing" {
				procTimeout := s.processingTimeout
				if procTimeout <= 0 {
					procTimeout = 60 * time.Second
				}
				if now.Sub(rec.UpdatedAt) > procTimeout {
					delete(s.records, key)
				} else {
					return idemProcessing, cloneIdem(rec), nil
				}
			} else {
				return idemProcessing, cloneIdem(rec), nil
			}
		}
	}
	rec := &IdempotencyRecord{Key: key, RequestHash: reqHash, Method: method, Path: path, State: "processing", CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(s.ttl)}
	if err := s.appendLocked(rec); err != nil {
		return 0, nil, err
	}
	s.records[key] = rec
	return idemNew, cloneIdem(rec), nil
}
func (s *IdempotencyStore) Complete(key, reqHash string, status int, contentType string, headers map[string][]string, response []byte) error {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	rec := s.records[key]
	if rec == nil {
		return nil
	}
	if rec.RequestHash != reqHash {
		return fmt.Errorf("idempotency hash mismatch for key %q", key)
	}
	completed := cloneIdem(rec)
	completed.State = "completed"
	completed.StatusCode = status
	completed.ContentType = contentType
	completed.Headers = cleanReplayHeaders(headers)
	completed.Response = append([]byte(nil), response...)
	completed.UpdatedAt = now
	if err := s.appendLocked(completed); err != nil {
		return err
	}
	s.records[key] = completed
	return nil
}
func (s *IdempotencyStore) Abort(key, reqHash string) error {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	rec := s.records[key]
	if rec == nil {
		return nil
	}
	if rec.RequestHash != reqHash {
		return fmt.Errorf("idempotency hash mismatch for key %q", key)
	}
	if rec.State != "processing" {
		return nil
	}
	delete(s.records, key)
	aborted := cloneIdem(rec)
	aborted.State = "aborted"
	aborted.UpdatedAt = now
	aborted.ExpiresAt = now
	return s.appendLocked(aborted)
}
func (s *IdempotencyStore) PurgeExpired(ctx context.Context, now time.Time) (int, error) {
	if s == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
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

func cleanReplayHeaders(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for k, v := range in {
		if strings.EqualFold(k, HeaderContentLength) || strings.EqualFold(k, HeaderConnection) || strings.EqualFold(k, HeaderTransferEncoding) || strings.EqualFold(k, HeaderDate) || strings.EqualFold(k, HeaderSetCookie) {
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
	written, err := s.file.Write(append(b, '\n'))
	if err == nil && written != len(b)+1 {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		s.failed = fmt.Errorf("fh: idempotency store write failed: %w", err)
		return s.failed
	}
	return nil
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
	Dir                   string
	Workers               int
	MaxAttempts           int
	PollInterval          time.Duration
	Backoff               time.Duration
	ConcurrencyLimitByKey bool
	LogError              func(msg string, args ...any)
}
type QueueJob struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	Payload        json.RawMessage   `json:"payload,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Attempts       int               `json:"attempts"`
	MaxAttempts    int               `json:"max_attempts"`
	VisibleAt      time.Time         `json:"visible_at"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	LastError      string            `json:"last_error,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	RunAt          time.Time         `json:"run_at,omitempty"`
	ConcurrencyKey string            `json:"concurrency_key,omitempty"`
	// LeaseFence increments for each SQL claim of this job ID and can be used
	// by handlers to fence their own transactional side effects.
	LeaseFence uint64 `json:"lease_fence,omitempty"`
	// LeaseExpiresAt is the expiry observed when Claim returned. A lease renewer
	// may extend the stored lease without mutating the handler's QueueJob.
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty"`
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

// QueueJobSnapshot is a redacted, admin-safe view of a durable queue job. It is
// intentionally metadata-first; payload previews are bounded to avoid leaking or
// returning very large bodies from ops endpoints.
type QueueJobSnapshot struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	State          string            `json:"state"`
	Headers        map[string]string `json:"headers,omitempty"`
	Attempts       int               `json:"attempts"`
	MaxAttempts    int               `json:"max_attempts"`
	VisibleAt      time.Time         `json:"visible_at"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	LastError      string            `json:"last_error,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	RunAt          time.Time         `json:"run_at,omitempty"`
	ConcurrencyKey string            `json:"concurrency_key,omitempty"`
	PayloadBytes   int               `json:"payload_bytes"`
	PayloadPreview string            `json:"payload_preview,omitempty"`
}

// SanitizeQueueJobSnapshot removes common secrets from queue metadata before
// returning it to operational interfaces. Payloads and headers may contain
// application credentials even when they are not named as credentials by the
// queue itself.
var queueSnapshotRedactor = NewRedactor(DefaultRedactionConfig())

func SanitizeQueueJobSnapshot(snapshot QueueJobSnapshot) QueueJobSnapshot {
	if len(snapshot.Headers) > 0 {
		headers := maps.Clone(snapshot.Headers)
		snapshot.Headers = headers
		for key, value := range headers {
			redacted := value
			if queueSnapshotRedactor.isSensitive(key) {
				redacted = queueSnapshotRedactor.Replacement
			} else {
				redacted = RedactSecrets(value)
			}
			if redacted != value {
				headers[key] = redacted
			}
		}
	}
	snapshot.PayloadPreview = RedactSecrets(snapshot.PayloadPreview)
	snapshot.LastError = RedactSecrets(snapshot.LastError)
	return snapshot
}

type DurableQueue struct {
	cfg        DurableQueueConfig
	store      QueueStorage
	mu         sync.RWMutex
	handlers   map[string]QueueHandler
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	started    bool
	activeKeys map[string]struct{}
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
	return &DurableQueue{cfg: cfg, store: storage, handlers: make(map[string]QueueHandler), activeKeys: make(map[string]struct{})}
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
	return q.EnqueueJob(QueueJob{Type: jobType}, payload, headers...)
}

func (q *DurableQueue) EnqueueDelayed(jobType string, payload any, runAt time.Time, headers ...map[string]string) (string, error) {
	return q.EnqueueJob(QueueJob{Type: jobType, RunAt: runAt, VisibleAt: runAt}, payload, headers...)
}

func (q *DurableQueue) EnqueuePriority(jobType string, payload any, priority int, headers ...map[string]string) (string, error) {
	return q.EnqueueJob(QueueJob{Type: jobType, Priority: priority}, payload, headers...)
}

func (q *DurableQueue) EnqueueWithKey(jobType string, payload any, concurrencyKey string, headers ...map[string]string) (string, error) {
	return q.EnqueueJob(QueueJob{Type: jobType, ConcurrencyKey: concurrencyKey}, payload, headers...)
}

func (q *DurableQueue) EnqueueJob(spec QueueJob, payload any, headers ...map[string]string) (string, error) {
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
	if spec.Type == "" {
		return "", errors.New("fh: queue job type is required")
	}
	vis := spec.VisibleAt
	if vis.IsZero() {
		vis = spec.RunAt
	}
	if vis.IsZero() {
		vis = now
	}
	job := &QueueJob{ID: spec.ID, Type: spec.Type, Payload: raw, Attempts: spec.Attempts, MaxAttempts: spec.MaxAttempts, CreatedAt: now, UpdatedAt: now, VisibleAt: vis, RunAt: spec.RunAt, Priority: spec.Priority, ConcurrencyKey: spec.ConcurrencyKey}
	if job.ID == "" {
		job.ID = newQueueID()
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = q.cfg.MaxAttempts
	}
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
	defer func() {
		if r := recover(); r != nil {
			if q.cfg.LogError != nil {
				q.cfg.LogError("queue worker panic", "error", r, "stack", string(debug.Stack()))
			} else {
				fmt.Fprintf(os.Stderr, "fh: queue worker panic: %v\n%s\n", r, debug.Stack())
			}
		}
	}()
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
	// QueueStorage implementations should report an error when no job is
	// available, but tolerate nil,nil as an empty result too. Never let an
	// adapter's empty claim take down every worker by dereferencing a nil job.
	if err != nil || job == nil {
		return false
	}
	// Keep the ownership token separate from the handler-visible job. Handlers
	// receive a mutable pointer and may change ID, LeaseFence or ConcurrencyKey;
	// those values must not redirect renewal or a later state transition.
	claim := *job
	if q.cfg.ConcurrencyLimitByKey && job.ConcurrencyKey != "" {
		q.mu.Lock()
		if _, ok := q.activeKeys[job.ConcurrencyKey]; ok {
			q.mu.Unlock()
			q.deferJob(job, q.cfg.Backoff)
			return true
		}
		q.activeKeys[job.ConcurrencyKey] = struct{}{}
		q.mu.Unlock()
		defer func() { q.mu.Lock(); delete(q.activeKeys, job.ConcurrencyKey); q.mu.Unlock() }()
	}
	q.mu.RLock()
	handler := q.handlers[job.Type]
	q.mu.RUnlock()
	if handler == nil {
		q.retry(job, fmt.Errorf("no handler registered for job type %q", job.Type), q.cfg.Backoff)
		return true
	}
	concurrencyLeaseHeld := false
	if q.cfg.ConcurrencyLimitByKey && job.ConcurrencyKey != "" {
		if leases, ok := q.store.(QueueConcurrencyLeaseStorage); ok {
			acquired, err := leases.AcquireConcurrencyLease(q.ctx, &claim)
			if err != nil {
				q.logStorageFailure("concurrency lease acquisition", job, err)
				return true
			}
			if !acquired {
				q.deferJob(job, q.cfg.Backoff)
				return true
			}
			concurrencyLeaseHeld = true
		}
	}
	handlerCtx, stopRenewal, lostLease := q.leaseContext(&claim)
	handlerErr := invokeQueueHandler(handlerCtx, handler, job)
	stopRenewal()
	// Restore fields used to authenticate the claim and key lease before any
	// completion, retry, logging or release operation observes handler changes.
	job.ID = claim.ID
	job.LeaseFence = claim.LeaseFence
	job.LeaseExpiresAt = claim.LeaseExpiresAt
	job.ConcurrencyKey = claim.ConcurrencyKey
	select {
	case err := <-lostLease:
		q.logStorageFailure("lease renewal", job, err)
		return true
	default:
	}
	if handlerErr != nil {
		if q.retry(job, handlerErr, q.cfg.Backoff) {
			q.releaseConcurrencyLease(job, concurrencyLeaseHeld)
		}
		return true
	}
	if err := q.store.Complete(q.ctx, job); err != nil {
		q.logStorageFailure("complete", job, err)
	} else {
		q.releaseConcurrencyLease(job, concurrencyLeaseHeld)
	}
	return true
}

func (q *DurableQueue) leaseContext(job *QueueJob) (context.Context, func(), <-chan error) {
	renewer, ok := q.store.(QueueLeaseRenewer)
	if !ok || renewer.LeaseTTL() <= 0 {
		return q.ctx, func() {}, nil
	}
	ctx, cancel := context.WithCancel(q.ctx)
	stop := make(chan struct{})
	done := make(chan struct{})
	lost := make(chan error, 1)
	interval := renewer.LeaseTTL() / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, cancelRenew := context.WithTimeout(ctx, interval)
				err := renewer.RenewLease(renewCtx, job)
				cancelRenew()
				if err != nil {
					select {
					case <-stop:
						return
					default:
					}
					if ctx.Err() != nil {
						return
					}
					lost <- err
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() {
		close(stop)
		cancel()
		<-done
	}, lost
}

func (q *DurableQueue) retry(job *QueueJob, cause error, backoff time.Duration) bool {
	if err := q.store.Retry(q.ctx, job, cause, backoff); err != nil {
		q.logStorageFailure("retry", job, err)
		return false
	}
	return true
}

func (q *DurableQueue) deferJob(job *QueueJob, delay time.Duration) {
	if deferrer, ok := q.store.(QueueJobDeferrer); ok {
		if err := deferrer.Defer(q.ctx, job, delay); err != nil {
			q.logStorageFailure("defer", job, err)
		}
		return
	}
	// Preserve compatibility with external adapters that have not implemented
	// the optional no-attempt deferral operation yet.
	q.retry(job, errors.New("concurrency key is busy"), delay)
}

func (q *DurableQueue) releaseConcurrencyLease(job *QueueJob, held bool) {
	if job == nil || !held {
		return
	}
	if leases, ok := q.store.(QueueConcurrencyLeaseStorage); ok {
		if err := leases.ReleaseConcurrencyLease(q.ctx, job); err != nil {
			q.logStorageFailure("concurrency lease release", job, err)
		}
	}
}

func (q *DurableQueue) logStorageFailure(operation string, job *QueueJob, err error) {
	if q.cfg.LogError != nil {
		q.cfg.LogError("queue storage operation failed", "operation", operation, "job_id", job.ID, "type", job.Type, "error", err)
		return
	}
	fmt.Fprintf(os.Stderr, "fh: queue storage operation failed: operation=%s job_id=%q type=%q error=%v\n", operation, job.ID, job.Type, err)
}

// invokeQueueHandler contains panics to the current job. A handler panic must
// not escape to worker's goroutine-level recovery, which would permanently
// remove that worker from the pool.
func invokeQueueHandler(ctx context.Context, handler QueueHandler, job *QueueJob) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("fh: queue handler panic: %v", recovered)
		}
	}()
	return handler(ctx, job)
}

func (q *DurableQueue) Stats() (QueueStats, error) {
	if q == nil || q.store == nil {
		return QueueStats{}, nil
	}
	return q.store.Stats(context.Background())
}

// ListJobs returns queue jobs for admin/ops usage when the storage supports it.
// state may be empty, pending, processing, done or failed.
func (q *DurableQueue) ListJobs(ctx context.Context, state string, limit int) ([]QueueJobSnapshot, error) {
	if q == nil || q.store == nil {
		return nil, errors.New("fh: queue disabled")
	}
	lister, ok := q.store.(QueueJobLister)
	if !ok {
		return nil, errors.New("fh: queue job listing unsupported by storage")
	}
	snapshots, err := lister.ListJobs(ctx, state, limit)
	if err != nil {
		return nil, err
	}
	for i := range snapshots {
		snapshots[i] = SanitizeQueueJobSnapshot(snapshots[i])
	}
	return snapshots, nil
}

// FileQueueStorage is the default file/directory based QueueStorage.
type FileQueueStorageConfig struct {
	Dir      string
	LogError func(msg string, args ...any)
}
type FileQueueStorage struct {
	dir      string
	logError func(msg string, args ...any)
	stateMu  *sync.Mutex
	eventMu  sync.Mutex
	events   *os.File
}

// Striped locks coordinate multiple storage handles opened for the same
// canonical directory without retaining one mutex per historical directory.
// A hash collision only serializes unrelated local queue operations.
var fileQueueStateLocks [256]sync.Mutex

func queueStateLock(dir string) *sync.Mutex {
	key, err := filepath.Abs(dir)
	if err == nil {
		if resolved, resolveErr := filepath.EvalSymlinks(key); resolveErr == nil {
			key = resolved
		}
	}
	key = filepath.Clean(key)
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return &fileQueueStateLocks[h.Sum64()%uint64(len(fileQueueStateLocks))]
}

func OpenFileQueueStorage(cfg FileQueueStorageConfig) (*FileQueueStorage, error) {
	if cfg.Dir == "" {
		cfg.Dir = ".fh-reliability/queue"
	}
	if err := ensureQueuePrivateDirectory(cfg.Dir); err != nil {
		return nil, err
	}
	for _, d := range []string{"pending", "processing", "done", "failed", "quarantine"} {
		if err := ensureQueuePrivateDirectory(filepath.Join(cfg.Dir, d)); err != nil {
			return nil, err
		}
	}
	eventsPath := filepath.Join(cfg.Dir, "events.jsonl")
	before, beforeErr := os.Lstat(eventsPath)
	if beforeErr != nil && !os.IsNotExist(beforeErr) {
		return nil, beforeErr
	}
	if beforeErr == nil && (!before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0) {
		return nil, fmt.Errorf("fh: queue event log %q must be a regular file", eventsPath)
	}
	events, err := os.OpenFile(eventsPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	fileInfo, statErr := events.Stat()
	after, pathErr := os.Lstat(eventsPath)
	if statErr != nil || pathErr != nil || !fileInfo.Mode().IsRegular() || !after.Mode().IsRegular() || !os.SameFile(fileInfo, after) || (beforeErr == nil && !os.SameFile(before, fileInfo)) {
		_ = events.Close()
		if statErr != nil {
			return nil, statErr
		}
		if pathErr != nil {
			return nil, pathErr
		}
		return nil, fmt.Errorf("fh: queue event log %q changed or is not a regular file", eventsPath)
	}
	if runtime.GOOS != "windows" && fileInfo.Mode().Perm() != 0o600 {
		_ = events.Close()
		return nil, fmt.Errorf("fh: queue event log %q must have mode 0600 (has %04o)", eventsPath, fileInfo.Mode().Perm())
	}
	return &FileQueueStorage{dir: cfg.Dir, logError: cfg.LogError, stateMu: queueStateLock(cfg.Dir), events: events}, nil
}

func ensureQueuePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("fh: queue directory %q must be a real directory", path)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		return fmt.Errorf("fh: queue directory %q must have mode 0700 (has %04o)", path, info.Mode().Perm())
	}
	return nil
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
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if err := s.enqueueJobAtomic(job); err != nil {
		return err
	}
	// The job file is authoritative; the event log is an observability record.
	// Returning its append error would tell callers the enqueue failed even
	// though the job is already durable, encouraging duplicate retries.
	s.recordEvent("enqueued", "pending", job, "")
	return nil
}

func (s *FileQueueStorage) enqueueJobAtomic(job *QueueJob) error {
	if job == nil {
		return errors.New("fh: nil queue job")
	}
	if job.ID == "" {
		job.ID = newQueueID()
	}
	if !validQueueJobID(job.ID) {
		return errors.New("fh: invalid queue job id")
	}
	job.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Join(s.dir, "pending")
	f, err := os.CreateTemp(dir, ".job-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Check terminal states before pending so a retry cannot resurrect a job
	// that has already completed or failed. stateMu serializes this check with
	// local claims; Link supplies atomic create-if-absent semantics for pending.
	if existing, state, found, err := s.findQueueJob(job.ID); err != nil {
		return err
	} else if found {
		if !sameQueueJobRequest(existing, job) {
			return ErrQueueJobIDConflict
		}
		return syncQueueDirectory(filepath.Join(s.dir, state))
	}
	final := filepath.Join(dir, job.ID+".json")
	if err := os.Link(tmp, final); err != nil {
		if !os.IsExist(err) {
			return err
		}
		existing, state, found, lookupErr := s.findQueueJob(job.ID)
		if lookupErr != nil {
			return lookupErr
		}
		if !found {
			return ErrQueueJobIDConflict
		}
		if !sameQueueJobRequest(existing, job) {
			return ErrQueueJobIDConflict
		}
		return syncQueueDirectory(filepath.Join(s.dir, state))
	}
	_ = os.Remove(tmp)
	return syncQueueDirectory(dir)
}

func (s *FileQueueStorage) findQueueJob(id string) (*QueueJob, string, bool, error) {
	for _, state := range []string{"done", "failed", "processing", "pending"} {
		path := filepath.Join(s.dir, state, id+".json")
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, "", false, err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, "", false, fmt.Errorf("fh: existing queue job %q is not a regular file", id)
		}
		job, err := readJob(path)
		if err != nil {
			return nil, "", false, err
		}
		if job == nil || job.ID != id {
			return nil, "", false, ErrQueueJobIDConflict
		}
		return job, state, true, nil
	}
	return nil, "", false, nil
}

func sameQueueJobRequest(a, b *QueueJob) bool {
	return a != nil && b != nil &&
		a.ID == b.ID && a.Type == b.Type && bytes.Equal(a.Payload, b.Payload) &&
		maps.Equal(a.Headers, b.Headers) && a.MaxAttempts == b.MaxAttempts &&
		a.Priority == b.Priority && a.RunAt.Equal(b.RunAt) &&
		a.ConcurrencyKey == b.ConcurrencyKey
}
func (s *FileQueueStorage) Claim(ctx context.Context, now time.Time) (*QueueJob, error) {
	files, err := os.ReadDir(filepath.Join(s.dir, "pending"))
	if err != nil {
		return nil, err
	}
	// file order is only a fallback; eligible jobs are sorted by priority/run time below
	type cand struct {
		name string
		job  *QueueJob
	}
	candidates := make([]cand, 0, len(files))
	for _, ent := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		pending := filepath.Join(s.dir, "pending", ent.Name())
		info, statErr := os.Lstat(pending)
		if os.IsNotExist(statErr) {
			continue
		}
		if statErr != nil {
			return nil, statErr
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			if s.logError != nil {
				s.logError("queue job file is not a regular file, quarantining", "file", pending)
			}
			if err := s.quarantineQueueFile(pending, "pending", ent.Name()); err != nil {
				return nil, err
			}
			continue
		}
		job, err := readJob(pending)
		if err == nil && (job == nil || !validQueueJobID(job.ID) || job.ID+".json" != ent.Name()) {
			err = errors.New("fh: queue job id does not match its file name")
		}
		if err != nil {
			if s.logError != nil {
				s.logError("queue job file corrupt, quarantining", "file", pending, "error", err)
			}
			if quarantineErr := s.quarantineQueueFile(pending, "pending", ent.Name()); quarantineErr != nil {
				return nil, quarantineErr
			}
			continue
		}
		if job.VisibleAt.IsZero() && !job.RunAt.IsZero() {
			job.VisibleAt = job.RunAt
		}
		if job.VisibleAt.After(now) {
			continue
		}
		candidates = append(candidates, cand{ent.Name(), job})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].job.Priority != candidates[j].job.Priority {
			return candidates[i].job.Priority > candidates[j].job.Priority
		}
		return candidates[i].job.VisibleAt.Before(candidates[j].job.VisibleAt)
	})
	for _, item := range candidates {
		pending := filepath.Join(s.dir, "pending", item.name)
		processing := filepath.Join(s.dir, "processing", item.name)
		// Rename is the claim operation. Filesystem rename is atomic within the
		// queue directory, so exactly one process can move this pending job.
		// This avoids the close-before-rename race of advisory file locks and
		// works consistently on Unix, Windows, AIX and Plan 9.
		s.stateMu.Lock()
		if os.Rename(pending, processing) == nil {
			if err := syncQueueMove(filepath.Dir(pending), filepath.Dir(processing)); err != nil {
				s.stateMu.Unlock()
				return nil, err
			}
			s.stateMu.Unlock()
			s.recordEvent("claimed", "processing", item.job, "")
			return item.job, nil
		}
		s.stateMu.Unlock()
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
	s.recordEvent("completed", "done", job, "")
	processing := filepath.Join(s.dir, "processing", job.ID+".json")
	if err := os.Remove(processing); err != nil {
		return err
	}
	return syncQueueDirectory(filepath.Dir(processing))
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
	s.recordEvent("retry_scheduled", "pending", job, job.LastError)
	processing := filepath.Join(s.dir, "processing", job.ID+".json")
	if err := os.Remove(processing); err != nil {
		return err
	}
	return syncQueueDirectory(filepath.Dir(processing))
}

func (s *FileQueueStorage) Defer(ctx context.Context, job *QueueJob, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if job == nil || !validQueueJobID(job.ID) {
		return errors.New("fh: invalid queue job id")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	processing := filepath.Join(s.dir, "processing", job.ID+".json")
	info, err := os.Lstat(processing)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("fh: processing queue job %q is not a regular file", job.ID)
	}
	stored, err := readJob(processing)
	if err != nil {
		return err
	}
	if stored == nil || stored.ID != job.ID {
		return ErrQueueJobIDConflict
	}
	if delay < 0 {
		delay = 0
	}
	now := time.Now().UTC()
	stored.UpdatedAt = now
	stored.VisibleAt = now.Add(delay)
	if err := s.writeJobAtomic("pending", stored); err != nil {
		return err
	}
	s.recordEvent("deferred", "pending", stored, "")
	if err := os.Remove(processing); err != nil {
		return err
	}
	return syncQueueDirectory(filepath.Dir(processing))
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
	s.recordEvent("failed", "failed", job, job.LastError)
	processing := filepath.Join(s.dir, "processing", job.ID+".json")
	if err := os.Remove(processing); err != nil {
		return err
	}
	return syncQueueDirectory(filepath.Dir(processing))
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
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		from := filepath.Join(s.dir, "processing", ent.Name())
		to := filepath.Join(s.dir, "pending", ent.Name())
		info, statErr := os.Lstat(from)
		if os.IsNotExist(statErr) {
			continue
		}
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			if err := s.quarantineQueueFile(from, "processing", ent.Name()); err != nil {
				return err
			}
			continue
		}
		if job, err := readJob(from); err == nil && job != nil && validQueueJobID(job.ID) && job.ID+".json" == ent.Name() {
			// Complete, Retry and Fail persist the destination record before
			// removing the processing record. A crash in that window leaves two
			// copies. Keep the already-persisted transition instead of allowing
			// the older processing copy to overwrite it during recovery.
			alreadyTransitioned := false
			terminalTransition := false
			for _, state := range []string{"done", "failed", "pending"} {
				destination := filepath.Join(s.dir, state, ent.Name())
				destinationInfo, destinationErr := os.Lstat(destination)
				if os.IsNotExist(destinationErr) {
					continue
				}
				if destinationErr != nil {
					return destinationErr
				}
				if !destinationInfo.Mode().IsRegular() || destinationInfo.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("fh: queue recovery destination %q is not a regular file", destination)
				}
				destinationJob, readErr := readJob(destination)
				if readErr != nil || destinationJob == nil || destinationJob.ID != job.ID {
					return fmt.Errorf("fh: queue recovery destination %q conflicts with processing job %q", destination, job.ID)
				}
				alreadyTransitioned = true
				terminalTransition = state == "done" || state == "failed"
				break
			}
			if alreadyTransitioned {
				if terminalTransition {
					pendingPath := filepath.Join(s.dir, "pending", ent.Name())
					pendingInfo, pendingErr := os.Lstat(pendingPath)
					if pendingErr == nil {
						if !pendingInfo.Mode().IsRegular() || pendingInfo.Mode()&os.ModeSymlink != 0 {
							return fmt.Errorf("fh: queue recovery destination %q is not a regular file", pendingPath)
						}
						pendingJob, readErr := readJob(pendingPath)
						if readErr != nil || pendingJob == nil || pendingJob.ID != job.ID {
							return fmt.Errorf("fh: queue recovery destination %q conflicts with processing job %q", pendingPath, job.ID)
						}
						if err := os.Remove(pendingPath); err != nil {
							return err
						}
						if err := syncQueueDirectory(filepath.Dir(pendingPath)); err != nil {
							return err
						}
					} else if !os.IsNotExist(pendingErr) {
						return pendingErr
					}
				}
				if err := os.Remove(from); err != nil {
					return err
				}
				if err := syncQueueDirectory(filepath.Dir(from)); err != nil {
					return err
				}
				continue
			}
			if os.Rename(from, to) == nil {
				if err := syncQueueMove(filepath.Dir(from), filepath.Dir(to)); err != nil {
					return err
				}
				s.recordEvent("recovered", "pending", job, "")
			}
		} else {
			if err := s.quarantineQueueFile(from, "processing", ent.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *FileQueueStorage) quarantineQueueFile(path, sourceState, name string) error {
	if s == nil {
		return errors.New("fh: nil queue storage")
	}
	quarantineDir := filepath.Join(s.dir, "quarantine")
	quarantineName := sourceState + "-" + newQueueEventID() + "-" + filepath.Base(name)
	quarantinePath := filepath.Join(quarantineDir, quarantineName)
	if err := os.Rename(path, quarantinePath); err != nil {
		return err
	}
	return syncQueueMove(filepath.Dir(path), quarantineDir)
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
	if job == nil {
		return errors.New("fh: nil queue job")
	}
	if job.ID == "" {
		job.ID = newQueueID()
	}
	if !validQueueJobID(job.ID) {
		return errors.New("fh: invalid queue job id")
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
	f, err := os.CreateTemp(dir, ".job-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
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
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	return syncQueueDirectory(dir)
}

func syncQueueDirectory(path string) error {
	// Go exposes directory fsync on Unix, but Windows directory handles do not
	// support FlushFileBuffers. Job contents are still synced before rename;
	// Windows directory-entry durability across sudden power loss is best-effort.
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func syncQueueMove(fromDir, toDir string) error {
	if err := syncQueueDirectory(fromDir); err != nil {
		return err
	}
	if filepath.Clean(fromDir) == filepath.Clean(toDir) {
		return nil
	}
	return syncQueueDirectory(toDir)
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
	line := append(b, '\n')
	n, err := s.events.Write(line)
	if err != nil {
		return err
	}
	if n != len(line) {
		return io.ErrShortWrite
	}
	return s.events.Sync()
}

func (s *FileQueueStorage) recordEvent(event, state string, job *QueueJob, errText string) {
	if err := s.appendEvent(event, state, job, errText); err != nil {
		jobID := ""
		if job != nil {
			jobID = job.ID
		}
		if s != nil && s.logError != nil {
			s.logError("queue event log append failed", "event", event, "job_id", jobID, "error", err)
			return
		}
		fmt.Fprintf(os.Stderr, "fh: queue event log append failed: event=%s job_id=%s error=%v\n", event, jobID, err)
	}
}

func readJob(path string) (*QueueJob, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxJobFileSize {
		return nil, fmt.Errorf("fh: job file %s exceeds maximum size %d bytes", path, maxJobFileSize)
	}
	b, err := io.ReadAll(f)
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

type ReliabilityPolicy struct {
	Enabled                bool
	RequireIdempotency     bool
	Journal                bool
	ReplayResponse         bool
	ConflictOnBodyDrift    bool
	MaxReplayAge           time.Duration
	IdempotencyKey         func(Ctx) string
	IdempotencyFingerprint func(Ctx) string
	Data                   DataPolicy
	Queue                  bool
	QueueType              string
	QueuePriority          int
	QueueDelay             time.Duration
	ConcurrencyKey         func(Ctx) string
}

// ApplyPolicy runs a route reliability policy against a request. Reusable
// middleware construction lives in mw/reliability.
func (r *Reliability) ApplyPolicy(c Ctx, p ReliabilityPolicy) error {
	if !p.Enabled {
		return c.Next()
	}
	if r == nil {
		return c.Next()
	}
	if p.Data.Sensitivity != "" {
		c.Locals("fh.data_policy", p.Data)
	}
	requestID, _ := c.Locals("request_id").(string)
	if requestID == "" {
		requestID = newRequestID()
		c.Set(HeaderRequestID, requestID)
		c.Locals("request_id", requestID)
	}
	if p.Journal && r.journal != nil {
		_ = r.journal.Append(RequestJournalEntry{RequestID: requestID, Event: "policy.received", Method: c.Method(), Path: c.Path(), BodyHash: hashBody(c.Body()), RemoteIP: c.IP(), Time: time.Now().UTC()})
	}
	if r.idem != nil && isUnsafeMethod(c.RequestHeader().Method) && c.Locals("fh.idem_started") == nil {
		key := ""
		if p.IdempotencyKey != nil {
			key = p.IdempotencyKey(c)
		}
		if key == "" {
			key = c.Get(r.cfg.IdempotencyHeader)
		}
		if key == "" && p.IdempotencyFingerprint != nil {
			key = p.IdempotencyFingerprint(c)
			c.Set(r.cfg.IdempotencyHeader, key)
		}
		if key == "" && p.RequireIdempotency {
			return c.Status(StatusBadRequest).JSON(Map{"error": "missing_idempotency_key", "request_id": requestID})
		}
		if key != "" {
			if !validExternalID(key) {
				return c.Status(StatusBadRequest).JSON(Map{"error": "invalid_idempotency_key", "request_id": requestID})
			}
			reqHash := hashRequest(c.RequestHeader().Method, []byte(c.Path()), c.Body())
			if p.IdempotencyFingerprint != nil {
				reqHash = p.IdempotencyFingerprint(c)
			}
			decision, rec, err := r.idem.Begin(key, reqHash, c.Method(), c.Path())
			if err != nil {
				return err
			}
			switch decision {
			case IdempotencyReplay:
				c.Set(HeaderReplayed, r.cfg.IdempotencyReplayHeaderValue)
				if p.ReplayResponse {
					for k, vals := range rec.Headers {
						for _, v := range vals {
							setReplayHeader(c, k, v)
						}
					}
					if rec.ContentType != "" {
						c.Type(rec.ContentType)
					}
					return c.Status(rec.StatusCode).SendBytes(rec.Response)
				}
			case IdempotencyConflict:
				return c.Status(StatusConflict).JSON(Map{"error": "idempotency_key_reused_with_different_payload", "request_id": requestID})
			case IdempotencyProcessing:
				return c.Status(r.cfg.IdempotencyProcessingStatus).JSON(Map{"error": "idempotency_key_processing", "request_id": requestID})
			}
			c.CaptureResponseBody()
			idemCompleted := false
			c.OnBeforeResponse(func(ctx Ctx) error {
				idemCompleted = true
				return r.idem.Complete(key, reqHash, ctx.StatusCode(), ctx.ResponseHeader(HeaderContentType), ctx.GetRespHeaders(), ctx.ResponseBody())
			})
			defer func() {
				if !idemCompleted {
					if aborter, ok := r.idem.(IdempotencyAborter); ok {
						_ = aborter.Abort(key, reqHash)
					}
				}
			}()
		}
	}
	return c.Next()
}

// Reliability returns the request's configured reliability runtime.
func (c *DefaultCtx) Reliability() *Reliability {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Reliability()
}

type ReliabilityTx interface {
	Journal() RequestJournalStore
	Idempotency() IdempotencyRepository
	Queue() QueueStorage
	Commit() error
	Rollback() error
}
type memoryReliabilityTx struct {
	r         *Reliability
	journal   []RequestJournalEntry
	jobs      []*QueueJob
	committed bool
}

func (r *Reliability) BeginTx(ctx context.Context) (ReliabilityTx, error) {
	if r == nil {
		return nil, errors.New("fh: reliability disabled")
	}
	return &memoryReliabilityTx{r: r}, nil
}
func (tx *memoryReliabilityTx) Journal() RequestJournalStore       { return tx }
func (tx *memoryReliabilityTx) Idempotency() IdempotencyRepository { return tx.r.idem }
func (tx *memoryReliabilityTx) Queue() QueueStorage                { return tx }
func (tx *memoryReliabilityTx) Append(e RequestJournalEntry) error {
	tx.journal = append(tx.journal, e)
	return nil
}
func (tx *memoryReliabilityTx) Close() error { return nil }
func (tx *memoryReliabilityTx) Enqueue(ctx context.Context, j *QueueJob) error {
	cp := *j
	cp.Payload = append([]byte(nil), j.Payload...)
	tx.jobs = append(tx.jobs, &cp)
	return nil
}
func (tx *memoryReliabilityTx) Claim(context.Context, time.Time) (*QueueJob, error) {
	return nil, ErrQueueEmpty
}
func (tx *memoryReliabilityTx) Complete(context.Context, *QueueJob) error { return nil }
func (tx *memoryReliabilityTx) Retry(context.Context, *QueueJob, error, time.Duration) error {
	return nil
}
func (tx *memoryReliabilityTx) Fail(context.Context, *QueueJob, error) error { return nil }
func (tx *memoryReliabilityTx) Recover(context.Context) error                { return nil }
func (tx *memoryReliabilityTx) Stats(context.Context) (QueueStats, error)    { return QueueStats{}, nil }
func (tx *memoryReliabilityTx) Commit() error {
	if tx.committed {
		return nil
	}
	tx.committed = true
	for _, e := range tx.journal {
		if tx.r.journal != nil {
			if err := tx.r.journal.Append(e); err != nil {
				return err
			}
		}
	}
	if tx.r.queue != nil {
		for _, j := range tx.jobs {
			if err := tx.r.queue.store.Enqueue(context.Background(), j); err != nil {
				return err
			}
		}
	}
	return nil
}
func (tx *memoryReliabilityTx) Rollback() error { tx.journal = nil; tx.jobs = nil; return nil }

type OutboxEvent struct {
	ID, Topic, Key string
	Payload        json.RawMessage
	Headers        map[string]string
	CreatedAt      time.Time
}
type InboxEvent struct {
	ID, Source, EventID string
	Payload             json.RawMessage
	Headers             map[string]string
	CreatedAt           time.Time
}
type Outbox struct{ q *DurableQueue }
type Inbox struct {
	idem IdempotencyRepository
	q    *DurableQueue
}

func (r *Reliability) Outbox() *Outbox {
	if r == nil {
		return nil
	}
	return &Outbox{q: r.queue}
}
func (r *Reliability) Inbox() *Inbox {
	if r == nil {
		return nil
	}
	return &Inbox{idem: r.idem, q: r.queue}
}
func (o *Outbox) Publish(ctx context.Context, ev OutboxEvent) (string, error) {
	if o == nil || o.q == nil {
		return "", errors.New("fh: outbox queue disabled")
	}
	if ev.ID == "" {
		ev.ID = newQueueID()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now().UTC()
	}
	return o.q.Enqueue("outbox."+ev.Topic, ev, ev.Headers)
}
func (i *Inbox) Accept(ctx context.Context, ev InboxEvent, queueType string) (string, error) {
	if i == nil || i.q == nil {
		return "", errors.New("fh: inbox queue disabled")
	}
	if ev.EventID == "" {
		return "", errors.New("fh: inbox event id required")
	}
	// Reject malformed Source/EventID up front: both flow unvalidated into
	// the dedup key below, and a caller-reachable Accept (e.g. behind a
	// webhook endpoint) could otherwise pre-seed an oversized or
	// separator-colliding key to poison the dedup cache ahead of a
	// legitimate event, causing it to be silently treated as a duplicate
	// and dropped.
	if !validExternalID(ev.EventID) || (ev.Source != "" && !validExternalID(ev.Source)) {
		return "", errors.New("fh: invalid inbox source or event id")
	}
	b, _ := json.Marshal(ev)
	// Length-prefixed rather than a plain "source:eventID" join: since both
	// fields may themselves legally contain ':' (validExternalID allows it),
	// a naive join lets Source="a:b",EventID="c" collide with Source="a",
	// EventID="b:c" on the same dedup key.
	key := strconv.Itoa(len(ev.Source)) + ":" + ev.Source + ":" + strconv.Itoa(len(ev.EventID)) + ":" + ev.EventID
	if i.idem != nil {
		dec, _, err := i.idem.Begin(key, hashBody(b), "INBOX", ev.Source)
		if err != nil {
			return "", err
		}
		if dec != IdempotencyNew {
			return "", nil
		}
		defer i.idem.Complete(key, hashBody(b), 202, "application/json", nil, []byte(`{"status":"accepted"}`))
	}
	if queueType == "" {
		queueType = "inbox." + ev.Source
	}
	return i.q.Enqueue(queueType, ev)
}

// RunReliableEndpoint applies a reliability policy around an endpoint handler.
// Endpoint construction itself lives in mw/reliability.
func (c *DefaultCtx) RunReliableEndpoint(policy ReliabilityPolicy, endpoint HandlerFunc) error {
	if !policy.Enabled {
		return endpoint(c)
	}
	originalHandlers, originalIndex := c.handlers, c.handlerIndex
	remaining := append([]HandlerFunc{endpoint}, originalHandlers[originalIndex:]...)
	c.handlers, c.handlerIndex = remaining, 0
	err := c.Reliability().ApplyPolicy(c, policy)
	c.handlers, c.handlerIndex = originalHandlers, originalIndex
	return err
}

// Queue returns the request's configured durable queue.
func (c *DefaultCtx) Queue() Queue {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Queue()
}

func (a *App) Outbox() *Outbox {
	if a == nil || a.reliability == nil {
		return nil
	}
	return a.reliability.Outbox()
}
func (a *App) Inbox() *Inbox {
	if a == nil || a.reliability == nil {
		return nil
	}
	return a.reliability.Inbox()
}

func AtomicHandoff(c Ctx, jobType string, payload any, opts ...QueueJob) (string, error) {
	q := c.Reliability().queue
	if q == nil {
		return "", errors.New("fh: queue disabled")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	spec := QueueJob{Type: jobType, Payload: raw}
	if len(opts) > 0 {
		spec = opts[0]
		spec.Type = jobType
		spec.Payload = raw
	}
	return q.EnqueueJob(spec, json.RawMessage(raw))
}

func (q *DurableQueue) PurgeJobs(ctx context.Context, state string, before time.Time, limit int) (int, error) {
	if q == nil || q.store == nil {
		return 0, errors.New("fh: queue disabled")
	}
	janitor, ok := q.store.(QueueJanitor)
	if !ok {
		return 0, errors.New("fh: queue purge unsupported by storage")
	}
	if before.IsZero() {
		before = time.Now().UTC()
	}
	return janitor.PurgeJobs(ctx, state, before, limit)
}

func (q *DurableQueue) RetryFailed(ctx context.Context, id string) error {
	if q == nil || q.store == nil {
		return errors.New("fh: queue disabled")
	}
	if op, ok := q.store.(QueueFailedOperator); ok {
		return op.RequeueFailed(ctx, id)
	}
	return errors.New("fh: retry failed unsupported by storage")
}
func (q *DurableQueue) DiscardFailed(ctx context.Context, id string) error {
	if q == nil || q.store == nil {
		return errors.New("fh: queue disabled")
	}
	if op, ok := q.store.(QueueFailedOperator); ok {
		return op.DiscardFailed(ctx, id)
	}
	return errors.New("fh: discard failed unsupported by storage")
}

func snapshotQueueJob(state string, job *QueueJob) QueueJobSnapshot {
	if job == nil {
		return QueueJobSnapshot{State: state}
	}
	preview := string(job.Payload)
	const maxPreview = 512
	if len(preview) > maxPreview {
		preview = preview[:maxPreview] + "..."
	}
	return SanitizeQueueJobSnapshot(QueueJobSnapshot{ID: job.ID, Type: job.Type, State: state, Headers: cloneStringMap(job.Headers), Attempts: job.Attempts, MaxAttempts: job.MaxAttempts, VisibleAt: job.VisibleAt, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, LastError: job.LastError, Priority: job.Priority, RunAt: job.RunAt, ConcurrencyKey: job.ConcurrencyKey, PayloadBytes: len(job.Payload), PayloadPreview: preview})
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func validQueueState(state string) bool {
	switch state {
	case "", "pending", "processing", "done", "failed":
		return true
	default:
		return false
	}
}

func normalizeQueueListLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func (s *FileQueueStorage) ListJobs(ctx context.Context, state string, limit int) ([]QueueJobSnapshot, error) {
	if !validQueueState(state) {
		return nil, errors.New("fh: invalid queue state")
	}
	limit = normalizeQueueListLimit(limit)
	states := []string{state}
	if state == "" {
		states = []string{"pending", "processing", "failed", "done"}
	}
	out := make([]QueueJobSnapshot, 0, min(limit, 64))
	for _, st := range states {
		entries, err := os.ReadDir(filepath.Join(s.dir, st))
		if err != nil {
			return nil, err
		}
		for _, ent := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
				continue
			}
			job, err := readJob(filepath.Join(s.dir, st, ent.Name()))
			if err != nil {
				continue
			}
			out = append(out, snapshotQueueJob(st, job))
			if len(out) >= limit {
				return out, nil
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}
func (s *FileQueueStorage) RequeueFailed(ctx context.Context, id string) error {
	if !validQueueJobID(id) {
		return errors.New("fh: invalid queue job id")
	}
	path := filepath.Join(s.dir, "failed", id+".json")
	j, err := readJob(path)
	if err != nil {
		return err
	}
	j.VisibleAt = time.Now().UTC()
	if err := s.writeJobAtomic("pending", j); err != nil {
		return err
	}
	s.recordEvent("requeued", "pending", j, "")
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncQueueDirectory(filepath.Dir(path))
}
func (s *FileQueueStorage) DiscardFailed(ctx context.Context, id string) error {
	if !validQueueJobID(id) {
		return errors.New("fh: invalid queue job id")
	}
	path := filepath.Join(s.dir, "failed", id+".json")
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncQueueDirectory(filepath.Dir(path))
}

// validQueueJobID rejects anything that isn't a plain job-id token
// (newQueueID only ever produces "job_<base36>_<base36>"), closing off path
// traversal via a crafted id (e.g. "../../pending/other-job" or an absolute
// path) reaching RequeueFailed/DiscardFailed's filepath.Join.
func validQueueJobID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}

func (s *FileQueueStorage) PurgeJobs(ctx context.Context, state string, before time.Time, limit int) (int, error) {
	if state == "" {
		state = "done"
	}
	if state != "done" && state != "failed" {
		return 0, errors.New("fh: only done or failed jobs can be purged")
	}
	if before.IsZero() {
		before = time.Now().UTC()
	}
	limit = normalizeQueueListLimit(limit)
	dir := filepath.Join(s.dir, state)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	purged := 0
	for _, ent := range entries {
		if err := ctx.Err(); err != nil {
			return purged, err
		}
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, ent.Name())
		job, err := readJob(path)
		if err != nil || job == nil || job.UpdatedAt.After(before) {
			continue
		}
		if err := os.Remove(path); err != nil {
			return purged, err
		}
		purged++
		if purged >= limit {
			break
		}
	}
	return purged, nil
}
