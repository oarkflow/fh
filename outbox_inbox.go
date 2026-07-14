package fh

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var ErrDuplicateInboxMessage = errors.New("fh: duplicate inbox message")

type OutboxMessage struct {
	ID          string            `json:"id"`
	Topic       string            `json:"topic"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	State       string            `json:"state"`
	Attempts    int               `json:"attempts"`
	MaxAttempts int               `json:"max_attempts"`
	NextAttempt time.Time         `json:"next_attempt,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type InboxMessage struct {
	ID        string    `json:"id"`
	Source    string    `json:"source,omitempty"`
	Hash      string    `json:"hash,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type OutboxStore interface {
	SaveOutbox(context.Context, *OutboxMessage) error
	ClaimOutbox(context.Context, time.Time, int) ([]*OutboxMessage, error)
	CompleteOutbox(context.Context, string) error
	RetryOutbox(context.Context, string, error, time.Duration) error
	FailOutbox(context.Context, string, error) error
	ListOutbox(context.Context, string, int) ([]OutboxMessage, error)
	Close() error
}

type InboxStore interface {
	BeginInbox(context.Context, InboxMessage) (bool, error)
	Close() error
}

type OutboxDispatcher func(context.Context, *OutboxMessage) error

type StoredOutbox struct {
	store       OutboxStore
	dispatch    OutboxDispatcher
	maxAttempts int
	backoff     time.Duration
}

type OutboxConfig struct {
	Store       OutboxStore
	Dispatcher  OutboxDispatcher
	MaxAttempts int
	Backoff     time.Duration
}

func NewStoredOutbox(cfg OutboxConfig) *StoredOutbox {
	if cfg.Store == nil {
		cfg.Store = NewMemoryOutboxInboxStore()
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = time.Second
	}
	return &StoredOutbox{store: cfg.Store, dispatch: cfg.Dispatcher, maxAttempts: cfg.MaxAttempts, backoff: cfg.Backoff}
}

func (o *StoredOutbox) Publish(ctx context.Context, topic string, payload any, headers map[string]string) (string, error) {
	if o == nil || o.store == nil {
		return "", errors.New("fh: outbox disabled")
	}
	if topic == "" {
		return "", errors.New("fh: outbox topic required")
	}
	raw, err := marshalRaw(payload)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	msg := &OutboxMessage{ID: newQueueID(), Topic: topic, Payload: raw, Headers: cloneOutboxStringMap(headers), State: "pending", MaxAttempts: o.maxAttempts, CreatedAt: now, UpdatedAt: now}
	return msg.ID, o.store.SaveOutbox(ctx, msg)
}

func (o *StoredOutbox) DispatchOnce(ctx context.Context, limit int) (int, error) {
	if o == nil || o.store == nil {
		return 0, errors.New("fh: outbox disabled")
	}
	if o.dispatch == nil {
		return 0, errors.New("fh: outbox dispatcher required")
	}
	msgs, err := o.store.ClaimOutbox(ctx, time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, msg := range msgs {
		if err := o.dispatch(ctx, msg); err != nil {
			if msg.Attempts >= msg.MaxAttempts {
				if ferr := o.store.FailOutbox(ctx, msg.ID, err); ferr != nil {
					return processed, fmt.Errorf("fh: outbox fail message %s: %w", msg.ID, ferr)
				}
			} else {
				if rerr := o.store.RetryOutbox(ctx, msg.ID, err, o.backoff); rerr != nil {
					return processed, fmt.Errorf("fh: outbox retry message %s: %w", msg.ID, rerr)
				}
			}
			continue
		}
		if cerr := o.store.CompleteOutbox(ctx, msg.ID); cerr != nil {
			return processed, fmt.Errorf("fh: outbox complete message %s: %w", msg.ID, cerr)
		}
		processed++
	}
	return processed, nil
}

func (o *StoredOutbox) List(ctx context.Context, state string, limit int) ([]OutboxMessage, error) {
	if o == nil || o.store == nil {
		return nil, errors.New("fh: outbox disabled")
	}
	return o.store.ListOutbox(ctx, state, limit)
}

func InboxDedupeKey(source string, payload []byte) string {
	sum := sha256.Sum256(payload)
	if source == "" {
		return hex.EncodeToString(sum[:])
	}
	return source + ":" + hex.EncodeToString(sum[:])
}

func marshalRaw(payload any) (json.RawMessage, error) {
	switch v := payload.(type) {
	case nil:
		return nil, nil
	case []byte:
		return append([]byte(nil), v...), nil
	case json.RawMessage:
		return append([]byte(nil), v...), nil
	default:
		return json.Marshal(v)
	}
}
func cloneOutboxStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type MemoryOutboxInboxStore struct {
	mu     sync.Mutex
	outbox map[string]*OutboxMessage
	inbox  map[string]InboxMessage
}

func NewMemoryOutboxInboxStore() *MemoryOutboxInboxStore {
	return &MemoryOutboxInboxStore{outbox: map[string]*OutboxMessage{}, inbox: map[string]InboxMessage{}}
}
func (s *MemoryOutboxInboxStore) SaveOutbox(ctx context.Context, m *OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || m == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := cloneOutbox(m)
	s.outbox[cp.ID] = cp
	return nil
}
func (s *MemoryOutboxInboxStore) ClaimOutbox(ctx context.Context, now time.Time, limit int) ([]*OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.outbox))
	for id, m := range s.outbox {
		if m.State == "pending" && (m.NextAttempt.IsZero() || !m.NextAttempt.After(now)) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]*OutboxMessage, 0, minOutboxInt(limit, len(ids)))
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		m := s.outbox[id]
		m.State = "processing"
		m.Attempts++
		m.UpdatedAt = now
		out = append(out, cloneOutbox(m))
	}
	return out, nil
}
func (s *MemoryOutboxInboxStore) CompleteOutbox(ctx context.Context, id string) error {
	return s.setOutbox(ctx, id, "done", nil, 0)
}
func (s *MemoryOutboxInboxStore) RetryOutbox(ctx context.Context, id string, err error, delay time.Duration) error {
	return s.setOutbox(ctx, id, "pending", err, delay)
}
func (s *MemoryOutboxInboxStore) FailOutbox(ctx context.Context, id string, err error) error {
	return s.setOutbox(ctx, id, "failed", err, 0)
}
func (s *MemoryOutboxInboxStore) setOutbox(ctx context.Context, id, state string, cause error, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.outbox[id]
	if m == nil {
		return fmt.Errorf("fh: outbox message %s not found", id)
	}
	m.State = state
	m.UpdatedAt = time.Now().UTC()
	if cause != nil {
		m.LastError = cause.Error()
	}
	if delay > 0 {
		m.NextAttempt = m.UpdatedAt.Add(delay)
	}
	return nil
}
func (s *MemoryOutboxInboxStore) ListOutbox(ctx context.Context, state string, limit int) ([]OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.outbox))
	for id, m := range s.outbox {
		if state == "" || m.State == state {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]OutboxMessage, 0, minOutboxInt(limit, len(ids)))
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		out = append(out, *cloneOutbox(s.outbox[id]))
	}
	return out, nil
}
func (s *MemoryOutboxInboxStore) BeginInbox(ctx context.Context, m InboxMessage) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s == nil {
		return false, nil
	}
	if m.ID == "" {
		return false, errors.New("fh: inbox id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inbox[m.ID]; ok {
		return false, ErrDuplicateInboxMessage
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	s.inbox[m.ID] = m
	return true, nil
}
func (s *MemoryOutboxInboxStore) Close() error { return nil }
func cloneOutbox(m *OutboxMessage) *OutboxMessage {
	if m == nil {
		return nil
	}
	cp := *m
	cp.Payload = append([]byte(nil), m.Payload...)
	cp.Headers = cloneOutboxStringMap(m.Headers)
	return &cp
}
func minOutboxInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
