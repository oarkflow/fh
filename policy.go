package fh

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SSE support.
type SSE struct {
	c   *DefaultCtx
	buf bytes.Buffer
	mu  sync.Mutex
}

func (c *DefaultCtx) SSE(fn func(*SSE) error) error {
	c.Type("text/event-stream; charset=utf-8")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	s := &SSE{c: c}
	if err := fn(s); err != nil {
		return err
	}
	return c.SendBytes(s.buf.Bytes())
}
func (s *SSE) Event(event string, data any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.ContainsAny(event, "\r\n") {
		return nil
	}
	if event != "" {
		fmt.Fprintf(&s.buf, "event: %s\n", event)
	}
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		fmt.Fprintf(&s.buf, "data: %s\n", scanner.Text())
	}
	s.buf.WriteByte('\n')
	return scanner.Err()
}
func (s *SSE) Comment(v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v = strings.ReplaceAll(v, "\n", "\\n")
	v = strings.ReplaceAll(v, "\r", "\\r")
	fmt.Fprintf(&s.buf, ": %s\n\n", v)
}

type Redactor struct {
	Keys        []string
	Replacement string
}

func DefaultRedactor() *Redactor {
	return &Redactor{Keys: []string{"password", "token", "secret", "authorization", "cookie", "set-cookie", "api_key", "access_token", "refresh_token"}, Replacement: "[REDACTED]"}
}
func (r *Redactor) RedactString(s string) string {
	if r == nil {
		return s
	}
	out := s
	for _, k := range r.Keys {
		out = redactKey(out, k, r.Replacement)
	}
	return out
}
func redactKey(s, k, repl string) string {
	low := strings.ToLower(s)
	kl := strings.ToLower(k)
	for {
		i := strings.Index(low, kl+"=")
		if i < 0 {
			return s
		}
		j := i + len(k) + 1
		e := j
		for e < len(s) && s[e] != '&' && s[e] != ' ' && s[e] != ';' {
			e++
		}
		s = s[:j] + repl + s[e:]
		low = strings.ToLower(s)
	}
}

// Security events.
type SecurityEvent struct {
	Type, RequestID, Path, Method, IP string
	Data                              map[string]any
	Time                              time.Time
}
type SecurityEventSink interface{ Emit(SecurityEvent) }
type SecurityEventStream struct {
	mu     sync.RWMutex
	sinks  []SecurityEventSink
	recent []SecurityEvent
	max    int
}

func NewSecurityEventStream(max int) *SecurityEventStream {
	if max <= 0 {
		max = 1024
	}
	return &SecurityEventStream{max: max}
}

var defaultSecurityEvents = NewSecurityEventStream(1024)

func (s *SecurityEventStream) AddSink(sink SecurityEventSink) {
	s.mu.Lock()
	s.sinks = append(s.sinks, sink)
	s.mu.Unlock()
}
func (s *SecurityEventStream) Emit(e SecurityEvent) {
	s.mu.Lock()
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	s.recent = append(s.recent, e)
	if len(s.recent) > s.max {
		s.recent = s.recent[len(s.recent)-s.max:]
	}
	sinks := append([]SecurityEventSink(nil), s.sinks...)
	s.mu.Unlock()
	for _, sink := range sinks {
		sink.Emit(e)
	}
}
func (s *SecurityEventStream) Handler() HandlerFunc {
	return func(c Ctx) error {
		s.mu.RLock()
		out := append([]SecurityEvent(nil), s.recent...)
		s.mu.RUnlock()
		return c.JSON(out)
	}
}
func EmitSecurityEvent(c Ctx, typ string, data map[string]any) {
	rid, _ := c.Locals("request_id").(string)
	defaultSecurityEvents.Emit(SecurityEvent{Type: typ, RequestID: rid, Path: c.Path(), Method: c.Method(), IP: c.IP(), Data: data, Time: time.Now().UTC()})
}
func (a *App) EnableSecurityEvents(path string) *SecurityEventStream {
	if path == "" {
		path = "/_fh/security-events"
	}
	a.Get(path, defaultSecurityEvents.Handler())
	return defaultSecurityEvents
}

// Lifecycle state and compensation.
type LifecycleState string

const (
	LifecycleReceived    LifecycleState = "received"
	LifecycleValidated   LifecycleState = "validated"
	LifecycleAuthorized  LifecycleState = "authorized"
	LifecycleAccepted    LifecycleState = "accepted"
	LifecycleQueued      LifecycleState = "queued"
	LifecycleProcessing  LifecycleState = "processing"
	LifecycleCompleted   LifecycleState = "completed"
	LifecycleFailed      LifecycleState = "failed"
	LifecycleCompensated LifecycleState = "compensated"
)

type RequestLifecycle struct {
	mu            sync.Mutex
	State         LifecycleState
	Events        []RequestJournalEntry
	compensations []func(context.Context) error
}

func (c *DefaultCtx) Lifecycle() *RequestLifecycle {
	if v, ok := c.Locals("fh.lifecycle").(*RequestLifecycle); ok {
		return v
	}
	l := &RequestLifecycle{State: LifecycleReceived}
	c.Locals("fh.lifecycle", l)
	return l
}
func (l *RequestLifecycle) Mark(c Ctx, state LifecycleState) {
	l.mu.Lock()
	l.State = state
	l.Events = append(l.Events, RequestJournalEntry{RequestID: fmt.Sprint(c.Locals("request_id")), Event: string(state), Method: c.Method(), Path: c.Path(), Status: c.StatusCode(), Time: time.Now().UTC()})
	l.mu.Unlock()
}
func (c *DefaultCtx) Compensate(fn func(context.Context) error) {
	l := c.Lifecycle()
	l.mu.Lock()
	l.compensations = append(l.compensations, fn)
	l.mu.Unlock()
}
func (c *DefaultCtx) RunCompensations() error {
	l := c.Lifecycle()
	l.mu.Lock()
	list := append([]func(context.Context) error(nil), l.compensations...)
	l.mu.Unlock()
	for i := len(list) - 1; i >= 0; i-- {
		if err := list[i](c.Context()); err != nil {
			return err
		}
	}
	l.Mark(c, LifecycleCompensated)
	return nil
}

// Data sensitivity and secure envelope.
type DataPolicy struct {
	Sensitivity   string
	RedactLogs    bool
	EncryptAtRest bool
	JournalMode   string
	KeyID         string
	Key           []byte
}
type SecureEnvelope struct {
	Version    int       `json:"version"`
	KeyID      string    `json:"key_id"`
	Nonce      []byte    `json:"nonce,omitempty"`
	Ciphertext []byte    `json:"ciphertext,omitempty"`
	Plaintext  []byte    `json:"plaintext,omitempty"`
	BodyHash   string    `json:"body_hash"`
	CreatedAt  time.Time `json:"created_at"`
}

func SealEnvelope(policy DataPolicy, b []byte) (SecureEnvelope, error) {
	if len(policy.Key) < 16 {
		return SecureEnvelope{}, errors.New("fh: envelope key must be at least 16 bytes")
	}
	env := SecureEnvelope{Version: 2, KeyID: policy.KeyID, BodyHash: hashBody(b), CreatedAt: time.Now().UTC()}
	if policy.EncryptAtRest {
		block, err := aes.NewCipher(policy.Key)
		if err != nil {
			return env, err
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return env, err
		}
		nonce := make([]byte, gcm.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return env, err
		}
		env.Nonce = nonce
		aad := envelopeAAD(env.Version, env.KeyID)
		env.Ciphertext = gcm.Seal(nil, nonce, b, aad)
		return env, nil
	}
	env.Plaintext = append([]byte(nil), b...)
	return env, nil
}

// ErrEnvelopeHashMismatch is returned when the BodyHash in a SecureEnvelope
// does not match the decrypted plaintext, indicating possible tampering.
var ErrEnvelopeHashMismatch = errors.New("fh: envelope body hash mismatch")

// ErrEnvelopeAADMismatch is returned when AAD verification fails during
// envelope decryption, indicating the KeyID or Version was tampered with.
var ErrEnvelopeAADMismatch = errors.New("fh: envelope AAD verification failed")

func OpenEnvelope(policy DataPolicy, env SecureEnvelope) ([]byte, error) {
	if len(policy.Key) < 16 {
		return nil, errors.New("fh: envelope key must be at least 16 bytes")
	}
	if len(env.Ciphertext) == 0 {
		plaintext := append([]byte(nil), env.Plaintext...)
		if env.BodyHash != "" && hashBody(plaintext) != env.BodyHash {
			return nil, ErrEnvelopeHashMismatch
		}
		return plaintext, nil
	}
	block, err := aes.NewCipher(policy.Key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	var aad []byte
	if env.Version >= 2 {
		aad = envelopeAAD(env.Version, env.KeyID)
	}
	plaintext, err := gcm.Open(nil, env.Nonce, env.Ciphertext, aad)
	if err != nil {
		if env.Version >= 2 {
			return nil, ErrEnvelopeAADMismatch
		}
		return nil, err
	}
	if env.BodyHash != "" && hashBody(plaintext) != env.BodyHash {
		return nil, ErrEnvelopeHashMismatch
	}
	return plaintext, nil
}

// envelopeAAD builds additional authenticated data from envelope metadata.
// This binds the ciphertext to its key ID and version so an attacker cannot
// swap ciphertext between different keys or envelope versions.
func envelopeAAD(version int, keyID string) []byte {
	return []byte(fmt.Sprintf("v%d:%s", version, keyID))
}

// Maintenance.
type MaintenanceReport struct {
	Queue     QueueStats `json:"queue"`
	Compacted bool       `json:"compacted"`
}

func (r *Reliability) Compact(ctx context.Context) (MaintenanceReport, error) {
	var rep MaintenanceReport
	if r == nil {
		return rep, nil
	}
	if r.queue != nil {
		st, err := r.queue.store.Stats(ctx)
		if err != nil {
			return rep, err
		}
		rep.Queue = st
	}
	rep.Compacted = true
	return rep, nil
}
func (r *Reliability) Repair(ctx context.Context) error {
	if r != nil && r.queue != nil {
		return r.queue.store.Recover(ctx)
	}
	return nil
}

// DeterministicIdempotencyKey creates a stable key from deterministic parts.
func DeterministicIdempotencyKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "idem_" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
