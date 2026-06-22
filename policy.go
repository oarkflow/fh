package fh

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is a tiny dependency-free in-process metrics collector.
type Metrics struct {
	started        time.Time
	requests       atomic.Uint64
	inflight       atomic.Int64
	status         sync.Map
	route          sync.Map
	panics         atomic.Uint64
	idemReplays    atomic.Uint64
	securityEvents atomic.Uint64
}

func NewMetrics() *Metrics { return &Metrics{started: time.Now()} }
func (m *Metrics) Middleware() HandlerFunc {
	return func(c *Ctx) error {
		m.requests.Add(1)
		m.inflight.Add(1)
		start := time.Now()
		defer func() {
			m.inflight.Add(-1)
			code := strconv.Itoa(c.StatusCode())
			v, _ := m.status.LoadOrStore(code, &atomic.Uint64{})
			v.(*atomic.Uint64).Add(1)
			key := c.Method() + " " + c.Path()
			rv, _ := m.route.LoadOrStore(key, &atomic.Uint64{})
			rv.(*atomic.Uint64).Add(1)
			c.Set("Server-Timing", fmt.Sprintf("app;dur=%d", time.Since(start).Milliseconds()))
		}()
		return c.Next()
	}
}
func (m *Metrics) Handler() HandlerFunc {
	return func(c *Ctx) error {
		stats := Map{"uptime_seconds": int64(time.Since(m.started).Seconds()), "requests_total": m.requests.Load(), "requests_inflight": m.inflight.Load(), "panics_total": m.panics.Load(), "idempotency_replays_total": m.idemReplays.Load(), "security_events_total": m.securityEvents.Load(), "status": mapFromSync(&m.status), "routes": mapFromSync(&m.route)}
		if c.Query("format") == "prometheus" {
			var b strings.Builder
			b.WriteString(fmt.Sprintf("fh_requests_total %d\nfh_requests_inflight %d\n", m.requests.Load(), m.inflight.Load()))
			return c.Type("text/plain; version=0.0.4").SendString(b.String())
		}
		return c.JSON(stats)
	}
}
func mapFromSync(m *sync.Map) map[string]uint64 {
	out := map[string]uint64{}
	m.Range(func(k, v any) bool {
		if u, ok := v.(*atomic.Uint64); ok {
			out[fmt.Sprint(k)] = u.Load()
		}
		return true
	})
	return out
}
func (a *App) EnableMetrics(path string) *Metrics {
	if path == "" {
		path = "/_fh/metrics"
	}
	m := NewMetrics()
	a.Use(m.Middleware())
	a.Get(path, m.Handler())
	return m
}

// Circuit breaker middleware.
type CircuitBreakerConfig struct {
	Name             string
	FailureThreshold int
	SuccessThreshold int
	ResetAfter       time.Duration
	IsFailure        func(*Ctx, error) bool
	OnOpen           func(*Ctx) error
}
type CircuitBreaker struct {
	cfg                 CircuitBreakerConfig
	mu                  sync.Mutex
	state               string
	failures, successes int
	opened              time.Time
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 1
	}
	if cfg.ResetAfter <= 0 {
		cfg.ResetAfter = 30 * time.Second
	}
	return &CircuitBreaker{cfg: cfg, state: "closed"}
}
func (b *CircuitBreaker) Middleware() HandlerFunc {
	return func(c *Ctx) error {
		b.mu.Lock()
		if b.state == "open" {
			if time.Since(b.opened) < b.cfg.ResetAfter {
				b.mu.Unlock()
				if b.cfg.OnOpen != nil {
					return b.cfg.OnOpen(c)
				}
				return c.Status(StatusServiceUnavailable).JSON(Map{"error": "circuit_open"})
			}
			b.state = "half-open"
		}
		b.mu.Unlock()
		err := c.Next()
		failed := err != nil || c.StatusCode() >= 500
		if b.cfg.IsFailure != nil {
			failed = b.cfg.IsFailure(c, err)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if failed {
			b.failures++
			b.successes = 0
			if b.failures >= b.cfg.FailureThreshold {
				b.state = "open"
				b.opened = time.Now()
			}
			return err
		}
		if b.state == "half-open" {
			b.successes++
			if b.successes >= b.cfg.SuccessThreshold {
				b.state = "closed"
				b.failures = 0
				b.successes = 0
			}
		} else {
			b.failures = 0
		}
		return err
	}
}
func CircuitBreakerMiddleware(cfg CircuitBreakerConfig) HandlerFunc {
	return NewCircuitBreaker(cfg).Middleware()
}

// Reverse proxy / API gateway.
type ProxyConfig struct {
	Target       string
	StripPrefix  string
	AddPrefix    string
	Timeout      time.Duration
	Director     func(*http.Request)
	ErrorHandler func(*Ctx, error) error
}

func ReverseProxy(cfg ProxyConfig) HandlerFunc {
	u, err := url.Parse(cfg.Target)
	if err != nil {
		panic(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	if cfg.Timeout > 0 {
		proxy.Transport = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: cfg.Timeout, ResponseHeaderTimeout: cfg.Timeout}
	}
	orig := proxy.Director
	proxy.Director = func(r *http.Request) {
		orig(r)
		if cfg.StripPrefix != "" {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, cfg.StripPrefix)
			if r.URL.Path == "" {
				r.URL.Path = "/"
			}
		}
		if cfg.AddPrefix != "" {
			r.URL.Path = cfg.AddPrefix + r.URL.Path
		}
		if cfg.Director != nil {
			cfg.Director(r)
		}
	}
	return func(c *Ctx) error {
		req, err := ctxToHTTPRequest(c, u)
		if err != nil {
			return err
		}
		rw := newProxyResponseWriter(c)
		proxy.ServeHTTP(rw, req)
		if rw.err != nil {
			if cfg.ErrorHandler != nil {
				return cfg.ErrorHandler(c, rw.err)
			}
			return rw.err
		}
		return nil
	}
}
func ctxToHTTPRequest(c *Ctx, target *url.URL) (*http.Request, error) {
	body := io.NopCloser(bytes.NewReader(c.Body()))
	req, err := http.NewRequestWithContext(c.Context(), c.Method(), target.String()+c.OriginalURL(), body)
	if err != nil {
		return nil, err
	}
	for k, vals := range c.GetReqHeaders() {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	req.RemoteAddr = c.IP()
	return req, nil
}

type proxyResponseWriter struct {
	c      *Ctx
	header http.Header
	code   int
	err    error
}

func newProxyResponseWriter(c *Ctx) *proxyResponseWriter {
	return &proxyResponseWriter{c: c, header: http.Header{}, code: StatusOK}
}
func (w *proxyResponseWriter) Header() http.Header { return w.header }
func (w *proxyResponseWriter) WriteHeader(code int) {
	w.code = code
	for k, vals := range w.header {
		for _, v := range vals {
			w.c.Set(k, v)
		}
	}
	w.c.Status(code)
}
func (w *proxyResponseWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(StatusOK)
	}
	w.err = w.c.SendBytes(b)
	if w.err != nil {
		return 0, w.err
	}
	return len(b), nil
}
func APIGateway(routes map[string]ProxyConfig) HandlerFunc {
	return func(c *Ctx) error {
		p := c.Path()
		var best string
		var cfg ProxyConfig
		for prefix, pc := range routes {
			if strings.HasPrefix(p, prefix) && len(prefix) > len(best) {
				best = prefix
				cfg = pc
			}
		}
		if best == "" {
			return c.Status(StatusBadGateway).JSON(Map{"error": "no_upstream"})
		}
		if cfg.StripPrefix == "" {
			cfg.StripPrefix = best
		}
		return ReverseProxy(cfg)(c)
	}
}

// SSE support.
type SSE struct {
	c   *Ctx
	buf bytes.Buffer
	mu  sync.Mutex
}

func (c *Ctx) SSE(fn func(*SSE) error) error {
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
func (s *SSE) Comment(v string) { s.mu.Lock(); defer s.mu.Unlock(); fmt.Fprintf(&s.buf, ": %s\n\n", v) }

// Static files.
type AdvancedStaticConfig struct {
	Root     string
	Index    string
	Prefix   string
	MaxAge   time.Duration
	ETag     bool
	Download bool
	Browse   bool
}

func StaticFilesAdvanced(cfg AdvancedStaticConfig) HandlerFunc {
	if cfg.Index == "" {
		cfg.Index = "index.html"
	}
	return func(c *Ctx) error {
		p := strings.TrimPrefix(c.Path(), cfg.Prefix)
		p = strings.TrimPrefix(p, "/")
		if strings.Contains(p, "..") {
			return c.Status(StatusForbidden).SendString("forbidden")
		}
		full := filepath.Join(cfg.Root, p)
		st, err := os.Stat(full)
		if err != nil {
			return c.Status(StatusNotFound).SendString("not found")
		}
		if st.IsDir() {
			idx := filepath.Join(full, cfg.Index)
			if _, err := os.Stat(idx); err == nil {
				full = idx
				st, _ = os.Stat(full)
			} else if !cfg.Browse {
				return c.Status(StatusForbidden).SendString("forbidden")
			}
		}
		if cfg.ETag {
			etag := fmt.Sprintf("%x-%x", st.ModTime().UnixNano(), st.Size())
			c.Set("ETag", etag)
			if c.Get("If-None-Match") == etag {
				return c.SendStatus(StatusNotModified)
			}
		}
		if cfg.MaxAge > 0 {
			c.Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(cfg.MaxAge.Seconds())))
		}
		if cfg.Download {
			c.Set("Content-Disposition", "attachment")
		}
		if ct := mime.TypeByExtension(filepath.Ext(full)); ct != "" {
			c.Type(ct)
		}
		b, err := os.ReadFile(full)
		if err != nil {
			return err
		}
		return c.SendBytes(b)
	}
}
func (a *App) StaticAdvanced(prefix, root string, cfg ...AdvancedStaticConfig) *App {
	c := AdvancedStaticConfig{Prefix: prefix, Root: root, ETag: true}
	if len(cfg) > 0 {
		c = cfg[0]
		c.Prefix = prefix
		c.Root = root
	}
	return a.Get(prefix+"/*", StaticFilesAdvanced(c))
}

// Access logs and redaction.
type AccessLogMode string

const (
	AccessLogJSON     AccessLogMode = "json"
	AccessLogCommon   AccessLogMode = "common"
	AccessLogCombined AccessLogMode = "combined"
)

type AccessLogConfig struct {
	Mode          AccessLogMode
	Logger        *log.Logger
	Redactor      *Redactor
	SlowThreshold time.Duration
}

func AccessLog(cfg AccessLogConfig) HandlerFunc {
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.Redactor == nil {
		cfg.Redactor = DefaultRedactor()
	}
	return func(c *Ctx) error {
		start := time.Now()
		err := c.Next()
		dur := time.Since(start)
		if cfg.SlowThreshold > 0 && dur < cfg.SlowThreshold {
			return err
		}
		path := cfg.Redactor.RedactString(c.OriginalURL())
		switch cfg.Mode {
		case AccessLogJSON:
			b, _ := json.Marshal(Map{"time": time.Now().UTC(), "request_id": c.Locals("request_id"), "method": c.Method(), "path": path, "status": c.StatusCode(), "latency_ms": dur.Milliseconds(), "ip": c.IP()})
			cfg.Logger.Println(string(b))
		default:
			cfg.Logger.Printf("%s %s %s %d %s", c.IP(), c.Method(), path, c.StatusCode(), dur)
		}
		return err
	}
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

// Signed requests.
type SignatureConfig struct {
	Secret          []byte
	SecretLookup    func(*Ctx) ([]byte, error)
	SignatureHeader string
	TimestampHeader string
	MaxSkew         time.Duration
	Replay          IdempotencyRepository
}

func SignedRequests(cfg SignatureConfig) HandlerFunc {
	if cfg.SignatureHeader == "" {
		cfg.SignatureHeader = "X-Signature"
	}
	if cfg.TimestampHeader == "" {
		cfg.TimestampHeader = "X-Timestamp"
	}
	if cfg.MaxSkew <= 0 {
		cfg.MaxSkew = 5 * time.Minute
	}
	return func(c *Ctx) error {
		sec := cfg.Secret
		var err error
		if cfg.SecretLookup != nil {
			sec, err = cfg.SecretLookup(c)
			if err != nil {
				return err
			}
		}
		if len(sec) == 0 {
			return errors.New("fh: signed request secret missing")
		}
		ts := c.Get(cfg.TimestampHeader)
		sig := c.Get(cfg.SignatureHeader)
		if ts == "" || sig == "" {
			EmitSecurityEvent(c, "signature.missing", nil)
			return c.Status(StatusUnauthorized).JSON(Map{"error": "signature_missing"})
		}
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil || time.Since(t) > cfg.MaxSkew || time.Until(t) > cfg.MaxSkew {
			EmitSecurityEvent(c, "signature.stale", nil)
			return c.Status(StatusUnauthorized).JSON(Map{"error": "signature_stale"})
		}
		mac := hmac.New(sha256.New, sec)
		mac.Write([]byte(ts))
		mac.Write([]byte("."))
		mac.Write(c.Body())
		expected := hex.EncodeToString(mac.Sum(nil))
		sig = strings.TrimPrefix(sig, "sha256=")
		if !hmac.Equal([]byte(expected), []byte(sig)) {
			EmitSecurityEvent(c, "signature.invalid", nil)
			return c.Status(StatusUnauthorized).JSON(Map{"error": "signature_invalid"})
		}
		return c.Next()
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
	return func(c *Ctx) error {
		s.mu.RLock()
		out := append([]SecurityEvent(nil), s.recent...)
		s.mu.RUnlock()
		return c.JSON(out)
	}
}
func EmitSecurityEvent(c *Ctx, typ string, data map[string]any) {
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
	LifecycleValidated                  = "validated"
	LifecycleAuthorized                 = "authorized"
	LifecycleAccepted                   = "accepted"
	LifecycleQueued                     = "queued"
	LifecycleProcessing                 = "processing"
	LifecycleCompleted                  = "completed"
	LifecycleFailed                     = "failed"
	LifecycleCompensated                = "compensated"
)

type RequestLifecycle struct {
	mu            sync.Mutex
	State         LifecycleState
	Events        []RequestJournalEntry
	compensations []func(context.Context) error
}

func (c *Ctx) Lifecycle() *RequestLifecycle {
	if v, ok := c.Locals("fh.lifecycle").(*RequestLifecycle); ok {
		return v
	}
	l := &RequestLifecycle{State: LifecycleReceived}
	c.Locals("fh.lifecycle", l)
	return l
}
func (l *RequestLifecycle) Mark(c *Ctx, state LifecycleState) {
	l.mu.Lock()
	l.State = state
	l.Events = append(l.Events, RequestJournalEntry{RequestID: fmt.Sprint(c.Locals("request_id")), Event: string(state), Method: c.Method(), Path: c.Path(), Status: c.StatusCode(), Time: time.Now().UTC()})
	l.mu.Unlock()
}
func (c *Ctx) Compensate(fn func(context.Context) error) {
	l := c.Lifecycle()
	l.mu.Lock()
	l.compensations = append(l.compensations, fn)
	l.mu.Unlock()
}
func (c *Ctx) RunCompensations() error {
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
	env := SecureEnvelope{Version: 1, KeyID: policy.KeyID, BodyHash: hashBody(b), CreatedAt: time.Now().UTC()}
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
		env.Ciphertext = gcm.Seal(nil, nonce, b, nil)
		return env, nil
	}
	env.Plaintext = append([]byte(nil), b...)
	return env, nil
}
func OpenEnvelope(policy DataPolicy, env SecureEnvelope) ([]byte, error) {
	if len(env.Ciphertext) == 0 {
		return append([]byte(nil), env.Plaintext...), nil
	}
	block, err := aes.NewCipher(policy.Key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, env.Nonce, env.Ciphertext, nil)
}

// Policy and contract firewall.
type RoutePolicy struct {
	Reliability ReliabilityPolicy
	Data        DataPolicy
	Contract    ContractPolicy
	Versions    VersionPolicy
	Actor       ActorPolicy
}

func Policy(p RoutePolicy) HandlerFunc {
	return func(c *Ctx) error {
		if p.Data.Sensitivity != "" {
			c.Locals("fh.data_policy", p.Data)
		}
		if p.Versions.Enabled {
			if err := APIVersion(p.Versions)(c); err != nil {
				return err
			}
			return nil
		}
		return c.Next()
	}
}

type ContractPolicy struct {
	Methods           []string
	ContentTypes      []string
	MaxBodyBytes      int
	RequireHeaders    []string
	RejectUnknownJSON bool
}

func ContractFirewall(p ContractPolicy) HandlerFunc {
	return func(c *Ctx) error {
		if len(p.Methods) > 0 && !containsFold(p.Methods, c.Method()) {
			return c.Status(StatusMethodNotAllowed).JSON(Map{"error": "method_not_allowed"})
		}
		if p.MaxBodyBytes > 0 && len(c.Body()) > p.MaxBodyBytes {
			return c.Status(StatusPayloadTooLarge).JSON(Map{"error": "body_too_large"})
		}
		if len(p.ContentTypes) > 0 && !containsPrefixFold(p.ContentTypes, c.Get(HeaderContentType)) {
			return c.Status(StatusUnsupportedMediaType).JSON(Map{"error": "unsupported_content_type"})
		}
		for _, h := range p.RequireHeaders {
			if c.Get(h) == "" {
				return c.Status(StatusBadRequest).JSON(Map{"error": "missing_header", "header": h})
			}
		}
		return c.Next()
	}
}
func containsFold(list []string, v string) bool {
	for _, x := range list {
		if strings.EqualFold(x, v) {
			return true
		}
	}
	return false
}
func containsPrefixFold(list []string, v string) bool {
	for _, x := range list {
		if strings.HasPrefix(strings.ToLower(v), strings.ToLower(x)) {
			return true
		}
	}
	return false
}

// Actors per key.
type ActorPolicy struct{ Key func(*Ctx) string }
type actorRegistry struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

var globalActors = &actorRegistry{locks: map[string]*sync.Mutex{}}

func Actor(p ActorPolicy) HandlerFunc {
	return func(c *Ctx) error {
		if p.Key == nil {
			return c.Next()
		}
		key := p.Key(c)
		if key == "" {
			return c.Next()
		}
		globalActors.mu.Lock()
		l := globalActors.locks[key]
		if l == nil {
			l = &sync.Mutex{}
			globalActors.locks[key] = l
		}
		globalActors.mu.Unlock()
		l.Lock()
		defer l.Unlock()
		return c.Next()
	}
}

// Simple workflow engine.
type Workflow struct {
	Name  string
	Steps []WorkflowStep
}
type WorkflowStep struct {
	Name      string
	Handler   HandlerFunc
	JobType   string
	Condition func(*Ctx) bool
}

func NewWorkflow(name string) *Workflow { return &Workflow{Name: name} }
func (w *Workflow) Use(name string, h HandlerFunc) *Workflow {
	w.Steps = append(w.Steps, WorkflowStep{Name: name, Handler: h})
	return w
}
func (w *Workflow) Job(name, typ string) *Workflow {
	w.Steps = append(w.Steps, WorkflowStep{Name: name, JobType: typ})
	return w
}
func (w *Workflow) Handler() HandlerFunc {
	return func(c *Ctx) error {
		for _, s := range w.Steps {
			if s.Condition != nil && !s.Condition(c) {
				continue
			}
			c.Lifecycle().Mark(c, LifecycleProcessing)
			if s.Handler != nil {
				if err := s.Handler(c); err != nil {
					return err
				}
			}
			if s.JobType != "" {
				id, err := AtomicHandoff(c, s.JobType, Map{"workflow": w.Name, "step": s.Name, "request_id": c.Locals("request_id")})
				if err != nil {
					return err
				}
				c.Locals("job_id", id)
			}
		}
		c.Lifecycle().Mark(c, LifecycleCompleted)
		return nil
	}
}

// API evolution/versioning.
type VersionPolicy struct {
	Enabled    bool
	Header     string
	Default    string
	Supported  []string
	Deprecated map[string]string
}

func APIVersion(p VersionPolicy) HandlerFunc {
	if p.Header == "" {
		p.Header = "Accept-Version"
	}
	return func(c *Ctx) error {
		v := c.Get(p.Header)
		if v == "" {
			v = p.Default
		}
		if len(p.Supported) > 0 && !containsFold(p.Supported, v) {
			return c.Status(StatusBadRequest).JSON(Map{"error": "unsupported_api_version", "version": v})
		}
		c.Locals("api_version", v)
		if msg := p.Deprecated[v]; msg != "" {
			c.Set("Sunset", msg)
			c.Set("Deprecation", "true")
		}
		return c.Next()
	}
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

func DeterministicIdempotency(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "idem_" + base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}
