// Package gateway bridges browser fetch/JSON and streaming NDJSON to MIRA UDP.
// Browsers cannot open arbitrary UDP sockets. The gateway is a distinct trust
// boundary; it does not provide end-to-end UDP security to a browser.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

// Authenticate MUST independently verify each incoming browser request. It
// returns a stable principal; gateway sessions are always scoped to it.
type Authenticate func(*http.Request) (string, error)

// DialBackend MUST apply that principal's authorization policy on the backend,
// not merely authenticate a shared service account.
type DialBackend func(context.Context, string) (*mira.Session, error)

type Config struct {
	Authenticate            Authenticate
	DialBackend             DialBackend
	BindPrincipal           func(context.Context, *mira.Session, protocol.Channel, string) error
	RequirePrincipalBinding bool // Required for multi-principal gateway deployments.
	MaxSessions             int
	IdleTimeout             time.Duration
	History                 int
	// MaxHistoryBytes bounds encoded event history retained for one session.
	MaxHistoryBytes int64
	// MaxTotalHistoryBytes bounds encoded event history across all sessions.
	MaxTotalHistoryBytes    int64
	AllowedOrigins          []string // Explicit HTTPS origins; no wildcards permitted.
	MaxSessionsPerPrincipal int
	AllowPlaintextLoopback  bool // integration/local development only
}
type Gateway struct {
	c                  Config
	mu                 sync.Mutex
	sessions           map[string]*browserSession
	pending            int
	pendingByPrincipal map[string]int
	historyBytes       int64
	closed             bool
	done               chan struct{}
	wg                 sync.WaitGroup
}
type Event struct {
	Cursor uint64 `json:"cursor"`
	Data   string `json:"data"`
}
type browserSession struct {
	id, owner    string
	conn         *mira.Session
	ch           protocol.Channel
	last         time.Time
	next         uint64
	history      []Event
	historyBytes int64
	changed      chan struct{}
	closed       bool
}

func New(c Config) (*Gateway, error) {
	if c.Authenticate == nil || c.DialBackend == nil || (c.RequirePrincipalBinding && c.BindPrincipal == nil) {
		return nil, errors.New("gateway: authenticating frontend and backend dialer are required")
	}
	if c.MaxSessions <= 0 {
		c.MaxSessions = 512
	}
	if c.MaxSessionsPerPrincipal <= 0 {
		c.MaxSessionsPerPrincipal = 4
	}
	if c.MaxSessionsPerPrincipal > c.MaxSessions {
		c.MaxSessionsPerPrincipal = c.MaxSessions
	}
	for _, origin := range c.AllowedOrigins {
		u, err := url.Parse(origin)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.String() != origin || strings.Contains(origin, "*") {
			return nil, errors.New("gateway: allowed origins must be exact HTTPS origins without wildcards")
		}
	}
	if c.MaxSessions > 100000 {
		return nil, errors.New("gateway: excessive session cap")
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 10 * time.Minute
	}
	if c.History <= 0 {
		c.History = 128
	}
	if c.History > 4096 {
		return nil, errors.New("gateway: excessive event history")
	}
	if c.MaxHistoryBytes <= 0 {
		c.MaxHistoryBytes = 1 << 20
	}
	if c.MaxTotalHistoryBytes <= 0 {
		c.MaxTotalHistoryBytes = 64 << 20
	}
	if c.MaxHistoryBytes > c.MaxTotalHistoryBytes {
		return nil, errors.New("gateway: per-session history budget exceeds total history budget")
	}
	g := &Gateway{c: c, sessions: make(map[string]*browserSession), pendingByPrincipal: make(map[string]int), done: make(chan struct{})}
	g.wg.Add(1)
	go g.sweep()
	return g, nil
}

// InvalidateAllSessions closes active backend sessions after emergency
// authentication-policy or revocation changes. This deliberately over-revokes
// rather than allowing long-lived browser streams to bypass the new policy.
func (g *Gateway) InvalidateAllSessions() {
	if g == nil {
		return
	}
	g.mu.Lock()
	all := g.sessions
	g.sessions = make(map[string]*browserSession)
	for _, bs := range all {
		if !bs.closed {
			bs.closed = true
			close(bs.changed)
		}
		g.clearHistoryLocked(bs)
	}
	g.mu.Unlock()
	for _, bs := range all {
		_ = bs.conn.Close(context.Background())
	}
}

func (g *Gateway) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	close(g.done)
	all := g.sessions
	g.sessions = make(map[string]*browserSession)
	for _, ss := range all {
		ss.closed = true
		close(ss.changed)
		g.clearHistoryLocked(ss)
	}
	g.mu.Unlock()
	for _, ss := range all {
		_ = ss.conn.Close(context.Background())
	}
	g.wg.Wait()
	return nil
}
func (g *Gateway) sweep() {
	defer g.wg.Done()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-g.done:
			return
		case <-tick.C:
			g.expire()
		}
	}
}
func (g *Gateway) expire() {
	now := time.Now()
	var old []*browserSession
	g.mu.Lock()
	for key, ss := range g.sessions {
		if now.Sub(ss.last) > g.c.IdleTimeout {
			ss.closed = true
			close(ss.changed)
			delete(g.sessions, key)
			g.clearHistoryLocked(ss)
			old = append(old, ss)
		}
	}
	g.mu.Unlock()
	for _, ss := range old {
		_ = ss.conn.Close(context.Background())
	}
}
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/connect", g.connect)
	mux.HandleFunc("DELETE /v1/connect", g.disconnect)
	mux.HandleFunc("POST /v1/send", g.send)
	mux.HandleFunc("GET /v1/pull", g.pull)
	mux.HandleFunc("GET /v1/stream", g.stream)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Vary", "Origin")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		origin := r.Header.Get("Origin")
		allowed := false
		for _, known := range g.c.AllowedOrigins {
			if origin == known {
				allowed = true
				break
			}
		}
		if origin != "" && !allowed {
			http.Error(w, "origin forbidden", http.StatusForbidden)
			return
		}
		if r.Header.Get("Sec-Fetch-Site") == "cross-site" && !allowed {
			http.Error(w, "cross-site request forbidden", http.StatusForbidden)
			return
		}
		if allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Max-Age", "300")
		}
		if r.Method == http.MethodOptions {
			if !allowed {
				http.Error(w, "preflight forbidden", http.StatusForbidden)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.TLS == nil {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil || !g.c.AllowPlaintextLoopback || !net.ParseIP(ip).IsLoopback() {
				http.Error(w, "TLS required", http.StatusUpgradeRequired)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
func (g *Gateway) principal(w http.ResponseWriter, r *http.Request) (string, bool) {
	owner, err := g.c.Authenticate(r)
	if err != nil || owner == "" || len(owner) > 256 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", false
	}
	return owner, true
}
func (g *Gateway) capacityLocked(owner string) bool {
	if g.closed || len(g.sessions)+g.pending >= g.c.MaxSessions {
		return false
	}
	count := 0
	for _, bs := range g.sessions {
		if bs.owner == owner {
			count++
		}
	}
	return count+g.pendingByPrincipal[owner] < g.c.MaxSessionsPerPrincipal
}

// releasePendingLocked removes reservation bookkeeping once a connect attempt
// no longer owns a slot. The caller holds g.mu.
func (g *Gateway) releasePendingLocked(owner string) {
	if g.pending > 0 {
		g.pending--
	}
	if count := g.pendingByPrincipal[owner]; count <= 1 {
		delete(g.pendingByPrincipal, owner)
	} else {
		g.pendingByPrincipal[owner] = count - 1
	}
}

func (g *Gateway) connect(w http.ResponseWriter, r *http.Request) {
	owner, ok := g.principal(w, r)
	if !ok {
		return
	}
	g.mu.Lock()
	if !g.capacityLocked(owner) {
		g.mu.Unlock()
		http.Error(w, "capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	// Reserve the slot BEFORE dialing the backend. Otherwise concurrent
	// connect requests can create an unbounded number of upstream handshakes.
	g.pending++
	g.pendingByPrincipal[owner]++
	g.mu.Unlock()
	reserved := true
	defer func() {
		g.mu.Lock()
		if reserved {
			g.releasePendingLocked(owner)
		}
		g.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	ss, err := g.c.DialBackend(ctx, owner)
	if err != nil {
		http.Error(w, "backend unavailable", http.StatusBadGateway)
		return
	}
	ch, err := ss.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		_ = ss.Close(context.Background())
		http.Error(w, "backend channel unavailable", http.StatusBadGateway)
		return
	}
	if g.c.BindPrincipal != nil {
		if err := g.c.BindPrincipal(ctx, ss, ch, owner); err != nil {
			_ = ss.Close(context.Background())
			http.Error(w, "backend principal delegation failed", http.StatusBadGateway)
			return
		}
	}
	var token [32]byte
	if _, err = rand.Read(token[:]); err != nil {
		_ = ss.Close(context.Background())
		http.Error(w, "randomness failure", 500)
		return
	}
	id := base64.RawURLEncoding.EncodeToString(token[:])
	bs := &browserSession{id: id, owner: owner, conn: ss, ch: ch, last: time.Now(), changed: make(chan struct{})}
	g.mu.Lock()
	g.releasePendingLocked(owner)
	reserved = false
	if !g.capacityLocked(owner) {
		g.mu.Unlock()
		_ = ss.Close(context.Background())
		http.Error(w, "capacity exceeded", http.StatusServiceUnavailable)
		return
	}
	g.sessions[id] = bs
	g.wg.Add(1)
	g.mu.Unlock()
	go g.receive(bs)
	jsonResponse(w, map[string]any{"session": id, "expires_in_seconds": int(g.c.IdleTimeout.Seconds())})
}
func (g *Gateway) receive(bs *browserSession) {
	defer g.wg.Done()
	ctx := context.Background()
	for {
		m, err := bs.ch.Receive(ctx)
		if err != nil {
			break
		}
		g.mu.Lock()
		if bs.closed {
			g.mu.Unlock()
			break
		}
		bs.next++
		encodedBytes := int64(base64.StdEncoding.EncodedLen(len(m.Payload)))
		if encodedBytes+historyEventOverhead <= g.c.MaxHistoryBytes && encodedBytes+historyEventOverhead <= g.c.MaxTotalHistoryBytes {
			g.appendHistoryLocked(bs, Event{Cursor: bs.next, Data: base64.StdEncoding.EncodeToString(m.Payload)})
		}
		close(bs.changed)
		bs.changed = make(chan struct{})
		g.mu.Unlock()
	}
	g.mu.Lock()
	if !bs.closed {
		bs.closed = true
		close(bs.changed)
		delete(g.sessions, bs.id)
		g.clearHistoryLocked(bs)
	}
	g.mu.Unlock()
	_ = bs.conn.Close(context.Background())
}
func (g *Gateway) lookup(w http.ResponseWriter, r *http.Request, owner, id string) (*browserSession, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	bs := g.sessions[id]
	if bs == nil || bs.closed || bs.owner != owner {
		http.Error(w, "session not found", http.StatusNotFound)
		return nil, false
	}
	bs.last = time.Now()
	return bs, true
}
func (g *Gateway) disconnect(w http.ResponseWriter, r *http.Request) {
	owner, ok := g.principal(w, r)
	if !ok {
		return
	}
	bs, ok := g.lookup(w, r, owner, r.URL.Query().Get("session"))
	if !ok {
		return
	}
	g.mu.Lock()
	if !bs.closed {
		bs.closed = true
		close(bs.changed)
		delete(g.sessions, bs.id)
		g.clearHistoryLocked(bs)
	}
	g.mu.Unlock()
	_ = bs.conn.Close(context.Background())
	w.WriteHeader(http.StatusNoContent)
}

const historyEventOverhead = int64(32)

func (g *Gateway) appendHistoryLocked(bs *browserSession, event Event) {
	cost := int64(len(event.Data)) + historyEventOverhead
	if cost > g.c.MaxHistoryBytes || cost > g.c.MaxTotalHistoryBytes {
		return
	}
	for len(bs.history) > 0 && (len(bs.history) >= g.c.History || bs.historyBytes+cost > g.c.MaxHistoryBytes || g.historyBytes+cost > g.c.MaxTotalHistoryBytes) {
		oldest := bs.history[0]
		oldestCost := int64(len(oldest.Data)) + historyEventOverhead
		copy(bs.history, bs.history[1:])
		bs.history[len(bs.history)-1] = Event{}
		bs.history = bs.history[:len(bs.history)-1]
		bs.historyBytes -= oldestCost
		g.historyBytes -= oldestCost
	}
	if bs.historyBytes+cost > g.c.MaxHistoryBytes || g.historyBytes+cost > g.c.MaxTotalHistoryBytes {
		return
	}
	bs.history = append(bs.history, event)
	bs.historyBytes += cost
	g.historyBytes += cost
}

// clearHistoryLocked releases the gateway and per-session accounting. The
// caller holds g.mu.
func (g *Gateway) clearHistoryLocked(bs *browserSession) {
	if bs == nil || bs.historyBytes == 0 {
		return
	}
	g.historyBytes -= bs.historyBytes
	bs.historyBytes = 0
	bs.history = nil
}
func (g *Gateway) send(w http.ResponseWriter, r *http.Request) {
	owner, ok := g.principal(w, r)
	if !ok {
		return
	}
	mediaType, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaErr != nil || mediaType != "application/json" {
		http.Error(w, "JSON required", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 512<<10)
	var req struct {
		Session string `json:"session"`
		Data    string `json:"data"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(w, "invalid JSON", 400)
		return
	}
	bs, ok := g.lookup(w, r, owner, req.Session)
	if !ok {
		return
	}
	if len(req.Data) > base64.StdEncoding.EncodedLen(256<<10) {
		http.Error(w, "invalid payload", 400)
		return
	}
	payload, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil || len(payload) > 256<<10 {
		http.Error(w, "invalid payload", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	id, err := bs.ch.Send(ctx, payload)
	if err == nil {
		_, err = bs.ch.AwaitReceipt(ctx, id, protocol.AckTransport)
	}
	if err != nil {
		http.Error(w, "backend delivery failed", http.StatusBadGateway)
		return
	}
	jsonResponse(w, map[string]uint64{"message_id": id})
}
func (g *Gateway) read(bs *browserSession, after uint64) ([]Event, <-chan struct{}, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if bs.closed {
		return nil, nil, http.StatusGone
	}
	expected := after + 1
	for _, e := range bs.history {
		if e.Cursor <= after {
			continue
		}
		if e.Cursor != expected {
			return nil, nil, http.StatusConflict
		}
		expected = e.Cursor + 1
	}
	if expected != 0 && expected <= bs.next {
		return nil, nil, http.StatusConflict
	}
	events := make([]Event, 0)
	for _, e := range bs.history {
		if e.Cursor > after {
			events = append(events, e)
		}
	}
	return events, bs.changed, 0
}
func parseCursor(r *http.Request) (uint64, error) {
	return strconv.ParseUint(r.URL.Query().Get("cursor"), 10, 64)
}
func (g *Gateway) pull(w http.ResponseWriter, r *http.Request) {
	owner, ok := g.principal(w, r)
	if !ok {
		return
	}
	bs, ok := g.lookup(w, r, owner, r.URL.Query().Get("session"))
	if !ok {
		return
	}
	cursor, err := parseCursor(r)
	if err != nil {
		http.Error(w, "invalid cursor", 400)
		return
	}
	events, changed, status := g.read(bs, cursor)
	if status != 0 {
		http.Error(w, "cursor no longer retained", status)
		return
	}
	if len(events) == 0 {
		t := time.NewTimer(25 * time.Second)
		defer t.Stop()
		select {
		case <-changed:
		case <-r.Context().Done():
			return
		case <-t.C:
		}
		events, _, status = g.read(bs, cursor)
		if status != 0 {
			http.Error(w, "cursor no longer retained", status)
			return
		}
	}
	jsonResponse(w, map[string]any{"events": events})
}
func (g *Gateway) stream(w http.ResponseWriter, r *http.Request) {
	owner, ok := g.principal(w, r)
	if !ok {
		return
	}
	bs, ok := g.lookup(w, r, owner, r.URL.Query().Get("session"))
	if !ok {
		return
	}
	cursor, err := parseCursor(r)
	if err != nil {
		http.Error(w, "invalid cursor", 400)
		return
	}
	_, flushing := w.(http.Flusher)
	if !flushing {
		http.Error(w, "stream unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Accel-Buffering", "no")
	started := false
	for {
		events, changed, status := g.read(bs, cursor)
		if status != 0 {
			if !started {
				http.Error(w, "cursor no longer retained", status)
			}
			return
		}
		for _, e := range events {
			if err := json.NewEncoder(w).Encode(e); err != nil {
				return
			}
			started = true
			cursor = e.Cursor
		}
		w.(http.Flusher).Flush()
		started = true
		timer := time.NewTimer(15 * time.Second)
		select {
		case <-r.Context().Done():
			timer.Stop()
			return
		case <-g.done:
			timer.Stop()
			return
		case <-changed:
			timer.Stop()
		case <-timer.C:
			if _, err := io.WriteString(w, "{\"type\":\"heartbeat\"}\n"); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}
func jsonResponse(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
