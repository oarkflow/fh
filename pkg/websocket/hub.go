package websocket

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

// Event WebSocket layer.
//
// This file is designed to sit on top of the low-level WebSocket implementation
// in this same package. It uses these existing primitives from websocket.go:
// Conn, Writer, NewWriter, NewWithConfig, Config, Text, ClosePolicyViolation,
// CloseNormalClosure, CloseMessageTooBig, CloseGoingAway, CloseInternalServerErr,
// ErrWebSocketTooLarge, ErrWebSocketClosed, and close codes.
//
// Protocol envelope example:
// {
//   "type":"emit",
//   "id":"optional-request-id",
//   "replyTo":"optional-reply-id",
//   "event":"chat.message",
//   "topic":"tenant:acme",
//   "channel":"room:general",
//   "payload":{"text":"hello"},
//   "ts":1710000000000
// }

var (
	ErrEventClosed            = errors.New("event websocket closed")
	ErrEventUnauthorized      = errors.New("event websocket unauthorized")
	ErrEventForbidden         = errors.New("event websocket forbidden")
	ErrEventNotFound          = errors.New("event handler not found")
	ErrEventBadEnvelope       = errors.New("bad event envelope")
	ErrEventPayloadTooLarge   = errors.New("event payload too large")
	ErrEventTooManySubs       = errors.New("too many websocket subscriptions")
	ErrEventSlowClient        = errors.New("slow websocket client")
	ErrEventAckTimeout        = errors.New("event ack timeout")
	ErrEventDuplicateClientID = errors.New("duplicate websocket client id")
)

const (
	EnvelopeHello       = "hello"
	EnvelopeEmit        = "emit"
	EnvelopeAck         = "ack"
	EnvelopeError       = "error"
	EnvelopeSubscribe   = "subscribe"
	EnvelopeUnsubscribe = "unsubscribe"
	EnvelopePresence    = "presence"
	EnvelopePing        = "ping"
	EnvelopePong        = "pong"
)

const (
	ActionSubscribe = "subscribe"
	ActionPublish   = "publish"
	ActionEmit      = "emit"
	ActionNotify    = "notify"
	ActionPresence  = "presence"
)

const subscriptionSep = "\x00"

// Envelope is the JSON wire format used by both Go and TS/JS clients.
type Envelope struct {
	Type      string          `json:"type,omitempty"`
	ID        string          `json:"id,omitempty"`
	ReplyTo   string          `json:"replyTo,omitempty"`
	Event     string          `json:"event,omitempty"`
	Topic     string          `json:"topic,omitempty"`
	Channel   string          `json:"channel,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Error     string          `json:"error,omitempty"`
	Code      string          `json:"code,omitempty"`
	Timestamp int64           `json:"ts,omitempty"`
}

// PresencePayload is emitted when presence is enabled and clients join/leave.
type PresencePayload struct {
	Action    string `json:"action"`
	ClientID  string `json:"clientId"`
	Topic     string `json:"topic,omitempty"`
	Channel   string `json:"channel,omitempty"`
	Timestamp int64  `json:"ts"`
}

// HandlerContext is passed to event handlers.
type HandlerContext struct {
	context.Context
	Hub      *EventHub
	Conn     *EventConn
	Envelope Envelope
	Payload  json.RawMessage
}

func (c *HandlerContext) Bind(v any) error {
	if c == nil || len(c.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(c.Payload, v)
}

func (c *HandlerContext) Topic() string {
	if c == nil {
		return ""
	}
	return c.Envelope.Topic
}

func (c *HandlerContext) Channel() string {
	if c == nil {
		return ""
	}
	return c.Envelope.Channel
}

// Context is a backward-compatible alias for HandlerContext.
// Use websocket.Context or websocket.HandlerContext in application code.
type Context = HandlerContext

type Handler func(*HandlerContext) (any, error)
type Middleware func(Handler) Handler

// MetadataFunc extracts metadata from the HTTP upgrade request. Store only safe,
// trusted server-side fields here. Do not trust user controlled values unless
// verified in this function.
type MetadataFunc func(fh.Ctx) map[string]string

// AuthFunc runs for every non-ack envelope. Use it for session/JWT checks.
type AuthFunc func(*EventConn, Envelope) error

// AuthorizeFunc controls topic/channel access and server fanout policies.
type AuthorizeFunc func(*EventConn, string, string, string) error

// ClientIDFunc can derive a stable client ID from metadata. Return empty to use
// a generated random ID.
type ClientIDFunc func(fh.Ctx, map[string]string) string

// Clock exists to simplify deterministic testing.
type Clock func() time.Time

type EventHubConfig struct {
	WriterQueueSize int

	// MaxEnvelopeBytes limits the entire incoming JSON envelope.
	MaxEnvelopeBytes int

	// MaxPayloadBytes limits the payload JSON field after decode.
	MaxPayloadBytes int

	AckTimeout        time.Duration
	ClientIdleTimeout time.Duration
	CleanupInterval   time.Duration

	// MaxSubscriptions is per connection.
	MaxSubscriptions int

	// MaxPendingRequests is per connection for server-side request/ack tracking.
	MaxPendingRequests int

	// If false, client messages with Event=server.broadcast/server.notify are rejected.
	AllowClientBroadcast bool
	AllowClientNotify    bool

	// CloseSlowClient closes a connection when the writer queue is full. If false,
	// the message is dropped and Send returns ErrEventSlowClient.
	CloseSlowClient bool

	// EnablePresence emits presence messages to topic/channel subscribers.
	EnablePresence bool

	// SendHello sends an initial hello envelope after upgrade.
	SendHello bool

	// Debug adds debug fields to metrics/log callbacks only; it does not print.
	Debug bool

	Auth      AuthFunc
	Authorize AuthorizeFunc
	ClientID  ClientIDFunc
	Now       Clock

	OnError       func(*EventConn, error)
	OnConnect     func(*EventConn)
	OnDisconnect  func(*EventConn, error)
	OnSubscribe   func(*EventConn, string, string)
	OnUnsubscribe func(*EventConn, string, string)
}

func DefaultEventHubConfig() EventHubConfig {
	return EventHubConfig{
		WriterQueueSize:    256,
		MaxEnvelopeBytes:   1 << 20,
		MaxPayloadBytes:    1 << 20,
		AckTimeout:         10 * time.Second,
		ClientIdleTimeout:  2 * time.Minute,
		CleanupInterval:    30 * time.Second,
		MaxSubscriptions:   512,
		MaxPendingRequests: 1024,
		CloseSlowClient:    true,
		EnablePresence:     true,
		SendHello:          true,
		Now:                time.Now,
	}
}

func normalizeEventHubConfig(cfg EventHubConfig) EventHubConfig {
	def := DefaultEventHubConfig()
	if cfg.WriterQueueSize <= 0 {
		cfg.WriterQueueSize = def.WriterQueueSize
	}
	if cfg.MaxEnvelopeBytes <= 0 {
		cfg.MaxEnvelopeBytes = def.MaxEnvelopeBytes
	}
	if cfg.MaxPayloadBytes <= 0 {
		cfg.MaxPayloadBytes = def.MaxPayloadBytes
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = def.AckTimeout
	}
	if cfg.ClientIdleTimeout <= 0 {
		cfg.ClientIdleTimeout = def.ClientIdleTimeout
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = def.CleanupInterval
	}
	if cfg.MaxSubscriptions <= 0 {
		cfg.MaxSubscriptions = def.MaxSubscriptions
	}
	if cfg.MaxPendingRequests <= 0 {
		cfg.MaxPendingRequests = def.MaxPendingRequests
	}
	if cfg.Now == nil {
		cfg.Now = def.Now
	}
	return cfg
}

type EventStats struct {
	ConnectedClients   int64 `json:"connectedClients"`
	TotalConnections   int64 `json:"totalConnections"`
	TotalDisconnects   int64 `json:"totalDisconnects"`
	IncomingEnvelopes  int64 `json:"incomingEnvelopes"`
	OutgoingEnvelopes  int64 `json:"outgoingEnvelopes"`
	HandledEvents      int64 `json:"handledEvents"`
	HandlerErrors      int64 `json:"handlerErrors"`
	DroppedEnvelopes   int64 `json:"droppedEnvelopes"`
	Subscriptions      int64 `json:"subscriptions"`
	PublishedEnvelopes int64 `json:"publishedEnvelopes"`
	AckTimeouts        int64 `json:"ackTimeouts"`
}

type eventStatsAtomic struct {
	connectedClients  atomic.Int64
	totalConnections  atomic.Int64
	totalDisconnects  atomic.Int64
	incomingEnvelopes atomic.Int64
	outgoingEnvelopes atomic.Int64
	handledEvents     atomic.Int64
	handlerErrors     atomic.Int64
	droppedEnvelopes  atomic.Int64
	subscriptions     atomic.Int64
	published         atomic.Int64
	ackTimeouts       atomic.Int64
}

func (s *eventStatsAtomic) Snapshot() EventStats {
	return EventStats{
		ConnectedClients:   s.connectedClients.Load(),
		TotalConnections:   s.totalConnections.Load(),
		TotalDisconnects:   s.totalDisconnects.Load(),
		IncomingEnvelopes:  s.incomingEnvelopes.Load(),
		OutgoingEnvelopes:  s.outgoingEnvelopes.Load(),
		HandledEvents:      s.handledEvents.Load(),
		HandlerErrors:      s.handlerErrors.Load(),
		DroppedEnvelopes:   s.droppedEnvelopes.Load(),
		Subscriptions:      s.subscriptions.Load(),
		PublishedEnvelopes: s.published.Load(),
		AckTimeouts:        s.ackTimeouts.Load(),
	}
}

type EventHub struct {
	cfg EventHubConfig

	mu       sync.RWMutex
	clients  map[string]*EventConn
	index    map[string]map[string]*EventConn // subscriptionKey(topic, channel) -> client id -> conn
	handlers map[string]Handler
	mw       []Middleware
	any      Handler

	closed      atomic.Bool
	stopCleanup chan struct{}
	stopOnce    sync.Once
	stats       eventStatsAtomic
}

func NewEventHub(cfg ...EventHubConfig) *EventHub {
	c := DefaultEventHubConfig()
	if len(cfg) > 0 {
		c = normalizeEventHubConfig(cfg[0])
	} else {
		c = normalizeEventHubConfig(c)
	}
	h := &EventHub{
		cfg:         c,
		clients:     make(map[string]*EventConn),
		index:       make(map[string]map[string]*EventConn),
		handlers:    make(map[string]Handler),
		stopCleanup: make(chan struct{}),
	}
	h.On(EnvelopePing, func(ctx *HandlerContext) (any, error) {
		return map[string]any{"pong": true, "ts": h.now().UnixMilli()}, nil
	})
	go h.cleanupLoop()
	return h
}

func (h *EventHub) now() time.Time {
	if h == nil || h.cfg.Now == nil {
		return time.Now()
	}
	return h.cfg.Now()
}

// Handler returns an fh-compatible WebSocket upgrade handler. It performs the
// low-level RFC6455 upgrade, extracts trusted metadata from the HTTP request,
// registers the connection in the hub, and starts the event loop.
func (h *EventHub) Handler(wsCfg Config, metadata MetadataFunc) fh.HandlerFunc {
	return NewEventHandler(h, wsCfg, metadata)
}

// HandlerWithContext is kept as a clearer alias for Handler.
func (h *EventHub) HandlerWithContext(wsCfg Config, metadata MetadataFunc) fh.HandlerFunc {
	return NewEventHandler(h, wsCfg, metadata)
}

// NewEventHandler is the production-ready combined HTTP upgrade + event hub
// integration. It intentionally mirrors NewWithConfig from the low-level layer
// so metadata and client IDs can be derived from fh.Ctx before the context is
// no longer safe to read.
func NewEventHandler(h *EventHub, wsCfg Config, metadata MetadataFunc) fh.HandlerFunc {
	if h == nil {
		h = NewEventHub()
	}
	wsCfg = normalizeConfig(wsCfg)
	if wsCfg.Manager == nil {
		wsCfg.Manager = NewManager()
	}

	return func(c fh.Ctx) error {
		if h.closed.Load() {
			return ErrEventClosed
		}
		if !isValidWebSocketRequest(c) {
			return ErrWebSocketHandshake
		}
		if !checkWebSocketOrigin(c, wsCfg) {
			return ErrWebSocketHandshake
		}

		key := fh.TrimOWS(c.RequestHeader().Peek([]byte("Sec-WebSocket-Key")))
		decoded, err := base64.StdEncoding.DecodeString(string(key))
		if err != nil || len(decoded) != 16 {
			return ErrWebSocketHandshake
		}

		selectedProtocol := selectSubprotocol(
			string(c.RequestHeader().Peek([]byte("Sec-WebSocket-Protocol"))),
			wsCfg.Subprotocols,
		)
		c.Set("Sec-WebSocket-Accept", Accept(key))
		if selectedProtocol != "" {
			c.Set("Sec-WebSocket-Protocol", selectedProtocol)
		}

		meta := map[string]string{}
		if metadata != nil {
			meta = metadata(c)
			if meta == nil {
				meta = map[string]string{}
			}
		}

		clientID := ""
		if h.cfg.ClientID != nil {
			clientID = h.cfg.ClientID(c, meta)
		}

		return c.Upgrade("websocket", func(conn net.Conn) error {
			ws := newConn(conn, wsCfg, selectedProtocol)

			if wsCfg.Manager != nil {
				wsCfg.Manager.Add(ws)
				defer wsCfg.Manager.Remove(ws)
			}
			if wsCfg.OnOpen != nil {
				wsCfg.OnOpen(ws)
			}

			var heartbeatDone chan struct{}
			if wsCfg.EnableHeartbeat && wsCfg.PingInterval > 0 {
				heartbeatDone = make(chan struct{})
				ws.StartHeartbeat(wsCfg.PingInterval, wsCfg.PongTimeout, []byte("ping"), heartbeatDone)
			}
			defer func() {
				if heartbeatDone != nil {
					close(heartbeatDone)
				}
				_ = ws.Close()
			}()

			ec, err := h.Accept(ws, meta, clientID)
			if err != nil {
				if wsCfg.OnError != nil {
					wsCfg.OnError(ws, err)
				}
				return err
			}

			err = h.Serve(ec)
			if wsCfg.OnClose != nil {
				wsCfg.OnClose(ws, err)
			}
			return err
		})
	}
}

// Accept registers an already-upgraded Conn in the hub. Use this from your own
// NewWithConfig callback when you need exact metadata control.
func (h *EventHub) Accept(ws *Conn, metadata map[string]string, requestedID string) (*EventConn, error) {
	if h == nil || ws == nil || h.closed.Load() {
		return nil, ErrEventClosed
	}
	if metadata == nil {
		metadata = make(map[string]string)
	}
	id := strings.TrimSpace(requestedID)
	if id == "" {
		id = newEventID()
	}
	if !validToken(id, 128) {
		return nil, ErrEventBadEnvelope
	}

	ec := &EventConn{
		ID:        id,
		Hub:       h,
		Conn:      ws,
		Writer:    NewWriter(ws, h.cfg.WriterQueueSize),
		Meta:      copyStringMap(metadata),
		subs:      make(map[string]Subscription),
		pending:   make(map[string]chan Envelope),
		createdAt: h.now(),
		lastSeen:  h.now(),
	}

	h.mu.Lock()
	if _, exists := h.clients[id]; exists {
		h.mu.Unlock()
		_ = ec.Close(ClosePolicyViolation, "duplicate client id")
		return nil, ErrEventDuplicateClientID
	}
	h.clients[id] = ec
	h.mu.Unlock()

	h.stats.connectedClients.Add(1)
	h.stats.totalConnections.Add(1)

	if h.cfg.SendHello {
		_ = ec.Send(Envelope{Type: EnvelopeHello, Event: EnvelopeHello, Payload: mustRaw(map[string]any{
			"clientId":   id,
			"serverTime": h.now().UnixMilli(),
		})})
	}
	if h.cfg.OnConnect != nil {
		h.cfg.OnConnect(ec)
	}
	return ec, nil
}

// Add registers an already-upgraded Conn with generated client ID.
// It is a convenience wrapper kept for application code that does:
//
//	ec := hub.Add(conn, metadata)
//	return hub.Serve(ec)
//
// Prefer Accept when you want to handle duplicate IDs or registration errors explicitly.
func (h *EventHub) Add(ws *Conn, metadata map[string]string) *EventConn {
	ec, err := h.Accept(ws, metadata, "")
	if err != nil {
		return nil
	}
	return ec
}

// AddWithID registers an already-upgraded Conn using a caller-supplied client ID.
// It returns nil on registration failure. Prefer Accept when the error must be surfaced.
func (h *EventHub) AddWithID(ws *Conn, metadata map[string]string, clientID string) *EventConn {
	ec, err := h.Accept(ws, metadata, clientID)
	if err != nil {
		return nil
	}
	return ec
}

func (h *EventHub) Use(m Middleware) {
	if h == nil || m == nil {
		return
	}
	h.mu.Lock()
	h.mw = append(h.mw, m)
	h.mu.Unlock()
}

func (h *EventHub) On(event string, handler Handler, middleware ...Middleware) {
	if h == nil || handler == nil {
		return
	}
	event = normalizeName(event)
	if event == "" || !validEventName(event) {
		return
	}
	for i := len(middleware) - 1; i >= 0; i-- {
		if middleware[i] != nil {
			handler = middleware[i](handler)
		}
	}
	h.mu.Lock()
	h.handlers[event] = handler
	h.mu.Unlock()
}

// OnAny receives unmatched events after normal lookup fails.
func (h *EventHub) OnAny(handler Handler) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.any = handler
	h.mu.Unlock()
}

func (h *EventHub) Off(event string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.handlers, normalizeName(event))
	h.mu.Unlock()
}

func (h *EventHub) Serve(c *EventConn) error {
	if h == nil || c == nil {
		return ErrEventClosed
	}
	var cause error
	defer func() { h.Remove(c, cause) }()
	for {
		op, data, err := c.Conn.ReadMessage()
		if err != nil {
			cause = err
			return err
		}
		if op != Text && op != Binary {
			continue
		}
		if h.cfg.MaxEnvelopeBytes > 0 && len(data) > h.cfg.MaxEnvelopeBytes {
			cause = ErrWebSocketTooLarge
			_ = c.Close(CloseMessageTooBig, "event envelope too large")
			return ErrWebSocketTooLarge
		}
		c.touch(h.now())
		h.stats.incomingEnvelopes.Add(1)

		var env Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			h.reportError(c, err)
			_ = c.SendError("", "bad_json", "invalid JSON envelope")
			continue
		}
		if env.Timestamp == 0 {
			env.Timestamp = h.now().UnixMilli()
		}
		if err := h.handleEnvelope(c, env); err != nil {
			h.reportError(c, err)
			if env.ID != "" || env.Type != EnvelopeError {
				_ = c.SendError(env.ID, codeForEventError(err), err.Error())
			}
		}
	}
}

func (h *EventHub) handleEnvelope(c *EventConn, env Envelope) error {
	if h == nil || c == nil || c.closed.Load() {
		return ErrEventClosed
	}
	env.Type = normalizeName(env.Type)
	if env.Type == "" {
		env.Type = EnvelopeEmit
	}
	env.Event = normalizeName(env.Event)
	env.Topic = normalizeName(env.Topic)
	env.Channel = normalizeName(env.Channel)

	if !validEnvelope(env, h.cfg.MaxPayloadBytes) {
		return ErrEventBadEnvelope
	}

	switch env.Type {
	case EnvelopeAck, EnvelopeError:
		c.resolvePending(env)
		return nil
	case EnvelopePong:
		c.touch(h.now())
		return nil
	}

	if h.cfg.Auth != nil {
		if err := h.cfg.Auth(c, env); err != nil {
			return err
		}
	}

	switch env.Type {
	case EnvelopeSubscribe:
		if err := h.Subscribe(c, env.Topic, env.Channel); err != nil {
			return err
		}
		if env.ID != "" {
			return c.Ack(env.ID, map[string]any{"subscribed": true, "topic": env.Topic, "channel": env.Channel})
		}
		return nil
	case EnvelopeUnsubscribe:
		if err := h.Unsubscribe(c, env.Topic, env.Channel); err != nil {
			return err
		}
		if env.ID != "" {
			return c.Ack(env.ID, map[string]any{"unsubscribed": true, "topic": env.Topic, "channel": env.Channel})
		}
		return nil
	case EnvelopePing:
		if env.ID != "" {
			return c.Ack(env.ID, map[string]any{"pong": true, "ts": h.now().UnixMilli()})
		}
		return c.Send(Envelope{Type: EnvelopePong, Event: EnvelopePong, Payload: env.Payload})
	case EnvelopeEmit:
		return h.dispatch(c, env)
	default:
		return ErrEventBadEnvelope
	}
}

func (h *EventHub) dispatch(c *EventConn, env Envelope) error {
	if env.Event == "server.broadcast" {
		if !h.cfg.AllowClientBroadcast {
			return ErrEventForbidden
		}
		if err := h.authorize(c, ActionPublish, env.Topic, env.Channel); err != nil {
			return err
		}
		out := Envelope{Type: EnvelopeEmit, Event: env.Event, Topic: env.Topic, Channel: env.Channel, Payload: env.Payload}
		return h.Broadcast(env.Topic, env.Channel, out)
	}
	if env.Event == "server.notify" {
		if !h.cfg.AllowClientNotify {
			return ErrEventForbidden
		}
		if err := h.authorize(c, ActionNotify, env.Topic, env.Channel); err != nil {
			return err
		}
		out := Envelope{Type: EnvelopeEmit, Event: env.Event, Topic: env.Topic, Channel: env.Channel, Payload: env.Payload}
		return h.Notify(env.Topic, env.Channel, out)
	}

	if err := h.authorize(c, ActionEmit, env.Topic, env.Channel); err != nil {
		return err
	}

	handler, mw := h.lookup(env.Event)
	if handler == nil {
		return ErrEventNotFound
	}
	for i := len(mw) - 1; i >= 0; i-- {
		if mw[i] != nil {
			handler = mw[i](handler)
		}
	}

	ctx := &HandlerContext{Context: context.Background(), Hub: h, Conn: c, Envelope: env, Payload: env.Payload}
	res, err := handler(ctx)
	h.stats.handledEvents.Add(1)
	if err != nil {
		h.stats.handlerErrors.Add(1)
		if env.ID != "" {
			_ = c.SendError(env.ID, codeForEventError(err), err.Error())
		}
		return err
	}
	if env.ID != "" {
		return c.Ack(env.ID, res)
	}
	return nil
}

func (h *EventHub) lookup(event string) (Handler, []Middleware) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if handler := h.handlers[event]; handler != nil {
		return handler, append([]Middleware(nil), h.mw...)
	}
	// Prefix wildcard: chat.message.created can match chat.* or chat.message.*.
	best := ""
	for pattern := range h.handlers {
		if strings.HasSuffix(pattern, ".*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(event, prefix) && len(pattern) > len(best) {
				best = pattern
			}
		}
	}
	if best != "" {
		return h.handlers[best], append([]Middleware(nil), h.mw...)
	}
	return h.any, append([]Middleware(nil), h.mw...)
}

func (h *EventHub) authorize(c *EventConn, action, topic, channel string) error {
	if h.cfg.Authorize == nil {
		return nil
	}
	return h.cfg.Authorize(c, action, topic, channel)
}

func (h *EventHub) Subscribe(c *EventConn, topic, channel string) error {
	if h == nil || c == nil || c.closed.Load() {
		return ErrEventClosed
	}
	topic = normalizeName(topic)
	channel = normalizeName(channel)
	if topic == "" && channel == "" {
		return ErrEventBadEnvelope
	}
	if !validTopicOrChannel(topic) || !validTopicOrChannel(channel) {
		return ErrEventBadEnvelope
	}
	if err := h.authorize(c, ActionSubscribe, topic, channel); err != nil {
		return err
	}
	key := subscriptionKey(topic, channel)

	h.mu.Lock()
	if _, exists := c.subs[key]; !exists && len(c.subs) >= h.cfg.MaxSubscriptions {
		h.mu.Unlock()
		return ErrEventTooManySubs
	}
	if h.index[key] == nil {
		h.index[key] = make(map[string]*EventConn)
	}
	h.index[key][c.ID] = c
	c.subs[key] = Subscription{Topic: topic, Channel: channel, CreatedAt: h.now()}
	h.mu.Unlock()

	h.stats.subscriptions.Add(1)
	if h.cfg.OnSubscribe != nil {
		h.cfg.OnSubscribe(c, topic, channel)
	}
	if h.cfg.EnablePresence {
		_ = h.emitPresence("join", c.ID, topic, channel)
	}
	return nil
}

func (h *EventHub) Unsubscribe(c *EventConn, topic, channel string) error {
	if h == nil || c == nil {
		return ErrEventClosed
	}
	topic = normalizeName(topic)
	channel = normalizeName(channel)
	key := subscriptionKey(topic, channel)

	h.mu.Lock()
	deleteFromEventIndex(h.index, key, c.ID)
	delete(c.subs, key)
	h.mu.Unlock()

	if h.cfg.OnUnsubscribe != nil {
		h.cfg.OnUnsubscribe(c, topic, channel)
	}
	if h.cfg.EnablePresence {
		_ = h.emitPresence("leave", c.ID, topic, channel)
	}
	return nil
}

func (h *EventHub) emitPresence(action, clientID, topic, channel string) error {
	payload := PresencePayload{Action: action, ClientID: clientID, Topic: topic, Channel: channel, Timestamp: h.now().UnixMilli()}
	return h.Broadcast(topic, channel, Envelope{Type: EnvelopePresence, Event: EnvelopePresence, Topic: topic, Channel: channel, Payload: mustRaw(payload)}, BroadcastOptions{SkipClientID: clientID})
}

func (h *EventHub) Remove(c *EventConn, cause error) {
	if h == nil || c == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		h.mu.Lock()
		delete(h.clients, c.ID)
		subs := make([]Subscription, 0, len(c.subs))
		for key, sub := range c.subs {
			deleteFromEventIndex(h.index, key, c.ID)
			subs = append(subs, sub)
		}
		c.subs = make(map[string]Subscription)
		h.mu.Unlock()

		c.closePending()
		_ = c.Writer.CloseWithStatus(CloseNormalClosure, "")
		h.stats.connectedClients.Add(-1)
		h.stats.totalDisconnects.Add(1)

		if h.cfg.EnablePresence {
			for _, sub := range subs {
				_ = h.emitPresence("leave", c.ID, sub.Topic, sub.Channel)
			}
		}
		if h.cfg.OnDisconnect != nil {
			h.cfg.OnDisconnect(c, cause)
		}
	})
}

func (h *EventHub) Close() error {
	if h == nil {
		return nil
	}
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}
	h.stopOnce.Do(func() { close(h.stopCleanup) })
	clients := h.Clients()
	for _, c := range clients {
		_ = c.Close(CloseGoingAway, "server shutting down")
	}
	return nil
}

func (h *EventHub) Count() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	n := len(h.clients)
	h.mu.RUnlock()
	return n
}

func (h *EventHub) Stats() EventStats { return h.stats.Snapshot() }

func (h *EventHub) Clients() []*EventConn {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*EventConn, 0, len(h.clients))
	for _, c := range h.clients {
		if !c.closed.Load() {
			out = append(out, c)
		}
	}
	return out
}

func (h *EventHub) Client(id string) *EventConn {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	c := h.clients[id]
	h.mu.RUnlock()
	return c
}

func (h *EventHub) Subscriptions() []Subscription {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]Subscription, 0)
	for _, c := range h.clients {
		for _, sub := range c.subs {
			out = append(out, sub)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic == out[j].Topic {
			return out[i].Channel < out[j].Channel
		}
		return out[i].Topic < out[j].Topic
	})
	return out
}

type BroadcastOptions struct {
	SkipClientID      string
	OnlyClientID      string
	RequireSubscribed bool
}

func (h *EventHub) EmitTo(clientID, event string, payload any) error {
	c := h.Client(clientID)
	if c == nil {
		return ErrEventNotFound
	}
	return c.Emit(event, payload)
}

func (h *EventHub) RequestTo(clientID, event string, payload any, timeout time.Duration) (Envelope, error) {
	c := h.Client(clientID)
	if c == nil {
		return Envelope{}, ErrEventNotFound
	}
	return c.Request(event, payload, timeout)
}

func (h *EventHub) Broadcast(topic, channel string, env Envelope, opts ...BroadcastOptions) error {
	return h.fanout(ActionPublish, topic, channel, env, firstBroadcastOption(opts))
}

func (h *EventHub) Notify(topic, channel string, env Envelope, opts ...BroadcastOptions) error {
	if env.Type == "" {
		env.Type = EnvelopeEmit
	}
	if env.Event == "" {
		env.Event = "notification"
	}
	return h.fanout(ActionNotify, topic, channel, env, firstBroadcastOption(opts))
}

func (h *EventHub) BroadcastEvent(topic, channel, event string, payload any, opts ...BroadcastOptions) error {
	b, err := marshalEventPayload(payload)
	if err != nil {
		return err
	}
	return h.Broadcast(topic, channel, Envelope{Type: EnvelopeEmit, Event: event, Topic: topic, Channel: channel, Payload: b}, opts...)
}

func (h *EventHub) NotifyEvent(topic, channel, event string, payload any, opts ...BroadcastOptions) error {
	b, err := marshalEventPayload(payload)
	if err != nil {
		return err
	}
	return h.Notify(topic, channel, Envelope{Type: EnvelopeEmit, Event: event, Topic: topic, Channel: channel, Payload: b}, opts...)
}

func (h *EventHub) fanout(_ string, topic, channel string, env Envelope, opt BroadcastOptions) error {
	if h == nil || h.closed.Load() {
		return ErrEventClosed
	}
	topic = normalizeName(topic)
	channel = normalizeName(channel)
	if !validTopicOrChannel(topic) || !validTopicOrChannel(channel) {
		return ErrEventBadEnvelope
	}
	env.Topic = topic
	env.Channel = channel
	if env.Type == "" {
		env.Type = EnvelopeEmit
	}
	if env.Timestamp == 0 {
		env.Timestamp = h.now().UnixMilli()
	}
	list := h.match(topic, channel, opt)
	for _, c := range list {
		if err := c.Send(env); err != nil {
			h.stats.droppedEnvelopes.Add(1)
		}
	}
	h.stats.published.Add(1)
	return nil
}

func (h *EventHub) match(topic, channel string, opt BroadcastOptions) []*EventConn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	seen := make(map[string]*EventConn)
	add := func(m map[string]*EventConn) {
		for id, c := range m {
			if opt.SkipClientID != "" && id == opt.SkipClientID {
				continue
			}
			if opt.OnlyClientID != "" && id != opt.OnlyClientID {
				continue
			}
			if !c.closed.Load() {
				seen[id] = c
			}
		}
	}
	if opt.OnlyClientID != "" {
		if c := h.clients[opt.OnlyClientID]; c != nil && !c.closed.Load() {
			seen[c.ID] = c
		}
	} else if topic == "" && channel == "" && !opt.RequireSubscribed {
		add(h.clients)
	} else {
		// Exact topic+channel first.
		add(h.index[subscriptionKey(topic, channel)])
		// Topic-wide subscribers.
		if topic != "" {
			add(h.index[subscriptionKey(topic, "")])
		}
		// Channel-wide subscribers.
		if channel != "" {
			add(h.index[subscriptionKey("", channel)])
		}
	}
	out := make([]*EventConn, 0, len(seen))
	for _, c := range seen {
		out = append(out, c)
	}
	return out
}

func (h *EventHub) cleanupLoop() {
	ticker := time.NewTicker(h.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.cleanupIdle()
		case <-h.stopCleanup:
			return
		}
	}
}

func (h *EventHub) cleanupIdle() {
	if h == nil || h.cfg.ClientIdleTimeout <= 0 {
		return
	}
	deadline := h.now().Add(-h.cfg.ClientIdleTimeout)
	for _, c := range h.Clients() {
		if c.LastSeen().Before(deadline) {
			_ = c.Close(CloseGoingAway, "event idle timeout")
			h.Remove(c, ErrEventClosed)
		}
	}
}

func (h *EventHub) reportError(c *EventConn, err error) {
	if err == nil {
		return
	}
	if h != nil && h.cfg.OnError != nil {
		h.cfg.OnError(c, err)
	}
}

type Subscription struct {
	Topic     string    `json:"topic"`
	Channel   string    `json:"channel"`
	CreatedAt time.Time `json:"createdAt"`
}

type EventConn struct {
	ID     string            `json:"id"`
	Hub    *EventHub         `json:"-"`
	Conn   *Conn             `json:"-"`
	Writer *Writer           `json:"-"`
	Meta   map[string]string `json:"meta,omitempty"`

	mu        sync.RWMutex
	subs      map[string]Subscription
	createdAt time.Time
	lastSeen  time.Time
	closed    atomic.Bool
	closeOnce sync.Once

	pendingMu sync.Mutex
	pending   map[string]chan Envelope
}

func (c *EventConn) Send(env Envelope) error {
	if c == nil || c.closed.Load() {
		return ErrEventClosed
	}
	if env.Timestamp == 0 {
		env.Timestamp = time.Now().UnixMilli()
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if c.Hub != nil && c.Hub.cfg.MaxEnvelopeBytes > 0 && len(b) > c.Hub.cfg.MaxEnvelopeBytes {
		return ErrEventPayloadTooLarge
	}
	if !c.Writer.Send(Text, b) {
		if c.Hub != nil {
			c.Hub.stats.droppedEnvelopes.Add(1)
		}
		if c.Hub != nil && c.Hub.cfg.CloseSlowClient {
			_ = c.Close(ClosePolicyViolation, "slow client")
		}
		return ErrEventSlowClient
	}
	if c.Hub != nil {
		c.Hub.stats.outgoingEnvelopes.Add(1)
	}
	return nil
}

func (c *EventConn) SendRawJSON(raw []byte) error {
	if c == nil || c.closed.Load() {
		return ErrEventClosed
	}
	if !json.Valid(raw) {
		return ErrEventBadEnvelope
	}
	if c.Hub != nil && c.Hub.cfg.MaxEnvelopeBytes > 0 && len(raw) > c.Hub.cfg.MaxEnvelopeBytes {
		return ErrEventPayloadTooLarge
	}
	if !c.Writer.Send(Text, raw) {
		return ErrEventSlowClient
	}
	if c.Hub != nil {
		c.Hub.stats.outgoingEnvelopes.Add(1)
	}
	return nil
}

func (c *EventConn) Emit(event string, payload any) error {
	b, err := marshalEventPayload(payload)
	if err != nil {
		return err
	}
	return c.Send(Envelope{Type: EnvelopeEmit, Event: normalizeName(event), Payload: b})
}

func (c *EventConn) EmitScoped(topic, channel, event string, payload any) error {
	b, err := marshalEventPayload(payload)
	if err != nil {
		return err
	}
	return c.Send(Envelope{Type: EnvelopeEmit, Event: normalizeName(event), Topic: normalizeName(topic), Channel: normalizeName(channel), Payload: b})
}

func (c *EventConn) Ack(replyTo string, payload any) error {
	b, err := marshalEventPayload(payload)
	if err != nil {
		return err
	}
	return c.Send(Envelope{Type: EnvelopeAck, ReplyTo: replyTo, Payload: b})
}

func (c *EventConn) SendError(replyTo, code, msg string) error {
	return c.Send(Envelope{Type: EnvelopeError, ReplyTo: replyTo, Code: code, Error: msg})
}

func (c *EventConn) Request(event string, payload any, timeout time.Duration) (Envelope, error) {
	return c.RequestScoped("", "", event, payload, timeout)
}

func (c *EventConn) RequestScoped(topic, channel, event string, payload any, timeout time.Duration) (Envelope, error) {
	if c == nil || c.closed.Load() {
		return Envelope{}, ErrEventClosed
	}
	if timeout <= 0 {
		if c.Hub != nil {
			timeout = c.Hub.cfg.AckTimeout
		} else {
			timeout = 10 * time.Second
		}
	}
	id := newEventID()
	ch := make(chan Envelope, 1)
	if err := c.addPending(id, ch); err != nil {
		return Envelope{}, err
	}
	defer c.removePending(id)
	b, err := marshalEventPayload(payload)
	if err != nil {
		return Envelope{}, err
	}
	if err := c.Send(Envelope{Type: EnvelopeEmit, ID: id, Event: normalizeName(event), Topic: normalizeName(topic), Channel: normalizeName(channel), Payload: b}); err != nil {
		return Envelope{}, err
	}
	select {
	case env, ok := <-ch:
		if !ok {
			return Envelope{}, ErrEventClosed
		}
		if env.Type == EnvelopeError {
			return env, fmt.Errorf("%s: %s", env.Code, env.Error)
		}
		return env, nil
	case <-time.After(timeout):
		if c.Hub != nil {
			c.Hub.stats.ackTimeouts.Add(1)
		}
		return Envelope{}, ErrEventAckTimeout
	}
}

func (c *EventConn) addPending(id string, ch chan Envelope) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if c.Hub != nil && len(c.pending) >= c.Hub.cfg.MaxPendingRequests {
		return ErrEventForbidden
	}
	c.pending[id] = ch
	return nil
}

func (c *EventConn) removePending(id string) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

func (c *EventConn) resolvePending(env Envelope) {
	id := env.ReplyTo
	if id == "" {
		id = env.ID
	}
	if id == "" {
		return
	}
	c.pendingMu.Lock()
	ch := c.pending[id]
	c.pendingMu.Unlock()
	if ch != nil {
		select {
		case ch <- env:
		default:
		}
	}
}

func (c *EventConn) closePending() {
	c.pendingMu.Lock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()
}

func (c *EventConn) Join(topic, channel string) error {
	if c == nil || c.Hub == nil {
		return ErrEventClosed
	}
	return c.Hub.Subscribe(c, topic, channel)
}

func (c *EventConn) Leave(topic, channel string) error {
	if c == nil || c.Hub == nil {
		return ErrEventClosed
	}
	return c.Hub.Unsubscribe(c, topic, channel)
}

func (c *EventConn) Subscriptions() []Subscription {
	if c == nil {
		return nil
	}
	if c.Hub != nil {
		c.Hub.mu.RLock()
		defer c.Hub.mu.RUnlock()
	} else {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}
	out := make([]Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		out = append(out, sub)
	}
	return out
}

func (c *EventConn) CreatedAt() time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.RLock()
	t := c.createdAt
	c.mu.RUnlock()
	return t
}
func (c *EventConn) LastSeen() time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.RLock()
	t := c.lastSeen
	c.mu.RUnlock()
	return t
}
func (c *EventConn) Closed() bool { return c == nil || c.closed.Load() }

func (c *EventConn) SetMeta(key, value string) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	if c.Meta == nil {
		c.Meta = make(map[string]string)
	}
	c.Meta[key] = value
	c.mu.Unlock()
}

func (c *EventConn) GetMeta(key string) string {
	if c == nil {
		return ""
	}
	c.mu.RLock()
	v := c.Meta[key]
	c.mu.RUnlock()
	return v
}

func (c *EventConn) touch(t time.Time) { c.mu.Lock(); c.lastSeen = t; c.mu.Unlock() }

func (c *EventConn) Close(code uint16, reason string) error {
	if c == nil {
		return nil
	}
	c.closed.Store(true)
	if c.Writer != nil {
		return c.Writer.CloseWithStatus(code, reason)
	}
	if c.Conn != nil {
		return c.Conn.CloseWithStatus(code, reason)
	}
	return nil
}

// Production middleware.
func RecoverMiddleware(onPanic func(any)) Middleware {
	return func(next Handler) Handler {
		return func(ctx *HandlerContext) (res any, err error) {
			defer func() {
				if r := recover(); r != nil {
					if onPanic != nil {
						onPanic(r)
					}
					err = errors.New("panic in websocket event handler")
				}
			}()
			return next(ctx)
		}
	}
}

func RequireMeta(key, expected string) Middleware {
	return func(next Handler) Handler {
		return func(ctx *HandlerContext) (any, error) {
			if ctx == nil || ctx.Conn == nil {
				return nil, ErrEventUnauthorized
			}
			actual := ctx.Conn.GetMeta(key)
			if expected != "" && actual != expected {
				return nil, ErrEventForbidden
			}
			if expected == "" && actual == "" {
				return nil, ErrEventUnauthorized
			}
			return next(ctx)
		}
	}
}

func RequireTopicPrefix(prefix string) Middleware {
	return func(next Handler) Handler {
		return func(ctx *HandlerContext) (any, error) {
			if prefix != "" && !strings.HasPrefix(ctx.Envelope.Topic, prefix) {
				return nil, ErrEventForbidden
			}
			return next(ctx)
		}
	}
}

func MaxPayloadMiddleware(max int) Middleware {
	return func(next Handler) Handler {
		return func(ctx *HandlerContext) (any, error) {
			if max > 0 && len(ctx.Payload) > max {
				return nil, ErrEventPayloadTooLarge
			}
			return next(ctx)
		}
	}
}

func JSONOnlyMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx *HandlerContext) (any, error) {
			if len(ctx.Payload) > 0 && !json.Valid(ctx.Payload) {
				return nil, ErrEventBadEnvelope
			}
			return next(ctx)
		}
	}
}

func firstBroadcastOption(opts []BroadcastOptions) BroadcastOptions {
	if len(opts) == 0 {
		return BroadcastOptions{}
	}
	return opts[0]
}

func deleteFromEventIndex(index map[string]map[string]*EventConn, key, id string) {
	m := index[key]
	if m == nil {
		return
	}
	delete(m, id)
	if len(m) == 0 {
		delete(index, key)
	}
}

func subscriptionKey(topic, channel string) string { return topic + subscriptionSep + channel }

func normalizeName(s string) string { return strings.TrimSpace(s) }

func validEnvelope(env Envelope, maxPayload int) bool {
	if !validToken(env.Type, 32) {
		return false
	}
	if env.ID != "" && !validToken(env.ID, 128) {
		return false
	}
	if env.ReplyTo != "" && !validToken(env.ReplyTo, 128) {
		return false
	}
	if env.Event != "" && !validEventName(env.Event) {
		return false
	}
	if !validTopicOrChannel(env.Topic) || !validTopicOrChannel(env.Channel) {
		return false
	}
	if maxPayload > 0 && len(env.Payload) > maxPayload {
		return false
	}
	return true
}

func validEventName(s string) bool      { return validScopedName(s, 128, true) }
func validTopicOrChannel(s string) bool { return validScopedName(s, 256, true) }
func validToken(s string, max int) bool { return validScopedName(s, max, false) }

func validScopedName(s string, max int, allowSlash bool) bool {
	if s == "" {
		return true
	}
	if len(s) > max {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '.' || c == ':' || c == '_' || c == '-':
		case allowSlash && c == '/':
		case c == '*' && i == len(s)-1:
		default:
			return false
		}
	}
	return true
}

func marshalEventPayload(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case json.RawMessage:
		if len(x) > 0 && !json.Valid(x) {
			return nil, ErrEventBadEnvelope
		}
		return x, nil
	case []byte:
		if len(x) == 0 {
			return nil, nil
		}
		if json.Valid(x) {
			return json.RawMessage(append([]byte(nil), x...)), nil
		}
		return json.Marshal(string(x))
	case string:
		return json.Marshal(x)
	default:
		return json.Marshal(v)
	}
}

func mustRaw(v any) json.RawMessage { b, _ := marshalEventPayload(v); return b }

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func codeForEventError(err error) string {
	switch {
	case errors.Is(err, ErrEventUnauthorized):
		return "unauthorized"
	case errors.Is(err, ErrEventForbidden):
		return "forbidden"
	case errors.Is(err, ErrEventNotFound):
		return "not_found"
	case errors.Is(err, ErrEventBadEnvelope):
		return "bad_envelope"
	case errors.Is(err, ErrEventPayloadTooLarge):
		return "payload_too_large"
	case errors.Is(err, ErrEventTooManySubs):
		return "too_many_subscriptions"
	case errors.Is(err, ErrEventSlowClient):
		return "slow_client"
	case errors.Is(err, ErrEventAckTimeout):
		return "ack_timeout"
	case errors.Is(err, ErrEventClosed):
		return "closed"
	default:
		return "internal_error"
	}
}

func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
