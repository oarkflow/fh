package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/oarkflow/fh"
)

const (
	Continuation = byte(0x0)
	Text         = byte(0x1)
	Binary       = byte(0x2)
	Close        = byte(0x8)
	Ping         = byte(0x9)
	Pong         = byte(0xa)
)

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseMandatoryExtension      = 1010
	CloseInternalServerErr       = 1011
	CloseServiceRestart          = 1012
	CloseTryAgainLater           = 1013
	CloseTLSHandshake            = 1015
)

var (
	ErrWebSocketHandshake = errors.New("invalid websocket handshake")
	ErrWebSocketProtocol  = errors.New("websocket protocol error")
	ErrWebSocketTooLarge  = errors.New("websocket message too large")
	ErrWebSocketRateLimit = errors.New("websocket message rate limit exceeded")
	ErrWebSocketClosed    = errors.New("websocket closed")
)

type Config struct {
	MaxMessageSize int
	MaxFrameSize   int
	MaxFragments   int

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PingInterval time.Duration
	PongTimeout  time.Duration

	MaxMessagesPerSecond int

	// Secure default:
	// - If CheckOrigin is nil and AllowedOrigins is empty:
	//   browser requests with Origin are rejected, non-browser requests without Origin are allowed.
	// - Set AllowAllOrigins=true only for trusted/internal/dev use.
	AllowAllOrigins bool
	AllowedOrigins  []string
	CheckOrigin     func(fh.Ctx) bool

	Subprotocols []string

	EnableHeartbeat bool

	Manager *Manager

	OnOpen    func(*Conn)
	OnClose   func(*Conn, error)
	OnError   func(*Conn, error)
	OnMessage func(*Conn, byte, int64)
}

func DefaultConfig() Config {
	return Config{
		MaxMessageSize:       1 << 20,
		MaxFrameSize:         64 << 10,
		MaxFragments:         32,
		ReadTimeout:          90 * time.Second,
		WriteTimeout:         10 * time.Second,
		PingInterval:         30 * time.Second,
		PongTimeout:          75 * time.Second,
		MaxMessagesPerSecond: 128,
		EnableHeartbeat:      true,
	}
}

func normalizeConfig(cfg Config) Config {
	def := DefaultConfig()

	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = def.MaxMessageSize
	}
	if cfg.MaxFrameSize <= 0 {
		cfg.MaxFrameSize = def.MaxFrameSize
	}
	if cfg.MaxFrameSize > cfg.MaxMessageSize {
		cfg.MaxFrameSize = cfg.MaxMessageSize
	}
	if cfg.MaxFragments <= 0 {
		cfg.MaxFragments = def.MaxFragments
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = def.ReadTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = def.WriteTimeout
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = def.PingInterval
	}
	if cfg.PongTimeout <= 0 {
		cfg.PongTimeout = def.PongTimeout
	}
	if cfg.MaxMessagesPerSecond <= 0 {
		cfg.MaxMessagesPerSecond = def.MaxMessagesPerSecond
	}

	return cfg
}

type CloseError struct {
	Code   uint16
	Reason string
}

func (e *CloseError) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason == "" {
		return "websocket closed"
	}
	return e.Reason
}

func IsCloseError(err error, codes ...uint16) bool {
	var ce *CloseError
	if !errors.As(err, &ce) {
		return false
	}

	if len(codes) == 0 {
		return true
	}

	for _, code := range codes {
		if ce.Code == code {
			return true
		}
	}

	return false
}

func IsNormalClose(err error) bool {
	return err == nil ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) ||
		IsCloseError(err, CloseNormalClosure, CloseGoingAway, CloseNoStatusReceived)
}

func New(handler func(*Conn) error) fh.HandlerFunc {
	return NewWithConfig(DefaultConfig(), handler)
}

func NewWithConfig(cfg Config, handler func(*Conn) error) fh.HandlerFunc {
	cfg = normalizeConfig(cfg)

	return func(c fh.Ctx) error {
		if handler == nil {
			return ErrWebSocketHandshake
		}

		if !isValidWebSocketRequest(c) {
			return ErrWebSocketHandshake
		}

		if !checkWebSocketOrigin(c, cfg) {
			return ErrWebSocketHandshake
		}

		key := fh.TrimOWS(c.RequestHeader().Peek([]byte("Sec-WebSocket-Key")))

		decoded, err := base64.StdEncoding.DecodeString(string(key))
		if err != nil || len(decoded) != 16 {
			return ErrWebSocketHandshake
		}

		selectedProtocol := selectSubprotocol(
			string(c.RequestHeader().Peek([]byte("Sec-WebSocket-Protocol"))),
			cfg.Subprotocols,
		)

		c.Set("Sec-WebSocket-Accept", Accept(key))

		if selectedProtocol != "" {
			c.Set("Sec-WebSocket-Protocol", selectedProtocol)
		}

		return c.Upgrade("websocket", func(conn net.Conn) error {
			ws := newConn(conn, cfg, selectedProtocol)

			if cfg.Manager != nil {
				cfg.Manager.Add(ws)
				defer cfg.Manager.Remove(ws)
			}

			if cfg.OnOpen != nil {
				cfg.OnOpen(ws)
			}

			var heartbeatDone chan struct{}

			if cfg.EnableHeartbeat && cfg.PingInterval > 0 {
				heartbeatDone = make(chan struct{})
				ws.StartHeartbeat(cfg.PingInterval, cfg.PongTimeout, []byte("ping"), heartbeatDone)
			}

			defer func() {
				if heartbeatDone != nil {
					close(heartbeatDone)
				}
				_ = ws.Close()
			}()

			err := handler(ws)

			if cfg.OnClose != nil {
				cfg.OnClose(ws, err)
			}

			return err
		})
	}
}

func isValidWebSocketRequest(c fh.Ctx) bool {
	return fh.BytesEqualFold(c.RequestHeader().Method, fh.MethodGETBytes) &&
		c.RequestHeader().ContentLength == 0 &&
		!c.RequestHeader().Chunked &&
		fh.StrEqFold(fh.TrimOWS(c.RequestHeader().Peek([]byte("Upgrade"))), "websocket") &&
		fh.HasHeaderToken(c.RequestHeader().Peek(fh.HeaderConnectionBytes), "upgrade") &&
		fh.StrEqFold(fh.TrimOWS(c.RequestHeader().Peek([]byte("Sec-WebSocket-Version"))), "13") &&
		len(fh.TrimOWS(c.RequestHeader().Peek([]byte("Sec-WebSocket-Key")))) > 0
}

func checkWebSocketOrigin(c fh.Ctx, cfg Config) bool {
	if cfg.CheckOrigin != nil {
		return cfg.CheckOrigin(c)
	}

	origin := string(fh.TrimOWS(c.RequestHeader().Peek([]byte("Origin"))))

	if cfg.AllowAllOrigins {
		return true
	}

	if len(cfg.AllowedOrigins) == 0 {
		// Secure default:
		// allow non-browser clients without Origin,
		// reject browser-originated requests unless explicitly configured.
		return origin == ""
	}

	if origin == "" {
		return false
	}

	for _, allowed := range cfg.AllowedOrigins {
		if allowed == "*" || strings.EqualFold(origin, allowed) {
			return true
		}
	}

	return false
}

func Accept(key []byte) string {
	h := sha1.New()
	_, _ = h.Write(key)
	_, _ = h.Write([]byte(GUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func selectSubprotocol(clientHeader string, supported []string) string {
	if clientHeader == "" || len(supported) == 0 {
		return ""
	}

	requested := strings.Split(clientHeader, ",")

	for _, raw := range requested {
		clientProtocol := strings.TrimSpace(raw)
		if clientProtocol == "" || !validSubprotocolToken(clientProtocol) {
			continue
		}

		for _, serverProtocol := range supported {
			if !validSubprotocolToken(serverProtocol) {
				continue
			}

			if clientProtocol == serverProtocol {
				return serverProtocol
			}
		}
	}

	return ""
}

func validSubprotocolToken(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		c := s[i]

		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\'' ||
			c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_' ||
			c == '`' || c == '|' || c == '~':
		default:
			return false
		}
	}

	return true
}

type Conn struct {
	conn net.Conn

	readMu  sync.Mutex
	writeMu sync.Mutex

	closeOnce sync.Once
	closed    atomic.Bool

	maxMessage   int
	maxFrame     int
	maxFragments int

	readTimeout  time.Duration
	writeTimeout time.Duration

	selectedProtocol string

	lastPong atomic.Int64

	rateMu          sync.Mutex
	rateWindowStart time.Time
	rateCount       int
	rateLimit       int

	onError   func(*Conn, error)
	onMessage func(*Conn, byte, int64)
}

func newConn(conn net.Conn, cfg Config, selectedProtocol string) *Conn {
	ws := &Conn{
		conn:             conn,
		maxMessage:       cfg.MaxMessageSize,
		maxFrame:         cfg.MaxFrameSize,
		maxFragments:     cfg.MaxFragments,
		readTimeout:      cfg.ReadTimeout,
		writeTimeout:     cfg.WriteTimeout,
		selectedProtocol: selectedProtocol,
		rateLimit:        cfg.MaxMessagesPerSecond,
		onError:          cfg.OnError,
		onMessage:        cfg.OnMessage,
	}

	ws.markPong()

	return ws
}

func (c *Conn) ReadMessage() (opcode byte, payload []byte, err error) {
	op, r, err := c.NextReader()
	if err != nil {
		return 0, nil, err
	}
	defer r.Close()

	payload, err = io.ReadAll(r)
	if err != nil {
		return 0, nil, err
	}

	return op, payload, nil
}

func (c *Conn) NextReader() (opcode byte, r io.ReadCloser, err error) {
	c.readMu.Lock()

	if c.closed.Load() {
		c.readMu.Unlock()
		return 0, nil, ErrWebSocketClosed
	}

	mr := &MessageReader{
		c:       c,
		unlock:  c.readMu.Unlock,
		utf8val: newUTF8StreamValidator(),
	}

	op, err := mr.prepareFirstFrame()
	if err != nil {
		c.readMu.Unlock()
		c.emitError(err)
		return 0, nil, err
	}

	return op, mr, nil
}

type MessageReader struct {
	c *Conn

	firstOpcode byte

	currentPayload []byte
	currentOffset  int

	fragments int
	total     int64

	finalFrame bool
	finished   bool
	closed     bool

	utf8val *utf8StreamValidator

	unlock func()
}

func (r *MessageReader) prepareFirstFrame() (byte, error) {
	for {
		fin, op, payload, err := r.c.readFrame()
		if err != nil {
			return 0, err
		}

		switch op {
		case Ping:
			if err := r.c.WriteMessage(Pong, payload); err != nil {
				return 0, err
			}
			continue

		case Pong:
			r.c.markPong()
			continue

		case Close:
			if !validClosePayload(payload) {
				_ = r.c.CloseWithStatus(CloseProtocolError, "bad close frame")
				return 0, ErrWebSocketProtocol
			}

			code, reason := parseClosePayload(payload)
			_ = r.c.WriteMessage(Close, payload)

			return Close, &CloseError{
				Code:   code,
				Reason: reason,
			}

		case Text, Binary:
			r.firstOpcode = op
			r.fragments = 1
			r.currentPayload = payload
			r.currentOffset = 0
			r.total = int64(len(payload))
			r.finalFrame = fin

			if r.c.maxMessage > 0 && r.total > int64(r.c.maxMessage) {
				_ = r.c.CloseWithStatus(CloseMessageTooBig, "message too large")
				return 0, ErrWebSocketTooLarge
			}

			return op, nil

		case Continuation:
			_ = r.c.CloseWithStatus(CloseProtocolError, "unexpected continuation")
			return 0, ErrWebSocketProtocol

		default:
			_ = r.c.CloseWithStatus(CloseProtocolError, "bad opcode")
			return 0, ErrWebSocketProtocol
		}
	}
}

func (r *MessageReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, ErrWebSocketClosed
	}

	if len(p) == 0 {
		return 0, nil
	}

	for {
		if r.currentOffset < len(r.currentPayload) {
			n := copy(p, r.currentPayload[r.currentOffset:])
			chunk := p[:n]
			r.currentOffset += n

			if r.firstOpcode == Text {
				if err := r.utf8val.Write(chunk); err != nil {
					_ = r.c.CloseWithStatus(CloseInvalidFramePayloadData, "invalid utf-8")
					r.release()
					r.c.emitError(err)
					return 0, err
				}
			}

			return n, nil
		}

		if r.finalFrame {
			if err := r.finishMessage(); err != nil {
				r.release()
				r.c.emitError(err)
				return 0, err
			}

			r.release()
			return 0, io.EOF
		}

		if err := r.nextContinuationFrame(); err != nil {
			r.release()
			r.c.emitError(err)
			return 0, err
		}
	}
}

func (r *MessageReader) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	if !r.finished {
		// Drain remaining fragments so the connection can remain usable
		// if the caller abandons the reader before EOF.
		for !r.finalFrame {
			if err := r.nextContinuationFrame(); err != nil {
				r.release()
				return err
			}
		}

		for r.currentOffset < len(r.currentPayload) {
			if r.firstOpcode == Text {
				if err := r.utf8val.Write(r.currentPayload[r.currentOffset:]); err != nil {
					_ = r.c.CloseWithStatus(CloseInvalidFramePayloadData, "invalid utf-8")
					r.release()
					return err
				}
			}
			r.currentOffset = len(r.currentPayload)
		}

		if err := r.finishMessage(); err != nil {
			r.release()
			return err
		}
	}

	r.release()
	return nil
}

func (r *MessageReader) release() {
	if r.unlock != nil {
		r.unlock()
		r.unlock = nil
	}
}

func (r *MessageReader) nextContinuationFrame() error {
	for {
		fin, op, payload, err := r.c.readFrame()
		if err != nil {
			return err
		}

		switch op {
		case Ping:
			if err := r.c.WriteMessage(Pong, payload); err != nil {
				return err
			}
			continue

		case Pong:
			r.c.markPong()
			continue

		case Close:
			if !validClosePayload(payload) {
				_ = r.c.CloseWithStatus(CloseProtocolError, "bad close frame")
				return ErrWebSocketProtocol
			}

			code, reason := parseClosePayload(payload)
			_ = r.c.WriteMessage(Close, payload)

			return &CloseError{
				Code:   code,
				Reason: reason,
			}

		case Continuation:
			r.fragments++

			if r.c.maxFragments > 0 && r.fragments > r.c.maxFragments {
				_ = r.c.CloseWithStatus(CloseMessageTooBig, "too many fragments")
				return ErrWebSocketTooLarge
			}

			r.total += int64(len(payload))

			if r.c.maxMessage > 0 && r.total > int64(r.c.maxMessage) {
				_ = r.c.CloseWithStatus(CloseMessageTooBig, "message too large")
				return ErrWebSocketTooLarge
			}

			r.currentPayload = payload
			r.currentOffset = 0
			r.finalFrame = fin

			return nil

		case Text, Binary:
			_ = r.c.CloseWithStatus(CloseProtocolError, "new data frame before final continuation")
			return ErrWebSocketProtocol

		default:
			_ = r.c.CloseWithStatus(CloseProtocolError, "bad opcode")
			return ErrWebSocketProtocol
		}
	}
}

func (r *MessageReader) finishMessage() error {
	if r.finished {
		return nil
	}

	r.finished = true

	if r.firstOpcode == Text {
		if err := r.utf8val.Finish(); err != nil {
			_ = r.c.CloseWithStatus(CloseInvalidFramePayloadData, "invalid utf-8")
			return err
		}
	}

	if !r.c.allowMessage() {
		_ = r.c.CloseWithStatus(ClosePolicyViolation, "message rate exceeded")
		return ErrWebSocketRateLimit
	}

	if r.c.onMessage != nil {
		r.c.onMessage(r.c, r.firstOpcode, r.total)
	}

	return nil
}

func (c *Conn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	if c.closed.Load() {
		return false, 0, nil, ErrWebSocketClosed
	}

	if c.readTimeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	var head [2]byte

	if _, err = io.ReadFull(c.conn, head[:]); err != nil {
		return false, 0, nil, err
	}

	fin = head[0]&0x80 != 0
	opcode = head[0] & 0x0f

	if head[0]&0x70 != 0 {
		_ = c.CloseWithStatus(CloseProtocolError, "rsv bits unsupported")
		return false, 0, nil, ErrWebSocketProtocol
	}

	switch opcode {
	case Continuation, Text, Binary, Close, Ping, Pong:
	default:
		_ = c.CloseWithStatus(CloseProtocolError, "bad opcode")
		return false, 0, nil, ErrWebSocketProtocol
	}

	masked := head[1]&0x80 != 0
	if !masked {
		_ = c.CloseWithStatus(CloseProtocolError, "client frame not masked")
		return false, 0, nil, ErrWebSocketProtocol
	}

	length := uint64(head[1] & 0x7f)

	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.conn, ext[:]); err != nil {
			return false, 0, nil, err
		}

		length = uint64(binary.BigEndian.Uint16(ext[:]))
		if length < 126 {
			_ = c.CloseWithStatus(CloseProtocolError, "non-minimal length")
			return false, 0, nil, ErrWebSocketProtocol
		}

	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.conn, ext[:]); err != nil {
			return false, 0, nil, err
		}

		length = binary.BigEndian.Uint64(ext[:])
		if length>>63 != 0 || length <= 65535 {
			_ = c.CloseWithStatus(CloseProtocolError, "bad extended length")
			return false, 0, nil, ErrWebSocketProtocol
		}
	}

	control := opcode >= 0x8
	if control && (!fin || length > 125) {
		_ = c.CloseWithStatus(CloseProtocolError, "bad control frame")
		return false, 0, nil, ErrWebSocketProtocol
	}

	if c.maxFrame > 0 && length > uint64(c.maxFrame) {
		_ = c.CloseWithStatus(CloseMessageTooBig, "frame too large")
		return false, 0, nil, ErrWebSocketTooLarge
	}

	if c.maxMessage > 0 && length > uint64(c.maxMessage) {
		_ = c.CloseWithStatus(CloseMessageTooBig, "message too large")
		return false, 0, nil, ErrWebSocketTooLarge
	}

	if length > uint64(^uint(0)>>1) {
		_ = c.CloseWithStatus(CloseMessageTooBig, "frame too large")
		return false, 0, nil, ErrWebSocketTooLarge
	}

	var mask [4]byte
	if _, err = io.ReadFull(c.conn, mask[:]); err != nil {
		return false, 0, nil, err
	}

	payload = make([]byte, int(length))

	if len(payload) > 0 {
		if _, err = io.ReadFull(c.conn, payload); err != nil {
			return false, 0, nil, err
		}

		for i := range payload {
			payload[i] ^= mask[i&3]
		}
	}

	return fin, opcode, payload, nil
}

func (c *Conn) WriteMessage(opcode byte, payload []byte) error {
	w, err := c.NextWriter(opcode)
	if err != nil {
		return err
	}

	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			_ = w.Close()
			return err
		}
	}

	return w.Close()
}

func (c *Conn) NextWriter(opcode byte) (io.WriteCloser, error) {
	if opcode != Text && opcode != Binary && opcode != Close && opcode != Ping && opcode != Pong {
		return nil, ErrWebSocketProtocol
	}

	if c.closed.Load() {
		if opcode == Close {
			return nopWriteCloser{}, nil
		}
		return nil, ErrWebSocketClosed
	}

	c.writeMu.Lock()

	w := &MessageWriter{
		c:           c,
		opcode:      opcode,
		first:       true,
		control:     opcode >= 0x8,
		unlockWrite: c.writeMu.Unlock,
		utf8val:     newUTF8StreamValidator(),
	}

	if c.maxFrame > 0 {
		w.buf = make([]byte, 0, c.maxFrame)
	} else {
		w.buf = make([]byte, 0, 4096)
	}

	return w, nil
}

type MessageWriter struct {
	c *Conn

	opcode byte
	first  bool

	control bool
	closed  bool

	buf   []byte
	total int64

	utf8val *utf8StreamValidator

	unlockWrite func()
}

func (w *MessageWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrWebSocketClosed
	}

	if len(p) == 0 {
		return 0, nil
	}

	if w.c.maxMessage > 0 && w.total+int64(len(p)) > int64(w.c.maxMessage) {
		return 0, ErrWebSocketTooLarge
	}

	if w.opcode == Text {
		if err := w.utf8val.Write(p); err != nil {
			return 0, err
		}
	}

	if w.control {
		if len(w.buf)+len(p) > 125 {
			return 0, ErrWebSocketProtocol
		}

		w.buf = append(w.buf, p...)
		w.total += int64(len(p))
		return len(p), nil
	}

	written := len(p)

	for len(p) > 0 {
		maxFrame := w.c.maxFrame
		if maxFrame <= 0 {
			maxFrame = len(p)
		}

		space := maxFrame - len(w.buf)
		if space <= 0 {
			if err := w.flushDataFrame(false); err != nil {
				return written - len(p), err
			}
			continue
		}

		if space > len(p) {
			space = len(p)
		}

		w.buf = append(w.buf, p[:space]...)
		w.total += int64(space)
		p = p[space:]

		if len(w.buf) == maxFrame && len(p) > 0 {
			if err := w.flushDataFrame(false); err != nil {
				return written - len(p), err
			}
		}
	}

	return written, nil
}

func (w *MessageWriter) Close() error {
	if w.closed {
		return nil
	}

	w.closed = true

	defer func() {
		if w.unlockWrite != nil {
			w.unlockWrite()
			w.unlockWrite = nil
		}
	}()

	if w.control {
		if w.opcode == Close && !validClosePayload(w.buf) {
			return ErrWebSocketProtocol
		}

		if err := w.c.writeFrame(true, w.opcode, w.buf); err != nil {
			w.c.closed.Store(true)
			return err
		}

		if w.opcode == Close {
			w.c.closed.Store(true)
		}

		return nil
	}

	if w.opcode == Text {
		if err := w.utf8val.Finish(); err != nil {
			return err
		}
	}

	return w.flushDataFrame(true)
}

func (w *MessageWriter) flushDataFrame(final bool) error {
	op := Continuation
	if w.first {
		op = w.opcode
		w.first = false
	}

	if err := w.c.writeFrame(final, op, w.buf); err != nil {
		w.c.closed.Store(true)
		return err
	}

	w.buf = w.buf[:0]
	return nil
}

func (c *Conn) writeFrame(fin bool, opcode byte, payload []byte) error {
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	var head [10]byte

	if fin {
		head[0] = 0x80 | opcode
	} else {
		head[0] = opcode
	}

	n := 2

	switch {
	case len(payload) < 126:
		head[1] = byte(len(payload))

	case len(payload) <= 65535:
		head[1] = 126
		binary.BigEndian.PutUint16(head[2:4], uint16(len(payload)))
		n = 4

	default:
		head[1] = 127
		binary.BigEndian.PutUint64(head[2:10], uint64(len(payload)))
		n = 10
	}

	if err := fh.WriteAll(c.conn, head[:n]); err != nil {
		return err
	}

	if len(payload) > 0 {
		if err := fh.WriteAll(c.conn, payload); err != nil {
			return err
		}
	}

	return nil
}

func (c *Conn) WriteText(s string) error {
	return c.WriteMessage(Text, []byte(s))
}

func (c *Conn) WriteBinary(b []byte) error {
	return c.WriteMessage(Binary, b)
}

func (c *Conn) ReadJSON(v any) error {
	op, payload, err := c.ReadMessage()
	if err != nil {
		return err
	}

	if op != Text && op != Binary {
		return ErrWebSocketProtocol
	}

	return json.Unmarshal(payload, v)
}

func (c *Conn) WriteJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return c.WriteMessage(Text, b)
}

func (c *Conn) Ping(payload []byte) error {
	if len(payload) > 125 {
		return ErrWebSocketProtocol
	}
	return c.WriteMessage(Ping, payload)
}

func (c *Conn) Pong(payload []byte) error {
	if len(payload) > 125 {
		return ErrWebSocketProtocol
	}
	return c.WriteMessage(Pong, payload)
}

func (c *Conn) CloseWithStatus(code uint16, reason string) error {
	if code == 0 {
		return c.WriteMessage(Close, nil)
	}

	reason = trimUTF8Bytes(reason, 123)

	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload[:2], code)
	copy(payload[2:], reason)

	if !validClosePayload(payload) {
		return ErrWebSocketProtocol
	}

	return c.WriteMessage(Close, payload)
}

func (c *Conn) Close() error {
	var err error

	c.closeOnce.Do(func() {
		_ = c.CloseWithStatus(CloseNormalClosure, "")
		c.closed.Store(true)
		err = c.conn.Close()
	})

	return err
}

func (c *Conn) Closed() bool {
	return c.closed.Load()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) Subprotocol() string {
	return c.selectedProtocol
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// NetConn is exposed only for advanced integration.
// Do not read/write directly after WebSocket upgrade.
// Direct reads/writes corrupt WebSocket framing.
func (c *Conn) NetConn() net.Conn {
	return c.conn
}

func (c *Conn) StartHeartbeat(interval, pongTimeout time.Duration, payload []byte, done <-chan struct{}) {
	if interval <= 0 {
		return
	}

	if pongTimeout <= 0 {
		pongTimeout = interval * 2
	}

	payload = trimBytes(payload, 125)

	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if c.closed.Load() {
					return
				}

				last := time.Unix(0, c.lastPong.Load())

				if !last.IsZero() && time.Since(last) > pongTimeout {
					_ = c.CloseWithStatus(CloseGoingAway, "pong timeout")
					_ = c.Close()
					return
				}

				if err := c.Ping(payload); err != nil {
					_ = c.Close()
					return
				}

			case <-done:
				return
			}
		}
	}()
}

func (c *Conn) markPong() {
	c.lastPong.Store(time.Now().UnixNano())
}

func (c *Conn) allowMessage() bool {
	if c.rateLimit <= 0 {
		return true
	}

	now := time.Now()

	c.rateMu.Lock()
	defer c.rateMu.Unlock()

	if c.rateWindowStart.IsZero() || now.Sub(c.rateWindowStart) >= time.Second {
		c.rateWindowStart = now
		c.rateCount = 0
	}

	c.rateCount++

	return c.rateCount <= c.rateLimit
}

func (c *Conn) emitError(err error) {
	if err != nil && c.onError != nil {
		c.onError(c, err)
	}
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopWriteCloser) Close() error                { return nil }

type utf8StreamValidator struct {
	tail []byte
}

func newUTF8StreamValidator() *utf8StreamValidator {
	return &utf8StreamValidator{
		tail: make([]byte, 0, utf8.UTFMax),
	}
}

func (v *utf8StreamValidator) Write(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	var data []byte

	if len(v.tail) > 0 {
		data = make([]byte, 0, len(v.tail)+len(p))
		data = append(data, v.tail...)
		data = append(data, p...)
		v.tail = v.tail[:0]
	} else {
		data = p
	}

	for len(data) > 0 {
		if data[0] < utf8.RuneSelf {
			data = data[1:]
			continue
		}

		if !utf8.FullRune(data) {
			if len(data) > utf8.UTFMax {
				return ErrWebSocketProtocol
			}

			v.tail = append(v.tail[:0], data...)
			return nil
		}

		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size == 1 {
			return ErrWebSocketProtocol
		}

		data = data[size:]
	}

	return nil
}

func (v *utf8StreamValidator) Finish() error {
	if len(v.tail) != 0 {
		return ErrWebSocketProtocol
	}
	return nil
}

type OutboundMessage struct {
	Opcode  byte
	Payload []byte
}

type Writer struct {
	Conn   *Conn
	Queue  chan OutboundMessage
	Done   chan struct{}
	closed atomic.Bool
	once   sync.Once
}

func NewWriter(conn *Conn, queueSize int) *Writer {
	if queueSize <= 0 {
		queueSize = 64
	}

	w := &Writer{
		Conn:  conn,
		Queue: make(chan OutboundMessage, queueSize),
		Done:  make(chan struct{}),
	}

	go w.loop()

	return w
}

func (w *Writer) Send(opcode byte, payload []byte) bool {
	if w.closed.Load() {
		return false
	}

	cp := make([]byte, len(payload))
	copy(cp, payload)

	select {
	case w.Queue <- OutboundMessage{Opcode: opcode, Payload: cp}:
		return true

	default:
		_ = w.CloseWithStatus(ClosePolicyViolation, "slow client")
		return false
	}
}

func (w *Writer) SendText(s string) bool {
	return w.Send(Text, []byte(s))
}

func (w *Writer) SendJSON(v any) bool {
	b, err := json.Marshal(v)
	if err != nil {
		return false
	}

	return w.Send(Text, b)
}

func (w *Writer) CloseWithStatus(code uint16, reason string) error {
	w.once.Do(func() {
		w.closed.Store(true)
		_ = w.Conn.CloseWithStatus(code, reason)
		_ = w.Conn.Close()
		close(w.Done)
	})

	return nil
}

func (w *Writer) loop() {
	for {
		select {
		case msg := <-w.Queue:
			if err := w.Conn.WriteMessage(msg.Opcode, msg.Payload); err != nil {
				_ = w.CloseWithStatus(CloseGoingAway, "write failed")
				return
			}

		case <-w.Done:
			return
		}
	}
}

type Manager struct {
	mu    sync.RWMutex
	conns map[*Conn]struct{}
}

func NewManager() *Manager {
	return &Manager{
		conns: make(map[*Conn]struct{}),
	}
}

func (m *Manager) Add(c *Conn) {
	if m == nil || c == nil {
		return
	}

	m.mu.Lock()
	m.conns[c] = struct{}{}
	m.mu.Unlock()
}

func (m *Manager) Remove(c *Conn) {
	if m == nil || c == nil {
		return
	}

	m.mu.Lock()
	delete(m.conns, c)
	m.mu.Unlock()
}

func (m *Manager) Count() int {
	if m == nil {
		return 0
	}

	m.mu.RLock()
	n := len(m.conns)
	m.mu.RUnlock()

	return n
}

func (m *Manager) Snapshot() []*Conn {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	list := make([]*Conn, 0, len(m.conns))
	for c := range m.conns {
		list = append(list, c)
	}
	m.mu.RUnlock()

	return list
}

func (m *Manager) BroadcastText(s string) int {
	return m.Broadcast(Text, []byte(s))
}

func (m *Manager) BroadcastJSON(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}

	return m.Broadcast(Text, b)
}

func (m *Manager) Broadcast(opcode byte, payload []byte) int {
	list := m.Snapshot()

	sent := int64(0)

	var wg sync.WaitGroup

	for _, c := range list {
		if c.Closed() {
			continue
		}

		wg.Add(1)

		go func(conn *Conn) {
			defer wg.Done()

			if err := conn.WriteMessage(opcode, payload); err == nil {
				atomic.AddInt64(&sent, 1)
			}
		}(c)
	}

	wg.Wait()

	return int(sent)
}

func (m *Manager) CloseAll(code uint16, reason string, timeout time.Duration) {
	list := m.Snapshot()

	var wg sync.WaitGroup

	for _, c := range list {
		wg.Add(1)

		go func(conn *Conn) {
			defer wg.Done()
			_ = conn.CloseWithStatus(code, reason)
			_ = conn.Close()
		}(c)
	}

	if timeout <= 0 {
		wg.Wait()
		return
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		for _, c := range list {
			_ = c.NetConn().Close()
		}
	}
}

func parseClosePayload(payload []byte) (uint16, string) {
	if len(payload) < 2 {
		return CloseNoStatusReceived, ""
	}

	code := binary.BigEndian.Uint16(payload[:2])
	reason := ""

	if len(payload) > 2 {
		reason = string(payload[2:])
	}

	return code, reason
}

func validClosePayload(payload []byte) bool {
	if len(payload) == 0 {
		return true
	}

	if len(payload) == 1 {
		return false
	}

	code := binary.BigEndian.Uint16(payload[:2])

	if code < 1000 ||
		code == 1004 ||
		code == 1005 ||
		code == 1006 ||
		code == 1015 ||
		(code >= 1016 && code < 3000) ||
		code > 4999 {
		return false
	}

	return utf8.Valid(payload[2:])
}

func trimUTF8Bytes(s string, max int) string {
	if max <= 0 {
		return ""
	}

	for len(s) > max {
		_, size := utf8.DecodeLastRuneInString(s)
		if size <= 0 {
			return ""
		}

		s = s[:len(s)-size]
	}

	if !utf8.ValidString(s) {
		return ""
	}

	return s
}

func trimBytes(b []byte, max int) []byte {
	if len(b) <= max {
		return b
	}

	cp := make([]byte, max)
	copy(cp, b[:max])

	return cp
}
