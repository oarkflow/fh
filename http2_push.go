package fh

import (
	"encoding/binary"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/oarkflow/fh/pkg/hpack"
)

// PushPromise represents an HTTP/2 Server Push promise.
type PushPromise struct {
	Method  string
	Path    string
	Headers map[string]string
}

// h2PushState tracks push promises sent on an HTTP/2 connection.
type h2PushState struct {
	mu       sync.Mutex
	nextID   uint32
	streams  map[uint32]bool
	pending  uint32
	maxPush  uint32
	disabled atomic.Bool
}

func newPushState(maxConcurrentStreams uint32) *h2PushState {
	p := &h2PushState{
		nextID:  2, // server-initiated push streams use even IDs
		streams: make(map[uint32]bool),
		maxPush: maxConcurrentStreams,
	}
	return p
}

// Push sends an HTTP/2 PUSH_PROMISE frame to the client for the given path.
// Returns false if push is not possible (disabled, too many promises, or client
// has disabled server push via SETTINGS_ENABLE_PUSH=0).
//
// Usage:
//
//	app.Get("/page", func(c *fh.Ctx) error {
//	    c.Push("/static/style.css", "GET", nil)
//	    c.Push("/static/app.js", "GET", nil)
//	    return c.JSON(pageData)
//	})
func (c *DefaultCtx) Push(path string, method string, headers map[string]string) bool {
	if c.h2 == nil {
		return false
	}
	return c.h2.pushPromise(path, method, headers)
}

// SetEnablePush configures whether server push is allowed on this HTTP/2 connection.
func (c *DefaultCtx) SetEnablePush(enabled bool) {
	if c.h2 != nil {
		c.h2.conn.pushState.disabled.Store(!enabled)
	}
}

// pushPromise sends a PUSH_PROMISE for the given path on an HTTP/2 stream.
func (r *h2Response) pushPromise(path string, method string, headers map[string]string) bool {
	if r.ended.Load() {
		return false
	}
	if (method != "GET" && method != "HEAD") || path == "" || path[0] != '/' || strings.ContainsAny(path, "\x00\r\n") {
		return false
	}
	conn := r.conn

	// Check if client allows push.
	if conn.pushState == nil || conn.pushState.disabled.Load() {
		return false
	}

	// Check SETTINGS_ENABLE_PUSH.
	if !conn.peerEnablePush.Load() {
		return false
	}

	// Allocate a server-initiated push stream ID.
	streamID := conn.allocatePushID()
	if streamID == 0 {
		return false
	}
	reserved := true
	defer func() {
		if reserved {
			conn.releasePushReservation()
		}
	}()

	// Build the PUSH_PROMISE header block.
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	conn.encBuf.Reset()

	scheme := r.stream.scheme
	if scheme == "" {
		scheme = "https"
	}
	fields := []hpackHeaderField{
		{Name: ":method", Value: method},
		{Name: ":path", Value: path},
		{Name: ":scheme", Value: scheme},
		{Name: ":authority", Value: string(r.stream.authority)},
	}

	requestHeaders := make([]hpack.HeaderField, 0, len(headers))
	for k, v := range headers {
		name := strings.ToLower(k)
		if strings.HasPrefix(name, ":") || !validToken([]byte(name)) || strings.ContainsAny(v, "\x00\r\n") {
			return false
		}
		fields = append(fields, hpackHeaderField{Name: name, Value: v})
		requestHeaders = append(requestHeaders, hpack.HeaderField{Name: name, Value: v})
	}

	for _, field := range fields {
		if strings.ContainsAny(field.Value, "\r\n") {
			return false
		}
	}

	for _, field := range fields {
		if err := conn.enc.WriteField(hpackHeaderFieldToHPACK(field)); err != nil {
			return false
		}
	}

	block := conn.encBuf.Bytes()

	// A PUSH_PROMISE carries the promised stream ID followed by the first
	// header-block fragment. The remaining fragments belong in CONTINUATION
	// frames. In particular, END_HEADERS must only be set on the final frame.
	max := int(conn.peerMaxFrame.Load())
	if max < 4 {
		return false
	}
	first := true
	for {
		limit := max
		if first {
			limit -= 4
		}
		n := minInt(len(block), limit)
		flags := uint8(0)
		if n == len(block) {
			flags |= h2FlagEndHeaders
		}

		if first {
			payload := make([]byte, 4+n)
			binary.BigEndian.PutUint32(payload[:4], streamID&0x7fffffff)
			copy(payload[4:], block[:n])
			if err := conn.writeFrameLocked(h2PushPromise, flags, r.stream.id, payload); err != nil {
				return false
			}
			first = false
		} else if err := conn.writeFrameLocked(h2Continuation, flags, r.stream.id, block[:n]); err != nil {
			return false
		}

		block = block[n:]
		if len(block) == 0 {
			break
		}
	}

	// Record the pushed stream.
	conn.pushState.mu.Lock()
	conn.pushState.streams[streamID] = true
	conn.pushState.pending--
	conn.pushState.mu.Unlock()
	reserved = false

	// A PUSH_PROMISE reserves a server-initiated stream. Dispatch the promised
	// request through the normal router so the client receives the promised
	// resource response instead of an orphaned stream that can never finish.
	streamCtx, cancel := conn.newStreamContext()
	pushed := &h2Stream{
		id:         streamID,
		method:     method,
		path:       path,
		authority:  r.stream.authority,
		scheme:     scheme,
		headers:    requestHeaders,
		ctx:        streamCtx,
		cancel:     cancel,
		sendWindow: conn.peerInitialWindow,
		recvWindow: h2InitialWindow,
		ended:      true,
	}
	pushed.bodyCond = sync.NewCond(&pushed.bodyMu)
	pushed.state.Store(int32(stateHalfClosedRemote))
	conn.mu.Lock()
	conn.streams[streamID] = pushed
	conn.mu.Unlock()
	conn.dispatch(pushed)

	return true
}

// allocatePushID allocates an even-numbered stream ID for a push promise.
func (h *h2Conn) allocatePushID() uint32 {
	if h.pushState == nil {
		return 0
	}

	// SETTINGS_MAX_CONCURRENT_STREAMS limits streams initiated by the peer, so
	// use the peer's value for pushes and keep the application value as a local
	// safety cap. Ordinary client-initiated streams do not consume push slots.
	h.pushState.mu.Lock()
	defer h.pushState.mu.Unlock()
	limit := h.pushState.maxPush
	if peerLimit := h.peerMaxConcurrentStreams.Load(); peerLimit < limit {
		limit = peerLimit
	}
	if uint32(len(h.pushState.streams))+h.pushState.pending >= limit {
		return 0
	}

	id := h.pushState.nextID
	h.pushState.nextID += 2 // Push promises use even IDs.
	h.pushState.pending++
	return id
}

func (h *h2Conn) releasePushReservation() {
	if h.pushState == nil {
		return
	}
	h.pushState.mu.Lock()
	if h.pushState.pending > 0 {
		h.pushState.pending--
	}
	h.pushState.mu.Unlock()
}

// ── HTTP/1.1 Push (Early Hints 103) ────────────────────────────────────────

// EarlyHint sends an HTTP 103 Early Hints response to hint at resources
// the server will likely include in the final response. This allows the
// client to begin fetching resources before the server finishes processing.
//
// Usage:
//
//	app.Get("/page", func(c *fh.Ctx) error {
//	    c.EarlyHint("/static/style.css")
//	    c.EarlyHint("/static/app.js")
//	    // ... expensive computation ...
//	    return c.JSON(pageData)
//	})
func (c *DefaultCtx) EarlyHint(uri string) bool {
	if !validLinkTarget(uri) || c.responded || c.upgraded {
		return false
	}
	if c.h2 != nil {
		return c.h2.sendEarlyHints(c, []string{"<" + uri + ">; rel=preload"})
	}
	return c.Send103EarlyHints([]string{uri})
}

// EarlyHintsWithHeaders sends an HTTP 103 Early Hints with custom Link headers
// and optional headers like rel, as, type, crossorigin.
//
// Usage:
//
//	c.EarlyHintsWithHeaders("/font.woff2", map[string]string{
//	    "rel": "preload", "as": "font", "type": "font/woff2", "crossorigin": "",
//	})
func (c *DefaultCtx) EarlyHintsWithHeaders(uri string, attrs map[string]string) bool {
	if !validLinkTarget(uri) || c.responded || c.upgraded {
		return false
	}
	link := "<" + uri + ">"
	for k, v := range attrs {
		if !validLinkParam(k, v) {
			return false
		}
		if v == "" {
			link += "; " + k
		} else {
			link += "; " + k + "=" + v
		}
	}

	if c.h2 != nil {
		return c.h2.sendEarlyHints(c, []string{link})
	}
	return c.sendEarlyHintsHTTP1([]string{link})
}

// Send103EarlyHints writes an HTTP 103 Early Hints response for HTTP/1.1 connections.
func (c *DefaultCtx) Send103EarlyHints(links []string) bool {
	if c.responded || c.upgraded || c.h2 != nil {
		return false
	}
	for _, link := range links {
		if !validLinkTarget(link) {
			return false
		}
	}
	return c.sendEarlyHintsHTTP1(links)
}

func (c *DefaultCtx) SendInformational(status int, headers map[string]string) bool {
	if status < 100 || status >= 200 || c.responded || c.upgraded {
		return false
	}
	if c.h2 != nil {
		values := make(map[string][]string, len(headers))
		for name, value := range headers {
			values[name] = []string{value}
		}
		return c.h2.conn.sendInformationalHeaders(c.h2.stream, status, values) == nil
	}
	resp := []byte("HTTP/1.1 " + strconv.Itoa(status) + " " + StatusReason(status) + "\r\n")
	for name, value := range headers {
		if !validToken([]byte(name)) || strings.ContainsAny(name+value, "\x00\r\n") {
			return false
		}
		resp = append(resp, name+": "+value+"\r\n"...)
	}
	resp = append(resp, "\r\n"...)
	_, err := c.conn.Write(resp)
	return err == nil
}

func (c *DefaultCtx) sendEarlyHintsHTTP1(links []string) bool {
	resp := make([]byte, 0, 64+len(links)*32)
	resp = append(resp, "HTTP/1.1 103 Early Hints\r\n"...)
	for _, link := range links {
		resp = append(resp, "Link: <"...)
		resp = append(resp, link...)
		resp = append(resp, ">; rel=preload\r\n"...)
	}
	resp = append(resp, "\r\n"...)

	_, err := c.conn.Write(resp)
	return err == nil
}

func validLinkTarget(uri string) bool {
	return uri != "" && !strings.ContainsAny(uri, "\x00\r\n<>")
}

func validLinkParam(name, value string) bool {
	return validToken([]byte(name)) && !strings.ContainsAny(value, "\x00\r\n")
}

// PushResource pushes resources during HTTP/2 or early hints during HTTP/1.1.
// This is a convenience method that automatically selects the right mechanism.
func (c *DefaultCtx) PushResource(path string, method string, headers map[string]string) bool {
	if c.h2 != nil {
		return c.Push(path, method, headers)
	}
	// HTTP/1.1: use Early Hints.
	attrs := map[string]string{
		"rel": "preload",
	}
	for k, v := range headers {
		attrs[k] = v
	}
	return c.EarlyHintsWithHeaders(path, attrs)
}

// PushStylesheet is a convenience helper to push a CSS file.
func (c *DefaultCtx) PushStylesheet(path string) bool {
	return c.PushResource(path, "GET", map[string]string{"as": "style", "type": "text/css"})
}

// PushScript is a convenience helper to push a JavaScript file.
func (c *DefaultCtx) PushScript(path string) bool {
	return c.PushResource(path, "GET", map[string]string{"as": "script", "type": "application/javascript"})
}

// PushImage is a convenience helper to push an image file.
func (c *DefaultCtx) PushImage(path string) bool {
	return c.PushResource(path, "GET", map[string]string{"as": "image"})
}

// PushFont is a convenience helper to push a font file.
func (c *DefaultCtx) PushFont(path string) bool {
	return c.PushResource(path, "GET", map[string]string{"as": "font", "type": "font/woff2", "crossorigin": ""})
}

// PushDocument is a convenience helper to push a document (JSON, XML, etc.).
func (c *DefaultCtx) PushDocument(path string) bool {
	return c.PushResource(path, "GET", map[string]string{"as": "document"})
}

// ── HPACK helper types ─────────────────────────────────────────────────────

// hpackHeaderField is a local HPACK header field type to avoid import cycles.
type hpackHeaderField struct {
	Name  string
	Value string
}

// hpackHeaderFieldToHPACK converts our local type to the pkg/hpack type.
func hpackHeaderFieldToHPACK(f hpackHeaderField) hpack.HeaderField {
	return hpack.HeaderField{Name: f.Name, Value: f.Value}
}

// atomicBool is a simple atomic boolean type for HTTP/2 settings.
type atomicBool struct {
	v atomic.Int32
}

func (b *atomicBool) Load() bool { return b.v.Load() != 0 }
func (b *atomicBool) Store(v bool) {
	if v {
		b.v.Store(1)
	} else {
		b.v.Store(0)
	}
}

// h2Conn push-related fields are defined in http2.go:
// - pushState *h2PushState
// - peerEnablePush atomicBool
// These are initialized in newH2Conn.
