package fh

import (
	"encoding/binary"
	"fmt"
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
	maxPush  uint32
	disabled atomic.Bool
}

func newPushState(maxConcurrentStreams uint32) *h2PushState {
	p := &h2PushState{
		nextID:  1, // push promises use odd stream IDs starting from 1
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
	conn := r.conn

	// Check if client allows push.
	if conn.pushState == nil || conn.pushState.disabled.Load() {
		return false
	}

	// Check SETTINGS_ENABLE_PUSH.
	if !conn.peerEnablePush.Load() {
		return false
	}

	// Allocate a push promise stream ID.
	streamID := conn.allocatePushID()
	if streamID == 0 {
		return false
	}

	// Build the PUSH_PROMISE header block.
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	conn.encBuf.Reset()

	fields := []hpackHeaderField{
		{Name: ":method", Value: method},
		{Name: ":path", Value: path},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: string(r.stream.authority)},
	}

	for k, v := range headers {
		fields = append(fields, hpackHeaderField{Name: k, Value: v})
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

	// PUSH_PROMISE frame: 8 bytes payload = 4 bytes reserved + 4 bytes promised stream ID.
	var payload [8]byte
	binary.BigEndian.PutUint32(payload[0:4], streamID&0x7fffffff)

	// Send the header block as CONTINUATION frames if needed.
	max := int(conn.peerMaxFrame.Load())
	first := true
	for first || len(block) > 0 {
		n := minInt(len(block), max)
		part := block[:n]
		block = block[n:]
		flags := uint8(0)
		if first {
			flags = h2FlagEndHeaders
			first = false
		}
		if len(block) == 0 {
			flags |= h2FlagEndHeaders
		}
		if err := conn.writeFrameLocked(h2PushPromise, flags, r.stream.id, payload[:]); err != nil {
			return false
		}
		if len(part) > 0 {
			if err := conn.writeFrameLocked(h2Continuation, 0, r.stream.id, part); err != nil {
				return false
			}
		}
	}

	// Record the pushed stream.
	conn.pushState.mu.Lock()
	conn.pushState.streams[streamID] = true
	conn.pushState.mu.Unlock()

	return true
}

// allocatePushID allocates an odd-numbered stream ID for a push promise.
func (h *h2Conn) allocatePushID() uint32 {
	if h.pushState == nil {
		return 0
	}

	h.pushState.mu.Lock()
	defer h.pushState.mu.Unlock()

	// Check concurrent push limit.
	h.mu.Lock()
	streamCount := len(h.streams)
	h.mu.Unlock()

	if uint32(streamCount) >= h.pushState.maxPush {
		return 0
	}

	id := h.pushState.nextID
	h.pushState.nextID += 2 // Push promises use odd IDs
	return id
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
	if c.responded || c.upgraded {
		return false
	}

	// Only supported over HTTP/1.1 connections.
	if c.h2 != nil {
		return false
	}

	// Build 103 Early Hints response.
	hint := fmt.Sprintf("HTTP/1.1 103 Early Hints\r\nLink: <%s>; rel=preload\r\n\r\n", uri)

	if _, err := c.conn.Write([]byte(hint)); err != nil {
		return false
	}
	return true
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
	if c.responded || c.upgraded {
		return false
	}
	if c.h2 != nil {
		return false
	}

	link := "<" + uri + ">"
	for k, v := range attrs {
		if v == "" {
			link += "; " + k
		} else {
			link += "; " + k + "=" + v
		}
	}

	hint := fmt.Sprintf("HTTP/1.1 103 Early Hints\r\nLink: %s\r\n\r\n", link)
	if _, err := c.conn.Write([]byte(hint)); err != nil {
		return false
	}
	return true
}

// Send103EarlyHints writes an HTTP 103 Early Hints response for HTTP/1.1 connections.
func (c *DefaultCtx) Send103EarlyHints(links []string) bool {
	if c.responded || c.upgraded || c.h2 != nil {
		return false
	}

	var resp []byte
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
