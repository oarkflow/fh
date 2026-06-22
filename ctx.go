package fh

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	dateBuf            atomic.Value
	dateCacheUnix      int64
	dateMu             sync.Mutex
	dateValueBuf       atomic.Value
	dateValueCacheUnix int64
	dateValueMu        sync.Mutex
)

// cachedDate returns the full "Date: ...\r\n" header line.
func cachedDate() []byte {
	now := time.Now().Unix()
	if now != atomic.LoadInt64(&dateCacheUnix) {
		dateMu.Lock()
		if now != dateCacheUnix {
			b := make([]byte, 0, 64)
			b = append(b, "Date: "...)
			b = time.Now().AppendFormat(b, "Mon, 02 Jan 2006 15:04:05 GMT")
			b = append(b, '\r', '\n')
			dateBuf.Store(b)
			dateCacheUnix = now
		}
		dateMu.Unlock()
	}
	b, _ := dateBuf.Load().([]byte)
	return b
}

// cachedDateValue returns just the RFC 1123 date string (no "Date: " prefix),
// cached and second-granular like cachedDate. Used for HTTP/2 responses.
func cachedDateValue() string {
	now := time.Now().Unix()
	if now != atomic.LoadInt64(&dateValueCacheUnix) {
		dateValueMu.Lock()
		if now != dateValueCacheUnix {
			b := make([]byte, 0, 56)
			b = time.Now().AppendFormat(b, "Mon, 02 Jan 2006 15:04:05 GMT")
			dateValueBuf.Store(b)
			dateValueCacheUnix = now
		}
		dateValueMu.Unlock()
	}
	b, _ := dateValueBuf.Load().([]byte)
	return string(b)
}

type Ctx struct {
	conn   net.Conn
	server *App

	Header RequestHeader

	params []Param

	status           int
	customHeaders    [16]Header
	extraHeaders     []Header
	chCount          int
	body             []byte
	contentType      []byte
	responded        bool
	forceClose       bool
	upgraded         bool
	upgradeBuffered  []byte
	trailers         []Header
	responseTrailers []Header
	requestContext   context.Context
	originalURI      []byte
	bodyTransform    func([]byte) ([]byte, error)
	h2               *h2Response
	cachedIP         string

	handlers     []HandlerFunc
	handlerIndex int

	locals        [16]localEntry
	lcount        int
	localOverflow map[string]any

	readBuf  *[]byte
	writeBuf *[]byte

	queryParsed bool
	queryParams []Param
	qcount      int

	responseCookies []Cookie
	responseTime    time.Time
	beforeResponse  []func(*Ctx) error
	beforeRan       bool
	multipartForm   *MultipartForm
	multipartErr    error
	multipartParsed bool
}

type localEntry struct {
	key string
	val any
}

var ctxPool = sync.Pool{
	New: func() any {
		c := &Ctx{
			params:      make([]Param, 0, 8),
			queryParams: make([]Param, 0, 8),
		}
		return c
	},
}

func acquireCtx(conn net.Conn, app *App) *Ctx {
	c := ctxPool.Get().(*Ctx)
	c.conn = conn
	c.server = app
	c.reset()
	return c
}

func releaseCtx(c *Ctx) {
	if c.writeBuf != nil {
		putBytes(c.writeBuf)
		c.writeBuf = nil
	}
	ctxPool.Put(c)
}

func (c *Ctx) reset() {
	c.Header.reset()
	clear(c.params)
	c.params = c.params[:0]
	c.status = 200
	if c.chCount > 0 {
		clear(c.customHeaders[:c.chCount])
	}
	c.chCount = 0
	clear(c.extraHeaders)
	c.extraHeaders = c.extraHeaders[:0]
	c.body = nil
	c.contentType = nil
	c.responded = false
	c.forceClose = false
	c.upgraded = false
	c.upgradeBuffered = nil
	clear(c.trailers)
	c.trailers = c.trailers[:0]
	clear(c.responseTrailers)
	c.responseTrailers = c.responseTrailers[:0]
	c.requestContext = context.Background()
	c.originalURI = c.originalURI[:0]
	c.bodyTransform = nil
	c.h2 = nil
	if c.lcount > 0 {
		clear(c.locals[:c.lcount])
	}
	c.lcount = 0
	clear(c.localOverflow)
	c.queryParsed = false
	c.qcount = 0
	clear(c.queryParams)
	c.queryParams = c.queryParams[:0]
	c.handlers = nil
	c.handlerIndex = 0
	c.cachedIP = ""
	clear(c.responseCookies)
	c.responseCookies = c.responseCookies[:0]
	c.responseTime = time.Time{}
	clear(c.beforeResponse)
	c.beforeResponse = c.beforeResponse[:0]
	c.beforeRan = false
	c.multipartForm = nil
	c.multipartErr = nil
	c.multipartParsed = false
}

// OnBeforeResponse registers a one-shot hook run immediately before response
// headers are encoded. It is intended for transactional middleware such as
// sessions that must persist before Set-Cookie reaches the wire.
func (c *Ctx) OnBeforeResponse(fn func(*Ctx) error) {
	if fn != nil && !c.responded && !c.beforeRan {
		c.beforeResponse = append(c.beforeResponse, fn)
	}
}

func (c *Ctx) runBeforeResponse() error {
	if c.beforeRan {
		return nil
	}
	c.beforeRan = true
	var firstErr error
	for _, fn := range c.beforeResponse {
		if err := fn(c); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Next continues the current middleware chain. A handler may return without
// calling Next to stop the chain. The index-based implementation avoids the
// per-request recursive closure used by many small middleware implementations.
func (c *Ctx) Next() error {
	if c.handlerIndex >= len(c.handlers) {
		return nil
	}
	h := c.handlers[c.handlerIndex]
	c.handlerIndex++
	return h(c)
}

// ── Request accessors ──────────────────────────────────────────────────────

func (c *Ctx) Method() string { return string(c.Header.Method) }

// OriginalURL returns the request target as it arrived, before any Rewrite.
// The name mirrors Fiber's Ctx API.
func (c *Ctx) OriginalURL() string {
	if len(c.originalURI) != 0 {
		return string(c.originalURI)
	}
	return string(c.Header.URI)
}

func (c *Ctx) path() []byte {
	uri := c.Header.URI
	for i, v := range uri {
		if v == '?' {
			return uri[:i]
		}
	}
	return uri
}

func (c *Ctx) Path() string { return string(c.path()) }

// Rewrite updates the request URI and asks the application to route it again.
// It is intended for rewrite middleware and is bounded by the application to
// prevent rewrite loops.
func (c *Ctx) Rewrite(target string) error {
	if target == "" || target[0] != '/' || strings.ContainsAny(target, "\x00\r\n") {
		return BadRequest("Invalid rewrite target")
	}
	c.Header.URI = []byte(target)
	c.queryParsed = false
	c.qcount = 0
	clear(c.queryParams)
	c.queryParams = c.queryParams[:0]
	return ErrRewrite
}

func (c *Ctx) Param(name string) string {
	for i := range c.params {
		if c.params[i].Key == name {
			return c.params[i].Value
		}
	}
	return ""
}

// Params is the Fiber-compatible alias for Param. If the parameter is absent,
// the optional default value is returned.
func (c *Ctx) Params(name string, defaults ...string) string {
	value := c.Param(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

func (c *Ctx) Query(name string, def ...string) string {
	if !c.queryParsed {
		c.parseQuery()
	}
	for i := 0; i < c.qcount; i++ {
		if c.queryParams[i].Key == name {
			return c.queryParams[i].Value
		}
	}
	if len(def) > 0 {
		return def[0]
	}
	return ""
}

func (c *Ctx) parseQuery() {
	c.queryParsed = true
	uri := c.Header.URI
	n := len(uri)
	if n == 0 {
		return
	}
	// Find '?' in a single pass along with parsing
	qi := -1
	for i := 0; i < n; i++ {
		if uri[i] == '?' {
			qi = i
			break
		}
	}
	if qi < 0 {
		return
	}
	qs := uri[qi+1:]
	nq := len(qs)
	if nq == 0 {
		return
	}
	i := 0
	for i < nq {
		if qs[i] == '&' {
			i++
			continue
		}
		start := i
		for i < nq && qs[i] != '&' {
			i++
		}
		pair := qs[start:i]
		if len(pair) > 0 {
			eq := -1
			for j := 0; j < len(pair); j++ {
				if pair[j] == '=' {
					eq = j
					break
				}
			}
			var key, value []byte
			if eq >= 0 {
				key, value = pair[:eq], pair[eq+1:]
			} else {
				key, value = pair, nil
			}
			k := urlDecode(key)
			v := urlDecode(value)
			if c.qcount < len(c.queryParams) {
				c.queryParams[c.qcount] = Param{Key: k, Value: v}
			} else {
				c.queryParams = append(c.queryParams, Param{Key: k, Value: v})
			}
			c.qcount++
		}
		if i >= nq {
			break
		}
		i++ // skip '&'
	}
}

func (c *Ctx) Body() []byte { return c.body }

// BodyRaw is the Fiber-compatible name for the unmodified request body.
func (c *Ctx) BodyRaw() []byte { return c.body }

// QueryParser decodes the query string into v. The target type should be
// *map[string]any for unstructured access; struct decoding is not yet supported.
// QueryParser decodes the query string into v. Supports the same formats
// as form-encoded bodies (nested keys via bracket notation, arrays, etc.).
// Target should be *map[string]any or *any.
func (c *Ctx) QueryParser(v any) error {
	uri := c.Header.URI
	qi := bytes.IndexByte(uri, '?')
	if qi < 0 {
		return nil
	}
	var fc formCodec
	return fc.Unmarshal(uri[qi+1:], v)
}

// Trailer returns a decoded chunked request trailer by name.
func (c *Ctx) Trailer(name string) string {
	for i := range c.trailers {
		if bytesEqualFold(c.trailers[i].Key, []byte(name)) {
			return string(c.trailers[i].Value)
		}
	}
	return ""
}

// SetTrailer sets a response trailer header. Trailers are sent after the
// chunked body (HTTP/1.1) or as trailing HEADERS (HTTP/2). The trailer
// name should also be announced via the Trailer response header.
func (c *Ctx) SetTrailer(key, value string) {
	if !validToken([]byte(key)) || strings.ContainsAny(value, "\x00\r\n") {
		return
	}
	if c.responseTrailers == nil {
		c.responseTrailers = make([]Header, 0, 4)
	}
	for i := range c.responseTrailers {
		if bytesEqualFold(c.responseTrailers[i].Key, []byte(key)) {
			c.responseTrailers[i].Value = []byte(value)
			return
		}
	}
	c.responseTrailers = append(c.responseTrailers, Header{Key: []byte(key), Value: []byte(value)})
}

func (c *Ctx) BodyParser(v any) error {
	ct := b2s(c.Header.ContentType)
	if codec := matchCodec(ct); codec != nil {
		if cta, ok := codec.(ContentTypeAwareCodec); ok {
			return cta.UnmarshalWithContentType(c.body, ct, v)
		}
		return codec.Unmarshal(c.body, v)
	}
	return json.Unmarshal(c.body, v)
}

// Context carries request cancellation and middleware deadlines.
func (c *Ctx) Context() context.Context { return c.requestContext }

func (c *Ctx) SetContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.requestContext = ctx
}

// TransformBody installs a buffered response transformation. It is intended
// for middleware such as gzip compression and does not affect Stream output.
func (c *Ctx) TransformBody(fn func([]byte) ([]byte, error)) { c.bodyTransform = fn }

// AddBodyTransform appends a response transformation without replacing an
// existing middleware transformation.
func (c *Ctx) AddBodyTransform(fn func([]byte) ([]byte, error)) {
	if fn == nil {
		return
	}
	previous := c.bodyTransform
	if previous == nil {
		c.bodyTransform = fn
		return
	}
	c.bodyTransform = func(body []byte) ([]byte, error) {
		body, err := previous(body)
		if err != nil {
			return nil, err
		}
		return fn(body)
	}
}

func (c *Ctx) Get(name string, defaults ...string) string {
	value := c.Header.PeekStr(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

// GetReqHeaders returns all request header values, preserving repeated fields.
func (c *Ctx) GetReqHeaders() map[string][]string { return c.Header.GetHeaders() }

// GetHeaders is an alias for GetReqHeaders.
func (c *Ctx) GetHeaders() map[string][]string { return c.GetReqHeaders() }

// Hostname returns the request host without its port.
func (c *Ctx) Hostname() string {
	host := string(c.Header.Host)
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		return parsed
	}
	return strings.Trim(host, "[]")
}

func (c *Ctx) Locals(key string, value ...any) any {
	if len(value) > 0 {
		for i := 0; i < c.lcount; i++ {
			if c.locals[i].key == key {
				c.locals[i].val = value[0]
				return value[0]
			}
		}
		if c.lcount < len(c.locals) {
			c.locals[c.lcount] = localEntry{key: key, val: value[0]}
			c.lcount++
		} else {
			if c.localOverflow == nil {
				c.localOverflow = make(map[string]any)
			}
			c.localOverflow[key] = value[0]
		}
		return value[0]
	}
	for i := 0; i < c.lcount; i++ {
		if c.locals[i].key == key {
			return c.locals[i].val
		}
	}
	if c.localOverflow != nil {
		return c.localOverflow[key]
	}
	return nil
}

func (c *Ctx) IP() string {
	if c.cachedIP != "" {
		return c.cachedIP
	}
	addr := c.conn.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		c.cachedIP = host
	} else {
		c.cachedIP = strings.Trim(addr, "[]")
	}
	return c.cachedIP
}

// ── Response builders ──────────────────────────────────────────────────────

func (c *Ctx) Status(code int) *Ctx {
	if code < 100 || code > 999 {
		code = 500
	}
	c.status = code
	return c
}

// StatusCode returns the current response status code.
// Used by middleware to inspect the status after calling Next().
func (c *Ctx) StatusCode() int {
	return c.status
}

func (c *Ctx) Set(key, value string) {
	k := []byte(key)
	v := []byte(value)
	if !validToken(k) || strings.ContainsAny(value, "\x00\r\n") {
		return
	}
	if bytesEqualFold(k, HeaderContentLengthBytes) || bytesEqualFold(k, HeaderTransferEncodingBytes) || bytesEqualFold(k, HeaderConnectionBytes) {
		return
	}
	if bytesEqualFold(k, HeaderContentTypeBytes) {
		c.contentType = v
		return
	}
	for i := 0; i < c.chCount; i++ {
		if bytesEqualFold(c.customHeaders[i].Key, k) {
			c.customHeaders[i].Value = v
			return
		}
	}
	for i := range c.extraHeaders {
		if bytesEqualFold(c.extraHeaders[i].Key, k) {
			c.extraHeaders[i].Value = v
			return
		}
	}
	if c.chCount < len(c.customHeaders) {
		c.customHeaders[c.chCount] = Header{Key: k, Value: v}
		c.chCount++
	} else {
		c.extraHeaders = append(c.extraHeaders, Header{Key: k, Value: v})
	}
}

// Append adds a comma-separated response header value without replacing an
// existing value. It is useful for fields such as Vary.
func (c *Ctx) Append(key, value string) {
	if !validToken([]byte(key)) || strings.ContainsAny(value, "\x00\r\n") {
		return
	}
	for i := 0; i < c.chCount; i++ {
		if bytesEqualFold(c.customHeaders[i].Key, []byte(key)) {
			if !headerValueContainsToken(c.customHeaders[i].Value, value) {
				c.customHeaders[i].Value = append(append(c.customHeaders[i].Value, ',', ' '), value...)
			}
			return
		}
	}
	for i := range c.extraHeaders {
		if bytesEqualFold(c.extraHeaders[i].Key, []byte(key)) {
			if !headerValueContainsToken(c.extraHeaders[i].Value, value) {
				c.extraHeaders[i].Value = append(append(c.extraHeaders[i].Value, ',', ' '), value...)
			}
			return
		}
	}
	c.Set(key, value)
}

func headerValueContainsToken(header []byte, token string) bool { return hasHeaderToken(header, token) }

// Responded reports whether response headers have already been written.
func (c *Ctx) Responded() bool { return c.responded }

func (c *Ctx) Type(mime string) *Ctx {
	if strings.ContainsAny(mime, "\x00\r\n") {
		return c
	}
	c.contentType = []byte(mime)
	return c
}

// ResponseHeader returns a response header set so far.
func (c *Ctx) ResponseHeader(name string) string {
	if strings.EqualFold(name, HeaderContentType) {
		return string(c.contentType)
	}
	for i := 0; i < c.chCount; i++ {
		if strings.EqualFold(string(c.customHeaders[i].Key), name) {
			return string(c.customHeaders[i].Value)
		}
	}
	for i := range c.extraHeaders {
		if strings.EqualFold(string(c.extraHeaders[i].Key), name) {
			return string(c.extraHeaders[i].Value)
		}
	}
	return ""
}

// GetRespHeader is the Fiber-compatible alias for ResponseHeader.
func (c *Ctx) GetRespHeader(name string, defaults ...string) string {
	value := c.ResponseHeader(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

// GetRespHeaders returns all response headers set on the context.
func (c *Ctx) GetRespHeaders() map[string][]string {
	headers := make(map[string][]string, c.chCount+len(c.extraHeaders)+1)
	if len(c.contentType) != 0 {
		headers[HeaderContentTypeStr] = []string{string(c.contentType)}
	}
	for i := 0; i < c.chCount; i++ {
		key := textproto.CanonicalMIMEHeaderKey(string(c.customHeaders[i].Key))
		headers[key] = append(headers[key], string(c.customHeaders[i].Value))
	}
	for i := range c.extraHeaders {
		key := textproto.CanonicalMIMEHeaderKey(string(c.extraHeaders[i].Key))
		headers[key] = append(headers[key], string(c.extraHeaders[i].Value))
	}
	for i := range c.responseCookies {
		headers[HeaderSetCookieStr] = append(headers[HeaderSetCookieStr], c.responseCookies[i].String())
	}
	return headers
}

// HasResponseCookies reports whether the response currently sets cookies.
func (c *Ctx) HasResponseCookies() bool { return len(c.responseCookies) > 0 }

func (c *Ctx) FirstCookie() string {
	if len(c.responseCookies) == 0 {
		return ""
	}
	return c.responseCookies[0].Value
}

func (c *Ctx) SendString(s string) error {
	if c.contentType == nil {
		c.contentType = plainTextCT
	}
	return c.writeResponseString(s)
}

func (c *Ctx) SendBytes(b []byte) error {
	return c.writeResponse(b)
}

func (c *Ctx) Send(b []byte) error { return c.SendBytes(b) }

func (c *Ctx) JSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.contentType = jsonCT
	return c.writeResponse(b)
}

func (c *Ctx) Render(name string, data any, layout ...string) error {
	engine := c.server.cfg.TemplateEngine
	if engine == nil {
		return NewHTTPError(StatusInternalServerError, "TEMPLATE_ENGINE_MISSING", "fasthttp: no template engine configured")
	}
	var buf bytes.Buffer
	if err := engine.Render(&buf, name, data, layout...); err != nil {
		return err
	}
	c.contentType = []byte("text/html; charset=utf-8")
	return c.writeResponse(buf.Bytes())
}

func (c *Ctx) SendStatus(code int) error {
	c.status = code
	return c.writeResponse(nil)
}

func (c *Ctx) Redirect(location string, code ...int) error {
	sc := 302
	if len(code) > 0 {
		sc = code[0]
	}
	c.status = sc
	c.Set("Location", location)
	return c.writeResponse(nil)
}

// RedirectTo redirects to a named route. Route parameters are substituted and
// additional values become query parameters.
func (c *Ctx) RedirectTo(name string, params map[string]string, code ...int) error {
	location, err := c.server.URL(name, params)
	if err != nil {
		return err
	}
	return c.Redirect(location, code...)
}

// RedirectBack redirects to a same-origin Referer, or to fallback when the
// Referer is absent, malformed, or points at another host.
func (c *Ctx) RedirectBack(fallback string, code ...int) error {
	location := fallback
	if raw := c.Get(HeaderRefererStr); raw != "" {
		if ref, err := url.Parse(raw); err == nil {
			sameOrigin := !ref.IsAbs() || strings.EqualFold(ref.Host, b2s(c.Header.Host))
			if sameOrigin && ref.User == nil && ref.Path != "" {
				location = ref.RequestURI()
			}
		}
	}
	return c.Redirect(location, code...)
}

type contextFlashStore interface {
	Flash(string, ...any) any
}

// Flash stores a value for the next request, or retrieves and consumes it when
// called without a value. The session middleware must be registered.
func (c *Ctx) Flash(key string, value ...any) any {
	store, ok := c.Locals("session").(contextFlashStore)
	if !ok {
		panic("fasthttp: flash messages require session middleware")
	}
	return store.Flash(key, value...)
}

// writeResponseString writes a response with a string body — zero alloc.
func (c *Ctx) writeResponseString(s string) error {
	if c.h2 != nil {
		return c.h2.writeResponse(c, []byte(s))
	}
	if c.bodyTransform != nil && responseBodyAllowed(c.status) {
		return c.writeResponse([]byte(s))
	}
	if c.responded {
		return nil
	}
	if err := c.runBeforeResponse(); err != nil {
		return err
	}
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]

	// Status line
	buf = appendStatusLine(buf, c.status)

	// Date (RFC 9110 §8.6)
	buf = append(buf, cachedDate()...)

	// Content-Type
	if c.contentType != nil {
		buf = append(buf, "Content-Type: "...)
		buf = append(buf, c.contentType...)
		buf = append(buf, '\r', '\n')
	}

	// Custom headers
	for i := 0; i < c.chCount; i++ {
		h := &c.customHeaders[i]
		buf = append(buf, h.Key...)
		buf = append(buf, ':', ' ')
		buf = append(buf, h.Value...)
		buf = append(buf, '\r', '\n')
	}
	buf = appendExtraHeaders(buf, c.extraHeaders)

	// Cookies
	for i := range c.responseCookies {
		buf = append(buf, "Set-Cookie: "...)
		buf = append(buf, c.responseCookies[i].String()...)
		buf = append(buf, '\r', '\n')
	}

	bodyAllowed := responseBodyAllowed(c.status)
	hasTrailers := len(c.responseTrailers) > 0

	if bodyAllowed && hasTrailers {
		// RFC 9112: trailers require chunked transfer encoding
		buf = append(buf, "Transfer-Encoding: chunked\r\n"...)
		buf = append(buf, "Trailer: "...)
		for i, t := range c.responseTrailers {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, t.Key...)
		}
		buf = append(buf, '\r', '\n')
	} else if bodyAllowed {
		buf = append(buf, "Content-Length: "...)
		buf = appendInt(buf, len(s))
		buf = append(buf, '\r', '\n')
	}

	// Connection
	if c.Header.KeepAlive && !c.forceClose {
		buf = append(buf, "Connection: keep-alive\r\n"...)
	} else {
		buf = append(buf, "Connection: close\r\n"...)
	}

	buf = append(buf, '\r', '\n')

	// Body
	if bodyAllowed && !bytesEqualFold(c.Header.Method, MethodHEADBytes) {
		if hasTrailers {
			// Write body as a single chunk
			if len(s) > 0 {
				buf = appendHex(buf, len(s))
				buf = append(buf, '\r', '\n')
				buf = append(buf, s...)
				buf = append(buf, '\r', '\n')
			}
			// End chunk with trailers
			buf = append(buf, "0\r\n"...)
			for _, t := range c.responseTrailers {
				buf = append(buf, t.Key...)
				buf = append(buf, ':', ' ')
				buf = append(buf, t.Value...)
				buf = append(buf, '\r', '\n')
			}
			buf = append(buf, '\r', '\n')
		} else {
			buf = append(buf, s...)
		}
	}

	*c.writeBuf = buf
	return writeAll(c.conn, buf)
}

// writeResponse writes a response with a byte body.
func (c *Ctx) writeResponse(body []byte) error {
	if c.h2 != nil {
		return c.h2.writeResponse(c, body)
	}
	if c.responded {
		return nil
	}
	if err := c.runBeforeResponse(); err != nil {
		return err
	}
	if c.bodyTransform != nil {
		var err error
		body, err = c.bodyTransform(body)
		if err != nil {
			return err
		}
	}
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]

	buf = appendStatusLine(buf, c.status)

	// Date (RFC 9110 §8.6)
	buf = append(buf, cachedDate()...)

	if c.contentType != nil {
		buf = append(buf, "Content-Type: "...)
		buf = append(buf, c.contentType...)
		buf = append(buf, '\r', '\n')
	}

	for i := 0; i < c.chCount; i++ {
		h := &c.customHeaders[i]
		buf = append(buf, h.Key...)
		buf = append(buf, ':', ' ')
		buf = append(buf, h.Value...)
		buf = append(buf, '\r', '\n')
	}
	buf = appendExtraHeaders(buf, c.extraHeaders)

	// Cookies
	for i := range c.responseCookies {
		buf = append(buf, "Set-Cookie: "...)
		buf = append(buf, c.responseCookies[i].String()...)
		buf = append(buf, '\r', '\n')
	}

	bodyAllowed := responseBodyAllowed(c.status)
	hasTrailers := len(c.responseTrailers) > 0

	if bodyAllowed && hasTrailers {
		// RFC 9112: trailers require chunked transfer encoding
		buf = append(buf, "Transfer-Encoding: chunked\r\n"...)
		buf = append(buf, "Trailer: "...)
		for i, t := range c.responseTrailers {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, t.Key...)
		}
		buf = append(buf, '\r', '\n')
	} else if bodyAllowed {
		buf = append(buf, "Content-Length: "...)
		buf = appendInt(buf, len(body))
		buf = append(buf, '\r', '\n')
	}

	// Connection
	if c.Header.KeepAlive && !c.forceClose {
		buf = append(buf, "Connection: keep-alive\r\n"...)
	} else {
		buf = append(buf, "Connection: close\r\n"...)
	}

	buf = append(buf, '\r', '\n')

	// RFC 9110: a HEAD response has the same headers as GET but no content.
	if bodyAllowed && !bytesEqualFold(c.Header.Method, MethodHEADBytes) {
		if hasTrailers {
			if len(body) > 0 {
				buf = appendHex(buf, len(body))
				buf = append(buf, '\r', '\n')
				buf = append(buf, body...)
				buf = append(buf, '\r', '\n')
			}
			buf = append(buf, "0\r\n"...)
			for _, t := range c.responseTrailers {
				buf = append(buf, t.Key...)
				buf = append(buf, ':', ' ')
				buf = append(buf, t.Value...)
				buf = append(buf, '\r', '\n')
			}
			buf = append(buf, '\r', '\n')
		} else {
			buf = append(buf, body...)
		}
	}

	*c.writeBuf = buf
	return writeAll(c.conn, buf)
}

func responseBodyAllowed(status int) bool {
	return status >= 200 && status != 204 && status != 205 && status != 304
}

func writeAll(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func appendExtraHeaders(buf []byte, headers []Header) []byte {
	for i := range headers {
		buf = append(buf, headers[i].Key...)
		buf = append(buf, ':', ' ')
		buf = append(buf, headers[i].Value...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

// appendStatusLine writes "HTTP/1.1 <code> <text>\r\n" to buf.
func appendStatusLine(buf []byte, code int) []byte {
	switch code {
	case StatusOK:
		return append(buf, "HTTP/1.1 200 OK\r\n"...)
	case StatusNotFound:
		return append(buf, "HTTP/1.1 404 Not Found\r\n"...)
	case StatusBadRequest:
		return append(buf, "HTTP/1.1 400 Bad Request\r\n"...)
	case StatusInternalServerError:
		return append(buf, "HTTP/1.1 500 Internal Server Error\r\n"...)
	}
	buf = append(buf, "HTTP/1.1 "...)
	buf = appendInt(buf, code)
	buf = append(buf, ' ')
	buf = append(buf, StatusReason(code)...)
	return append(buf, '\r', '\n')
}

// ── Helpers ────────────────────────────────────────────────────────────────

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

func urlDecode(b []byte) string {
	n := len(b)
	hasSpecial := false
	for i := 0; i < n; i++ {
		c := b[i]
		if c == '%' || c == '+' {
			hasSpecial = true
			break
		}
	}
	if !hasSpecial {
		return b2s(b)
	}
	out := make([]byte, 0, n)
	for i := 0; i < n; {
		switch b[i] {
		case '+':
			out = append(out, ' ')
			i++
		case '%':
			if i+2 < n {
				h := unhexTable[b[i+1]]
				l := unhexTable[b[i+2]]
				if h >= 0 && l >= 0 {
					out = append(out, byte(h<<4|l))
					i += 3
					continue
				}
			}
			out = append(out, b[i])
			i++
		default:
			out = append(out, b[i])
			i++
		}
	}
	return string(out)
}

var unhexTable [256]int8

func init() {
	for i := 0; i < 256; i++ {
		unhexTable[i] = -1
	}
	for i := '0'; i <= '9'; i++ {
		unhexTable[i] = int8(i - '0')
	}
	for i := 'a'; i <= 'f'; i++ {
		unhexTable[i] = int8(i - 'a' + 10)
	}
	for i := 'A'; i <= 'F'; i++ {
		unhexTable[i] = int8(i - 'A' + 10)
	}
}

var jsonCT = []byte("application/json")
