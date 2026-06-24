package fh

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	dateBuf            atomic.Value
	dateCacheUnix      int64
	dateValueBuf       atomic.Value
	dateValueCacheUnix int64
)

// cachedDate returns the full "Date: ...\r\n" header line.
func cachedDate() []byte {
	// Updated once per second by refreshDateCache. A response hot path must not
	// call time.Now(): on Linux that is still a measurable vDSO/syscall-sized tax
	// at 250k-350k RPS. atomic.Value gives readers a lock-free immutable slice.
	b, _ := dateBuf.Load().([]byte)
	return b
}

// cachedDateValue returns just the RFC 1123 date string (no "Date: " prefix).
// Used for HTTP/2 responses.
func cachedDateValue() string {
	b, _ := dateValueBuf.Load().([]byte)
	return b2s(b)
}

func refreshDateCache() {
	now := time.Now().UTC()
	unix := now.Unix()
	if unix == atomic.LoadInt64(&dateCacheUnix) {
		return
	}
	b := make([]byte, 0, 64)
	b = append(b, "Date: "...)
	b = now.AppendFormat(b, "Mon, 02 Jan 2006 15:04:05 GMT")
	b = append(b, '\r', '\n')
	v := make([]byte, 0, 56)
	v = now.AppendFormat(v, "Mon, 02 Jan 2006 15:04:05 GMT")
	dateBuf.Store(b)
	dateValueBuf.Store(v)
	atomic.StoreInt64(&dateCacheUnix, unix)
	atomic.StoreInt64(&dateValueCacheUnix, unix)
}

type Ctx interface {
	App() *App
	Append(key, value string)
	Audit() AuditRecorder
	Body() []byte
	BodyCopy() []byte
	BodyParser(v any) error
	BodyRaw() []byte
	CaptureResponseBody()
	Context() context.Context
	Deadline() (time.Time, bool)
	DelCookie(name string)
	Done() <-chan struct{}
	EchoBody(contentType ...string) error
	EchoJSON(validate ...bool) error
	Err() error
	ErrorReport(err error) ErrorReport
	ErrorResponse(err error) error
	File(filename string) error
	FirstCookie() string
	Flash(key string, value ...any) any
	FormFile(field string) (*MultipartFile, error)
	Get(name string, defaults ...string) string
	GetCookie(name string) string
	GetHeaders() map[string][]string
	GetReqHeaders() map[string][]string
	GetRespHeader(name string, defaults ...string) string
	GetRespHeaders() map[string][]string
	HasResponseCookies() bool
	Hijack(handler func(*ResponseConn) error) error
	Hostname() string
	IP() string
	JSON(v any) error
	JSONAppend(fn JSONAppendFunc) error
	JSONBytes(b []byte) error
	JSONString(s string) error
	Ledger(action, resource, resourceID string, before, after []byte) error
	Lifecycle() *RequestLifecycle
	Locals(key string, value ...any) any
	Method() string
	MultipartForm() (*MultipartForm, error)
	Next() error
	OnBeforeResponse(fn func(Ctx) error)
	OriginalURL() string
	Param(name string) string
	Params(name string, defaults ...string) string
	Path() string
	Problem(p Problem) error
	Query(name string, def ...string) string
	QueryParser(v any) error
	Queue() Queue
	Redirect(location string, code ...int) error
	RedirectBack(fallback string, code ...int) error
	RedirectTo(name string, params map[string]string, code ...int) error
	Reliability() *Reliability
	Render(name string, data any, layout ...string) error
	RequestHeader() *RequestHeader
	Responded() bool
	ResponseBody() []byte
	ResponseHeader(name string) string
	Rewrite(target string) error
	RunCompensations() error
	RunReliableEndpoint(policy ReliabilityPolicy, endpoint HandlerFunc) error
	SSE(fn func(*SSE) error) error
	SafeErrorResponse(err error) error
	SaveFile(file *MultipartFile, dst string) error
	Send(b []byte) error
	SendBytes(b []byte) error
	SendFile(filename string) error
	SendStatus(code int) error
	SendStream(r io.Reader) error
	SendString(s string) error
	ServerInbox() *Inbox
	ServerOutbox() *Outbox
	Set(key, value string)
	SetContext(ctx context.Context)
	SetCookie(cookie *Cookie)
	SetTrailer(key, value string)
	Status(code int) Ctx
	StatusCode() int
	Stream(fn func(*StreamWriter) error) error
	Trailer(name string) string
	TransformBody(fn func([]byte) ([]byte, error))
	AddBodyTransform(fn func([]byte) ([]byte, error))
	Type(mime string) Ctx
	Upgrade(protocol string, handler func(net.Conn) error) error
}

type DefaultCtx struct {
	conn   net.Conn
	server *App

	Header RequestHeader

	params []Param

	status           int
	customHeaders    [16]Header
	extraHeaders     []Header
	chCount          int
	body             []byte
	responseBody     []byte
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
	localsMu      sync.Mutex

	readBuf  *[]byte
	writeBuf *[]byte

	queryParsed bool
	queryParams []Param
	qcount      int

	responseCookies     []Cookie
	responseTime        time.Time
	beforeResponse      []func(Ctx) error
	beforeRan           bool
	multipartForm       *MultipartForm
	multipartErr        error
	multipartParsed     bool
	captureResponseBody bool
}

type localEntry struct {
	key string
	val any
}

var ctxPool = sync.Pool{
	New: func() any {
		c := &DefaultCtx{
			params:      make([]Param, 0, 8),
			queryParams: make([]Param, 0, 8),
		}
		return c
	},
}

func acquireCtx(conn net.Conn, app *App) *DefaultCtx {
	c := ctxPool.Get().(*DefaultCtx)
	c.conn = conn
	c.server = app
	c.reset()
	return c
}

func releaseCtx(c *DefaultCtx) {
	if c.writeBuf != nil {
		putBytes(c.writeBuf)
		c.writeBuf = nil
	}
	ctxPool.Put(c)
}

func (c *DefaultCtx) reset() {
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
	c.responseBody = c.responseBody[:0]
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
	c.captureResponseBody = c.server != nil && c.server.cfg.CaptureResponseBody
}

// CaptureResponseBody enables a stable in-request response body snapshot for
// middleware that must inspect or persist the final response (cache, idempotency,
// request journal). It is intentionally opt-in to keep the hot path zero-copy.
func (c *DefaultCtx) CaptureResponseBody() { c.captureResponseBody = true }

// OnBeforeResponse registers a one-shot hook run immediately before response
// headers are encoded. It is intended for transactional middleware such as
// sessions that must persist before Set-Cookie reaches the wire.
func (c *DefaultCtx) OnBeforeResponse(fn func(Ctx) error) {
	if fn != nil && !c.responded && !c.beforeRan {
		c.beforeResponse = append(c.beforeResponse, fn)
	}
}

func (c *DefaultCtx) runBeforeResponse() error {
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
func (c *DefaultCtx) Next() error {
	if c.handlerIndex >= len(c.handlers) {
		return nil
	}
	h := c.handlers[c.handlerIndex]
	c.handlerIndex++
	return h(c)
}

// ── Request accessors ──────────────────────────────────────────────────────

func (c *DefaultCtx) RequestHeader() *RequestHeader { return &c.Header }

func (c *DefaultCtx) Method() string { return string(c.Header.Method) }

// OriginalURL returns the request target as it arrived, before any Rewrite.
// The name mirrors Fiber's Ctx API.
func (c *DefaultCtx) OriginalURL() string {
	if len(c.originalURI) != 0 {
		return string(c.originalURI)
	}
	return string(c.Header.URI)
}

func (c *DefaultCtx) path() []byte {
	uri := c.Header.URI
	for i, v := range uri {
		if v == '?' {
			return uri[:i]
		}
	}
	return uri
}

func (c *DefaultCtx) Path() string { return string(c.path()) }

// Rewrite updates the request URI and asks the application to route it again.
// It is intended for rewrite middleware and is bounded by the application to
// prevent rewrite loops.
func (c *DefaultCtx) Rewrite(target string) error {
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

func (c *DefaultCtx) Param(name string) string {
	for i := range c.params {
		if c.params[i].Key == name {
			return c.params[i].Value
		}
	}
	return ""
}

// Params is the Fiber-compatible alias for Param. If the parameter is absent,
// the optional default value is returned.
func (c *DefaultCtx) Params(name string, defaults ...string) string {
	value := c.Param(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

func (c *DefaultCtx) Query(name string, def ...string) string {
	if !c.queryParsed {
		if v, ok := c.peekQuery(name); ok {
			return v
		}
		if len(def) > 0 {
			return def[0]
		}
		return ""
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

func (c *DefaultCtx) peekQuery(name string) (string, bool) {
	uri := c.Header.URI
	qi := indexByte(uri, '?')
	if qi < 0 || qi+1 >= len(uri) {
		return "", false
	}
	qs := uri[qi+1:]
	for len(qs) > 0 {
		if qs[0] == '&' {
			qs = qs[1:]
			continue
		}
		end := indexByte(qs, '&')
		pair := qs
		if end >= 0 {
			pair = qs[:end]
		}
		eq := indexByte(pair, '=')
		key, val := pair, []byte(nil)
		if eq >= 0 {
			key, val = pair[:eq], pair[eq+1:]
		}
		if rawQueryKeyEqual(key, name) {
			return urlDecode(val), true
		}
		if end < 0 {
			break
		}
		qs = qs[end+1:]
	}
	return "", false
}

func rawQueryKeyEqual(key []byte, name string) bool {
	if len(key) != len(name) {
		return false
	}
	for i := 0; i < len(name); i++ {
		if key[i] != name[i] {
			return false
		}
	}
	return true
}

func (c *DefaultCtx) parseQuery() {
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

func (c *DefaultCtx) Body() []byte { return c.body }

// BodyCopy returns a stable copy of the request body. Use it when data must
// outlive the handler, for example when enqueueing async work.
func (c *DefaultCtx) BodyCopy() []byte {
	if len(c.body) == 0 {
		return nil
	}
	out := make([]byte, len(c.body))
	copy(out, c.body)
	return out
}

// BodyRaw is the Fiber-compatible name for the unmodified request body.
func (c *DefaultCtx) BodyRaw() []byte { return c.body }

// QueryParser decodes the query string into v. The target type should be
// *map[string]any for unstructured access; struct decoding is not yet supported.
// QueryParser decodes the query string into v. Supports the same formats
// as form-encoded bodies (nested keys via bracket notation, arrays, etc.).
// Target should be *map[string]any or *any.
func (c *DefaultCtx) QueryParser(v any) error {
	uri := c.Header.URI
	qi := bytes.IndexByte(uri, '?')
	if qi < 0 {
		return nil
	}
	var fc formCodec
	return fc.Unmarshal(uri[qi+1:], v)
}

// Trailer returns a decoded chunked request trailer by name.
func (c *DefaultCtx) Trailer(name string) string {
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
func (c *DefaultCtx) SetTrailer(key, value string) {
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

func (c *DefaultCtx) BodyParser(v any) error {
	ct := b2s(c.Header.ContentType)
	if codec := matchCodec(ct); codec != nil {
		if cta, ok := codec.(ContentTypeAwareCodec); ok {
			return cta.UnmarshalWithContentType(c.body, ct, v)
		}
		return codec.Unmarshal(c.body, v)
	}
	return JSONUnmarshal(c.body, v)
}

// Context carries request cancellation and middleware deadlines.
func (c *DefaultCtx) Context() context.Context { return c.requestContext }

func (c *DefaultCtx) SetContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.requestContext = ctx
}

// Done returns a channel that is closed when the request context is cancelled
// (timeout, client disconnect, server draining). Handlers should select on
// this channel alongside their own work to implement cooperative cancellation.
func (c *DefaultCtx) Done() <-chan struct{} {
	return c.requestContext.Done()
}

// Err returns nil while the request is still active and a non-nil error
// (context.Canceled or context.DeadlineExceeded) once the context has been
// cancelled or its deadline has expired.
func (c *DefaultCtx) Err() error {
	return c.requestContext.Err()
}

// Deadline returns the time at which the request context will be cancelled,
// if a deadline has been set (e.g. via WriteTimeout or the timeout middleware).
func (c *DefaultCtx) Deadline() (time.Time, bool) {
	return c.requestContext.Deadline()
}

// TransformBody installs a buffered response transformation. It is intended
// for middleware such as gzip compression and does not affect Stream output.
func (c *DefaultCtx) TransformBody(fn func([]byte) ([]byte, error)) { c.bodyTransform = fn }

// AddBodyTransform appends a response transformation without replacing an
// existing middleware transformation.
func (c *DefaultCtx) AddBodyTransform(fn func([]byte) ([]byte, error)) {
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

func (c *DefaultCtx) Get(name string, defaults ...string) string {
	value := c.Header.PeekStr(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

// GetReqHeaders returns all request header values, preserving repeated fields.
func (c *DefaultCtx) GetReqHeaders() map[string][]string { return c.Header.GetHeaders() }

// GetHeaders is an alias for GetReqHeaders.
func (c *DefaultCtx) GetHeaders() map[string][]string { return c.GetReqHeaders() }

// Hostname returns the request host without its port.
func (c *DefaultCtx) Hostname() string {
	host := string(c.Header.Host)
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		return parsed
	}
	return strings.Trim(host, "[]")
}

func (c *DefaultCtx) Locals(key string, value ...any) any {
	c.localsMu.Lock()
	defer c.localsMu.Unlock()
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

func (c *DefaultCtx) IP() string {
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

func (c *DefaultCtx) Status(code int) Ctx {
	if code < 100 || code > 999 {
		code = 500
	}
	c.status = code
	return c
}

// StatusCode returns the current response status code.
// Used by middleware to inspect the status after calling Next().
func (c *DefaultCtx) StatusCode() int {
	return c.status
}

func (c *DefaultCtx) Set(key, value string) {
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
func (c *DefaultCtx) Append(key, value string) {
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
func (c *DefaultCtx) Responded() bool { return c.responded }

func (c *DefaultCtx) Type(mime string) Ctx {
	if strings.ContainsAny(mime, "\x00\r\n") {
		return c
	}
	c.contentType = []byte(mime)
	return c
}

// ResponseHeader returns a response header set so far.
func (c *DefaultCtx) ResponseHeader(name string) string {
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
func (c *DefaultCtx) GetRespHeader(name string, defaults ...string) string {
	value := c.ResponseHeader(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

// GetRespHeaders returns all response headers set on the context.
func (c *DefaultCtx) GetRespHeaders() map[string][]string {
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

// ResponseBody returns the currently prepared response body snapshot.
// It is primarily used by reliability/idempotency middleware. The slice is
// valid only during the request lifecycle; copy it if it must be retained.
func (c *DefaultCtx) ResponseBody() []byte { return c.responseBody }

// HasResponseCookies reports whether the response currently sets cookies.
func (c *DefaultCtx) HasResponseCookies() bool { return len(c.responseCookies) > 0 }

func (c *DefaultCtx) FirstCookie() string {
	if len(c.responseCookies) == 0 {
		return ""
	}
	return c.responseCookies[0].Value
}

func (c *DefaultCtx) SendString(s string) error {
	if c.contentType == nil {
		c.contentType = plainTextCT
	}
	return c.writeResponseString(s)
}

func (c *DefaultCtx) SendBytes(b []byte) error {
	return c.writeResponse(b)
}

func (c *DefaultCtx) Send(b []byte) error { return c.SendBytes(b) }

// JSON writes v as application/json using the active JSON engine. Types that
// implement JSONAppender are encoded directly into the response buffer, avoiding
// a marshal allocation and a second response-copy on the normal hot path.
func (c *DefaultCtx) JSON(v any) error {
	c.contentType = jsonCT
	if app, ok := v.(JSONAppender); ok {
		return c.writeJSONAppender(app)
	}
	switch vv := v.(type) {
	case map[string]string:
		return c.writeJSONMapStringString(vv)
	case map[string]any:
		return c.writeJSONMapStringAny(vv)
	}
	b, err := (jsonCodec{}).Marshal(v)
	if err != nil {
		return err
	}
	return c.writeResponse(b)
}

// JSONBytes sends an already encoded JSON document without re-marshalling.
func (c *DefaultCtx) JSONBytes(b []byte) error {
	c.contentType = jsonCT
	return c.writeResponse(b)
}

// JSONString sends an already encoded JSON document without re-marshalling.
func (c *DefaultCtx) JSONString(s string) error {
	c.contentType = jsonCT
	return c.writeResponseString(s)
}

// JSONAppend writes JSON generated directly into fh's pooled response buffer.
// This is the preferred hot-path API for small dynamic JSON responses because it
// avoids string concatenation, reflection, and a second body copy.
func (c *DefaultCtx) JSONAppend(fn JSONAppendFunc) error {
	c.contentType = jsonCT
	if fn == nil {
		return c.JSONBytes([]byte("null"))
	}
	return c.writeJSONAppender(fn)
}

// EchoBody sends the request body back without parsing or copying. This is the
// correct hot-path primitive for proxy, webhook, and raw echo endpoints; do not
// decode and re-encode JSON just to return the same payload.
func (c *DefaultCtx) EchoBody(contentType ...string) error {
	if len(contentType) > 0 && contentType[0] != "" {
		c.Type(contentType[0])
	} else if len(c.Header.ContentType) > 0 {
		c.contentType = c.Header.ContentType
	}
	return c.writeResponse(c.body)
}

// EchoJSON sends the request body back as JSON. By default it trusts upstream
// validation for maximum throughput. Pass true to validate with the active JSON
// engine before echoing.
func (c *DefaultCtx) EchoJSON(validate ...bool) error {
	if len(validate) > 0 && validate[0] && !CurrentJSONEngine().Valid(c.body) {
		return BadRequest("Invalid JSON body")
	}
	c.contentType = jsonCT
	return c.writeResponse(c.body)
}

func (c *DefaultCtx) Render(name string, data any, layout ...string) error {
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

func (c *DefaultCtx) SendStatus(code int) error {
	c.status = code
	return c.writeResponse(nil)
}

func (c *DefaultCtx) Redirect(location string, code ...int) error {
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
func (c *DefaultCtx) RedirectTo(name string, params map[string]string, code ...int) error {
	location, err := c.server.URL(name, params)
	if err != nil {
		return err
	}
	return c.Redirect(location, code...)
}

// RedirectBack redirects to a same-origin Referer, or to fallback when the
// Referer is absent, malformed, or points at another host.
func (c *DefaultCtx) RedirectBack(fallback string, code ...int) error {
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
func (c *DefaultCtx) Flash(key string, value ...any) any {
	store, ok := c.Locals("session").(contextFlashStore)
	if !ok {
		panic("fasthttp: flash messages require session middleware")
	}
	return store.Flash(key, value...)
}

// App returns the owning application instance for advanced integrations.
func (c *DefaultCtx) App() *App { return c.server }

// ServerOutbox returns the reliability outbox for the current app.
func (c *DefaultCtx) ServerOutbox() *Outbox {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Outbox()
}

// ServerInbox returns the reliability inbox for the current app.
func (c *DefaultCtx) ServerInbox() *Inbox {
	if c == nil || c.server == nil {
		return nil
	}
	return c.server.Inbox()
}

// writeResponseString writes a response with a string body — zero alloc.
func (c *DefaultCtx) writeResponseString(s string) error {
	if c.captureResponseBody {
		c.responseBody = append(c.responseBody[:0], s...)
	} else {
		c.responseBody = c.responseBody[:0]
	}
	if c.canFastWrite200() {
		return c.writeFast200String(s)
	}
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

	if c.server.cfg.SendDateHeader {
		buf = append(buf, cachedDate()...)
	}

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

	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			buf = append(buf, "Connection: keep-alive\r\n"...)
		}
	} else {
		buf = append(buf, "Connection: close\r\n"...)
	}

	buf = append(buf, '\r', '\n')

	// Body
	if bodyAllowed && !methodIs(c.Header.Method, 'H', 'E', 'A', 'D') {
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

func (c *DefaultCtx) writeJSONMapStringString(m map[string]string) error {
	bp := jsonBytePool.Get().(*[]byte)
	body := (*bp)[:0]
	body = append(body, '{')
	i := 0
	for k, v := range m {
		if i > 0 {
			body = append(body, ',')
		}
		body = appendJSONString(body, k)
		body = append(body, ':')
		body = appendJSONString(body, v)
		i++
	}
	body = append(body, '}')
	err := c.writeResponse(body)
	if cap(body) <= 64<<10 {
		*bp = body[:0]
		jsonBytePool.Put(bp)
	}
	return err
}

func (c *DefaultCtx) writeJSONMapStringAny(m map[string]any) error {
	bp := jsonBytePool.Get().(*[]byte)
	body := (*bp)[:0]
	body = append(body, '{')
	i := 0
	for k, v := range m {
		if i > 0 {
			body = append(body, ',')
		}
		body = appendJSONString(body, k)
		body = append(body, ':')
		var err error
		body, err = appendJSONValue(body, v)
		if err != nil {
			*bp = body[:0]
			jsonBytePool.Put(bp)
			return err
		}
		i++
	}
	body = append(body, '}')
	err := c.writeResponse(body)
	if cap(body) <= 64<<10 {
		*bp = body[:0]
		jsonBytePool.Put(bp)
	}
	return err
}

func appendJSONValue(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case string:
		return appendJSONString(dst, x), nil
	case bool:
		if x {
			return append(dst, "true"...), nil
		}
		return append(dst, "false"...), nil
	case int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int8:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case uint:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint8:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(dst, x, 10), nil
	case float32:
		return strconv.AppendFloat(dst, float64(x), 'g', -1, 32), nil
	case float64:
		return strconv.AppendFloat(dst, x, 'g', -1, 64), nil
	case JSONAppender:
		return x.AppendJSON(dst)
	case JSONMarshaler:
		b, err := x.MarshalJSON()
		if err != nil {
			return dst, err
		}
		return append(dst, b...), nil
	default:
		b, err := CurrentJSONEngine().Marshal(v)
		if err != nil {
			return dst, err
		}
		return append(dst, b...), nil
	}
}

func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c == '\\' || c == '"' {
			dst = append(dst, s[start:i]...)
			switch c {
			case '\\', '"':
				dst = append(dst, '\\', c)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexLower[c>>4], hexLower[c&0x0f])
			}
			start = i + 1
		}
	}
	dst = append(dst, s[start:]...)
	dst = append(dst, '"')
	return dst
}

const hexLower = "0123456789abcdef"

func (c *DefaultCtx) writeJSONAppender(app JSONAppender) error {
	bp := jsonBytePool.Get().(*[]byte)
	body := (*bp)[:0]
	out, err := app.AppendJSON(body)
	if err != nil {
		*bp = body[:0]
		jsonBytePool.Put(bp)
		return err
	}
	err = c.writeResponse(out)
	if cap(out) <= 64<<10 {
		*bp = out[:0]
		jsonBytePool.Put(bp)
	}
	return err
}

// writeResponse writes a response with a byte body.
func (c *DefaultCtx) writeResponse(body []byte) error {
	if c.captureResponseBody {
		c.responseBody = append(c.responseBody[:0], body...)
	} else {
		c.responseBody = c.responseBody[:0]
	}
	if c.canFastWrite200() {
		return c.writeFast200Bytes(body)
	}
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

	if c.server.cfg.SendDateHeader {
		buf = append(buf, cachedDate()...)
	}

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

	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			buf = append(buf, "Connection: keep-alive\r\n"...)
		}
	} else {
		buf = append(buf, "Connection: close\r\n"...)
	}

	buf = append(buf, '\r', '\n')

	// RFC 9110: a HEAD response has the same headers as GET but no content.
	if bodyAllowed && !methodIs(c.Header.Method, 'H', 'E', 'A', 'D') {
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

func (c *DefaultCtx) canFastWrite200() bool {
	return !c.responded && c.status == StatusOK && c.h2 == nil && c.bodyTransform == nil && !c.captureResponseBody &&
		c.chCount == 0 && len(c.extraHeaders) == 0 && len(c.responseCookies) == 0 && len(c.responseTrailers) == 0 &&
		len(c.beforeResponse) == 0 && !methodIs(c.Header.Method, 'H', 'E', 'A', 'D')
}

func (c *DefaultCtx) writeFast200String(s string) error {
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]
	buf = append(buf, "HTTP/1.1 200 OK\r\n"...)
	if c.server.cfg.SendDateHeader {
		buf = append(buf, cachedDate()...)
	}
	if c.contentType != nil {
		buf = append(buf, "Content-Type: "...)
		buf = append(buf, c.contentType...)
		buf = append(buf, '\r', '\n')
	}
	buf = append(buf, "Content-Length: "...)
	buf = appendInt(buf, len(s))
	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			buf = append(buf, "\r\nConnection: keep-alive\r\n\r\n"...)
		} else {
			buf = append(buf, "\r\n\r\n"...)
		}
	} else {
		buf = append(buf, "\r\nConnection: close\r\n\r\n"...)
	}
	buf = append(buf, s...)
	*c.writeBuf = buf
	return writeAll(c.conn, buf)
}

func (c *DefaultCtx) writeFast200Bytes(body []byte) error {
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]
	buf = append(buf, "HTTP/1.1 200 OK\r\n"...)
	if c.server.cfg.SendDateHeader {
		buf = append(buf, cachedDate()...)
	}
	if c.contentType != nil {
		buf = append(buf, "Content-Type: "...)
		buf = append(buf, c.contentType...)
		buf = append(buf, '\r', '\n')
	}
	buf = append(buf, "Content-Length: "...)
	buf = appendInt(buf, len(body))
	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			buf = append(buf, "\r\nConnection: keep-alive\r\n\r\n"...)
		} else {
			buf = append(buf, "\r\n\r\n"...)
		}
	} else {
		buf = append(buf, "\r\nConnection: close\r\n\r\n"...)
	}
	if len(body) >= writevBodyThreshold {
		*c.writeBuf = buf
		return writeBuffers(c.conn, buf, body)
	}
	buf = append(buf, body...)
	*c.writeBuf = buf
	return writeAll(c.conn, buf)
}

func responseBodyAllowed(status int) bool {
	return status >= 200 && status != 204 && status != 205 && status != 304
}

const writevBodyThreshold = 512

func writeBuffers(conn net.Conn, bufs ...[]byte) error {
	// net.Buffers uses writev on Unix for TCP connections, avoiding a body copy
	// into the response header buffer for echo/proxy/large JSON responses.
	var nb net.Buffers = bufs
	_, err := nb.WriteTo(conn)
	return err
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
	refreshDateCache()
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			refreshDateCache()
		}
	}()

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
