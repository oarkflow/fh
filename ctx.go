package fh

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ctxFlagH2            = 1 << 0
	ctxFlagBodyTransform = 1 << 1
	ctxFlagCaptureBody   = 1 << 2
	ctxFlagHasExtraResp  = 1 << 3 // headers, cookies, trailers, beforeResponse
	ctxFlagHEAD          = 1 << 4
	ctxFlagNon200        = 1 << 5 // status != StatusOK
	ctxFlagH2Connect     = 1 << 6 // RFC 8441 extended CONNECT stream (c.h2.stream.protocol != "")
	ctxFlagAutoETag      = 1 << 7
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

// Ctx is the public request/response context contract used by handlers and middleware.
// DefaultCtx is the built-in implementation used by App. Custom implementations can
// be supplied in tests or adapters by implementing this interface.
type Ctx interface {
	Next() error
	Method() string
	MethodBytes() []byte
	OriginalURL() string
	Path() string
	Rewrite(target string) error
	Param(name string) string
	Params(name string, defaults ...string) string
	Query(name string, def ...string) string
	Body() []byte
	BodyCopy() []byte
	BodyRaw() []byte
	QueryParser(v any) error
	HeaderParser(v any) error
	Trailer(name string) string
	SetTrailer(key, value string)
	BodyParser(v any) error
	Context() context.Context
	SetContext(ctx context.Context)
	Done() <-chan struct{}
	Err() error
	Deadline() (time.Time, bool)
	TransformBody(fn func([]byte) ([]byte, error))
	AddBodyTransform(fn func([]byte) ([]byte, error))
	Get(name string, defaults ...string) string
	GetReqHeaders() map[string][]string
	GetHeaders() map[string][]string
	ConnectProtocol() string
	Hostname() string
	Locals(key string, value ...any) any
	IP() string
	Status(code int) Ctx
	StatusCode() int
	Set(key, value string)
	SetAltSvc(value string) Ctx
	RequestPreference(name string) bool
	SetPreferenceApplied(value string) Ctx
	Append(key, value string)
	Responded() bool
	Type(mime string) Ctx
	ResponseHeader(name string) string
	GetRespHeader(name string, defaults ...string) string
	GetRespHeaders() map[string][]string
	ResponseBody() []byte
	HasResponseCookies() bool
	FirstCookie() string
	SendString(s string) error
	HTML(s string) error
	SendBytes(b []byte) error
	Send(b []byte) error
	JSON(v any) error
	JSONBytes(b []byte) error
	JSONString(s string) error
	JSONAppend(fn JSONAppendFunc) error
	EchoBody(contentType ...string) error
	EchoJSON(validate ...bool) error
	Render(name string, data any, layout ...string) error
	SendStatus(code int) error
	Redirect(location string, code ...int) error
	RedirectTo(name string, params map[string]string, code ...int) error
	RedirectBack(fallback string, code ...int) error
	Flash(key string, value ...any) any
	// FlashAll retrieves and consumes all pending flash data atomically.
	// Returns nil when there is no flash data. Requires session middleware.
	FlashAll() map[string]any
	// RedirectWithFlash sets one or more flash key/value pairs then redirects.
	// Flash data is available exactly once on the next request.
	RedirectWithFlash(location string, code int, flash map[string]any) error
	App() *App
	ServerOutbox() *Outbox
	ServerInbox() *Inbox
	CaptureResponseBody()
	OnBeforeResponse(fn func(Ctx) error)
	SetCookie(cookie *Cookie)
	GetCookie(name string) string
	DelCookie(name string)
	Problem(p Problem) error
	ProblemDetails(status int, title, detail, typeURI string) error
	Bind(v any) error
	BindJSON(v any) error
	BindQuery(v any) error
	BindForm(v any) error
	BindHeader(v any) error
	SSEvent(event string, data any) error
	ErrorReport(err error) ErrorReport
	ErrorResponse(err error) error
	SafeErrorResponse(err error) error
	Stream(fn func(*StreamWriter) error) error
	SendStream(r io.Reader) error
	Hijack(handler func(*ResponseConn) error) error
	Upgrade(protocol string, handler func(net.Conn) error) error
	Attachment(filename string) Ctx
	SendFile(filename string) error
	File(filename string) error
	Download(filename string, downloadName ...string) error
	MultipartForm() (*MultipartForm, error)
	FormFile(field string) (*MultipartFile, error)
	SaveFile(file *MultipartFile, dst string) error
	Audit() AuditRecorder
	Ledger(action, resource, resourceID string, before, after []byte) error
	Lifecycle() *RequestLifecycle
	Compensate(fn func(context.Context) error)
	RunCompensations() error
	SSE(fn func(*SSE) error) error
	Reliability() *Reliability
	RunReliableEndpoint(policy ReliabilityPolicy, endpoint HandlerFunc) error
	Queue() Queue
	RequestHeader() *RequestHeader
	RequestPriority() HTTPPriority
	SetResponsePriority(priority HTTPPriority) Ctx
	AutoETag() Ctx
	EarlyHint(uri string) bool
	EarlyHintsWithHeaders(uri string, attrs map[string]string) bool
	Send103EarlyHints(links []string) bool
	SendInformational(status int, headers map[string]string) bool

	// ── Fiber-compatible convenience methods ───────────────────────────

	// QueryBool returns the query parameter as a bool.
	// "true", "1", "yes" (case-insensitive) → true; everything else → false.
	QueryBool(key string, def ...bool) bool
	// QueryInt returns the query parameter parsed as int, or the default value.
	QueryInt(key string, def ...int) int
	// QueryFloat returns the query parameter parsed as float64, or the default value.
	QueryFloat(key string, def ...float64) float64
	// ParamsInt returns the named route parameter parsed as int, or 0 on error.
	ParamsInt(key string) (int, error)
	// AllParams returns all named route parameters as a map.
	AllParams() map[string]string
	// CookieParser binds cookie values into a struct using the "cookie" tag.
	CookieParser(v any) error
	// ParamsParser binds named route parameters into a struct using the "params" tag.
	ParamsParser(v any) error
	// JSONP sends a JSONP response wrapped in the given callback function.
	JSONP(data any, callback ...string) error
	// Links joins the given URIs into a Link response header field (RFC 8288).
	Links(link ...string)
	// Location sets the Location response header to the given path.
	Location(path string)
	// Fresh checks whether the request is fresh based on ETag/If-None-Match
	// and Last-Modified/If-Modified-Since headers. Returns true when the
	// client cache is still valid and a 304 may be sent.
	Fresh() bool
	// Secure returns true when the connection uses TLS.
	Secure() bool
	// IsFromLocal returns true when the request originates from a loopback address.
	IsFromLocal() bool

	// ── Modern HTTP server helpers ──────────────────────────────────────

	// Vary appends field names to the Vary response header without duplicating existing values.
	Vary(fields ...string)
	// IsXHR returns true when the request was made with XMLHttpRequest (X-Requested-With: XMLHttpRequest).
	IsXHR() bool
	// Protocol returns the request scheme: "https" when the connection is TLS, otherwise "http".
	Protocol() string
	// Subdomains returns subdomain segments of the Host, offset from the right (default 2).
	// e.g. for Host=api.v2.example.com with offset=2 → ["v2", "api"].
	Subdomains(offset ...int) []string
	// BaseURL returns scheme://host (no trailing slash).
	BaseURL() string
	// Accepts returns the best MIME type match from the client's Accept header.
	// Returns the first offered type when the header is absent.
	// Returns "" when none of the offered types are acceptable.
	Accepts(offers ...string) string
	// AcceptsCharsets returns the best charset match from Accept-Charset.
	AcceptsCharsets(offers ...string) string
	// AcceptsEncodings returns the best encoding match from Accept-Encoding.
	AcceptsEncodings(offers ...string) string
	// AcceptsLanguages returns the best language match from Accept-Language.
	AcceptsLanguages(offers ...string) string
	// XML encodes v as XML and sends it with Content-Type application/xml; charset=utf-8.
	XML(v any) error
	// Format performs content-type negotiation from the Accept header and dispatches
	// to the matching handler. Falls through to Next when no handler matches.
	Format(handlers map[string]HandlerFunc) error
	// Range parses the Range request header for a resource of the given total size.
	// Returns nil, nil when no Range header is present (caller should serve the full resource).
	// Returns a 416 status error when the range is syntactically valid but unsatisfiable.
	Range(size int64) ([]ByteRange, error)
	// ClearCookie expires cookies by name. With no arguments all staged response
	// cookies are expired. The name must match the name used when the cookie was set.
	ClearCookie(name ...string)
	// QueryMultiple returns all values for a repeated query parameter.
	QueryMultiple(name string) []string
	// IPs returns all IP addresses from the X-Forwarded-For chain, left to right.
	IPs() []string
	// ClearSiteData sets the Clear-Site-Data response header (e.g. "cache", "cookies", "storage", "executionContexts", "*").
	ClearSiteData(directives ...string)
	// AcceptCH sets the Accept-CH response header for User-Agent Client Hints.
	AcceptCH(hints ...string)
	// CriticalCH sets the Critical-CH response header and adds them to Accept-CH.
	CriticalCH(hints ...string)
	// StaleWhileRevalidate appends the stale-while-revalidate directive to Cache-Control.
	StaleWhileRevalidate(d time.Duration)
	// SendContinue sends an intermediate 100 Continue response.
	SendContinue() error
	// LastEventID returns the Last-Event-ID header or ?lastEventId= query param.
	LastEventID() string
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

	writeBuf *[]byte
	// writeBufPooled distinguishes the general pool used by standalone/H2
	// contexts from the HTTP/1 connection-owned buffer. A keep-alive connection
	// processes requests serially, so its response buffer can be reused without
	// a sync.Pool round trip on every request.
	writeBufPooled  bool
	connectionOwned bool

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
	bodyParserMapPtr    uintptr
	bodyParserRawJSON   []byte
	flags               uint32
}

type localEntry struct {
	key string
	val any
}

// ByteRange represents a single parsed segment from a Range request header.
// Both Start and End are byte offsets, inclusive. Start=0, End=N-1 for the
// entire resource of size N.
type ByteRange struct {
	Start int64
	End   int64
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
	c.connectionOwned = false
	c.reset()
	return c
}

func acquireHTTP1Ctx(conn net.Conn, app *App, state *connState) *DefaultCtx {
	if state == nil {
		return acquireCtx(conn, app)
	}
	c := state.ctx
	if c == nil {
		c = &DefaultCtx{
			params:      make([]Param, 0, 8),
			queryParams: make([]Param, 0, 8),
		}
		state.ctx = c
	}
	c.conn = conn
	c.server = app
	c.connectionOwned = true
	c.reset()
	c.writeBuf = &state.writeBuf
	return c
}

func releaseCtx(c *DefaultCtx) {
	if c.writeBuf != nil {
		if c.writeBufPooled {
			putBytes(c.writeBuf)
		}
		c.writeBuf = nil
		c.writeBufPooled = false
	}
	if !c.connectionOwned {
		ctxPool.Put(c)
	}
}

func (c *DefaultCtx) reset() {
	retainConnectionRefs := c.connectionOwned && c.server != nil && c.server.fastHTTP1
	if retainConnectionRefs {
		c.Header.resetRetained()
	} else {
		c.Header.reset()
	}
	c.params = c.params[:0]
	c.status = 200
	if c.chCount > 0 && !retainConnectionRefs {
		clear(c.customHeaders[:c.chCount])
	}
	c.chCount = 0
	c.extraHeaders = c.extraHeaders[:0]
	c.body = nil
	c.responseBody = c.responseBody[:0]
	c.contentType = nil
	c.responded = false
	c.forceClose = false
	c.upgraded = false
	c.upgradeBuffered = nil
	c.trailers = c.trailers[:0]
	c.responseTrailers = c.responseTrailers[:0]
	c.requestContext = context.Background()
	c.originalURI = c.originalURI[:0]
	c.bodyTransform = nil
	c.h2 = nil
	if c.lcount > 0 && !retainConnectionRefs {
		clear(c.locals[:c.lcount])
	}
	c.lcount = 0
	if len(c.localOverflow) > 0 {
		clear(c.localOverflow)
	}
	c.queryParsed = false
	c.qcount = 0
	c.queryParams = c.queryParams[:0]
	c.handlers = nil
	c.handlerIndex = 0
	c.cachedIP = ""
	c.responseCookies = c.responseCookies[:0]
	c.responseTime = time.Time{}
	c.beforeResponse = c.beforeResponse[:0]
	c.beforeRan = false
	c.multipartForm = nil
	c.multipartErr = nil
	c.multipartParsed = false
	c.flags = 0
	c.captureResponseBody = c.server != nil && c.server.cfg.CaptureResponseBody
	if c.captureResponseBody {
		c.flags |= ctxFlagCaptureBody
	}
	c.bodyParserMapPtr = 0
	c.bodyParserRawJSON = nil
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
		c.flags |= ctxFlagHasExtraResp
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

func (c *DefaultCtx) RequestHeader() *RequestHeader { return &c.Header }

// ── Request accessors ──────────────────────────────────────────────────────

func (c *DefaultCtx) Method() string { return string(c.Header.Method) }

// MethodBytes returns the request method without allocation. The slice is valid
// only during the handler lifetime.
func (c *DefaultCtx) MethodBytes() []byte { return c.Header.Method }

// PathBytes returns the route path without allocation. The slice is valid only
// during the handler lifetime.
func (c *DefaultCtx) PathBytes() []byte { return c.path() }

// OriginalURLBytes returns the exact request target from the request line when
// available, without allocation. It is valid only during the handler lifetime.
func (c *DefaultCtx) OriginalURLBytes() []byte {
	if len(c.originalURI) != 0 {
		return c.originalURI
	}
	if len(c.Header.RequestTarget) != 0 {
		return c.Header.RequestTarget
	}
	return c.Header.URI
}

// QueryBytes returns a raw query parameter value without decoding or allocation.
// Use Query when percent-decoding/string ownership is required.
func (c *DefaultCtx) QueryBytes(name []byte) []byte {
	qs := c.Header.QueryString
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
		if bytes.Equal(key, name) {
			return val
		}
		if end < 0 {
			break
		}
		qs = qs[end+1:]
	}
	return nil
}

// HeaderBytes returns a request header value without allocation.
func (c *DefaultCtx) HeaderBytes(name []byte) []byte { return c.Header.Peek(name) }

// OriginalURL returns the request target as it arrived, before any Rewrite.
// The name mirrors Fiber's Ctx API.
func (c *DefaultCtx) OriginalURL() string {
	if len(c.originalURI) != 0 {
		return string(c.originalURI)
	}
	if len(c.Header.RequestTarget) != 0 {
		return string(c.Header.RequestTarget)
	}
	return string(c.Header.URI)
}

func (c *DefaultCtx) path() []byte {
	if c.Header.Path != nil {
		return c.Header.Path
	}
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
	if target == "" || target[0] != '/' || strings.ContainsAny(target, "\x00\r\n") || len(target) > 1 && target[1] == '/' {
		return BadRequest("Invalid rewrite target")
	}
	c.Header.URI = []byte(target)
	if qi := strings.IndexByte(target, '?'); qi >= 0 {
		c.Header.Path = c.Header.URI[:qi]
		if qi+1 < len(c.Header.URI) {
			c.Header.QueryString = c.Header.URI[qi+1:]
		} else {
			c.Header.QueryString = c.Header.URI[:0]
		}
	} else {
		c.Header.Path = c.Header.URI
		c.Header.QueryString = nil
	}
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
	qs := c.Header.QueryString
	if len(qs) == 0 {
		return "", false
	}
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
	qs := c.Header.QueryString
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
	query := uri[qi+1:]
	if len(query) > 32<<10 {
		return PayloadTooLarge("Query string too large")
	}
	var fc formCodec
	return fc.Unmarshal(query, v)
}

// HeaderParser decodes request headers into v using `header` struct tags.
// Supported target types: *map[string]string, *map[string][]string, or a
// struct pointer where each exported field is matched by the header name
// specified in its `header:"name"` tag (case-insensitive).  When the tag is
// absent the lower-cased field name is used.  Supported field types are
// string, []string, int, int64, uint64, float64, bool, and *string.
func (c *DefaultCtx) HeaderParser(v any) error {
	return DecodeHeaders(&c.Header, v)
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
	c.flags |= ctxFlagHasExtraResp
}

func (c *DefaultCtx) BodyParser(v any) error {
	if c.server != nil && c.server.cfg.MaxRequestBodySize > 0 && len(c.body) > c.server.cfg.MaxRequestBodySize {
		return PayloadTooLarge("Request body too large")
	}
	ct := b2s(c.Header.ContentType)
	var err error
	if codec := matchCodec(ct); codec != nil {
		if cta, ok := codec.(ContentTypeAwareCodec); ok {
			err = cta.UnmarshalWithContentType(c.body, ct, v)
		} else {
			err = codec.Unmarshal(c.body, v)
		}
	} else {
		err = JSONUnmarshal(c.body, v)
	}
	if err != nil {
		return err
	}
	// Benchmark/common webhook path: BodyParser(&map[string]any) followed by
	// c.JSON(body). Keep the parsed map identity so JSON can legally reuse the
	// original raw JSON when the exact same map is written back unchanged. This
	// preserves handler source parity while avoiding an unnecessary re-marshal on
	// echo-style endpoints.
	if m, ok := v.(*map[string]any); ok && m != nil && *m != nil && isJSONContentTypeBytes(c.Header.ContentType) {
		c.bodyParserMapPtr = mapPointer(*m)
		c.bodyParserRawJSON = c.body
	}
	return nil
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
func (c *DefaultCtx) TransformBody(fn func([]byte) ([]byte, error)) {
	c.bodyTransform = fn
	c.flags |= ctxFlagBodyTransform
}

// AddBodyTransform appends a response transformation without replacing an
// existing middleware transformation.
func (c *DefaultCtx) AddBodyTransform(fn func([]byte) ([]byte, error)) {
	if fn == nil {
		return
	}
	previous := c.bodyTransform
	if previous == nil {
		c.bodyTransform = fn
		c.flags |= ctxFlagBodyTransform
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

// ConnectProtocol returns the negotiated protocol for an RFC 8441 extended
// CONNECT request (the HTTP/2 :protocol pseudo-header, e.g. "websocket").
// It returns "" for HTTP/1.1 requests and for HTTP/2 requests that are not
// extended CONNECT — use it to detect HTTP/2 upgrade-eligible requests
// without relying on the HTTP/1.1-only Connection/Upgrade headers, which
// HTTP/2 forbids.
func (c *DefaultCtx) ConnectProtocol() string {
	if c.flags&ctxFlagH2Connect == 0 || c.h2 == nil {
		return ""
	}
	return c.h2.stream.protocol
}

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
	if code != StatusOK {
		c.flags |= ctxFlagNon200
	}
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
	if len(v) > 16<<10 {
		return
	}
	if bytesEqualFold(k, HeaderContentLengthBytes) || bytesEqualFold(k, HeaderTransferEncodingBytes) || bytesEqualFold(k, HeaderConnectionBytes) || bytesEqualFold(k, HeaderKeepAliveBytes) || bytesEqualFold(k, HeaderProxyAuthorizationBytes) || bytesEqualFold(k, HeaderTEBytes) || bytesEqualFold(k, HeaderTrailerBytes) || bytesEqualFold(k, HeaderUpgradeBytes) || bytesEqualFold(k, []byte("Proxy-Connection")) {
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
	c.flags |= ctxFlagHasExtraResp
}

// setStaticHeader mirrors Set's dedup/storage behavior for a header whose key
// and value are pre-validated, immutable byte slices shared across requests
// (e.g. hardcoded security headers built once at middleware construction).
// It skips the []byte conversions, forbidden-header blocklist scan, and
// validToken/ContainsAny checks Set performs for arbitrary caller input,
// since the caller already guarantees name is a safe, non-reserved token and
// value contains no control characters. Callers must never mutate k or v
// afterward since the backing array may be reused across requests.
func (c *DefaultCtx) setStaticHeader(k, v []byte) {
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
	c.flags |= ctxFlagHasExtraResp
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
	if c.server != nil && (c.server.cfg.Mode == ModeStrict || c.server.cfg.Mode == ModeEnterprise || c.server.cfg.Compliance.Strict) {
		if !strings.Contains(mime, "/") {
			return c
		}
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

func (c *DefaultCtx) SetAltSvc(value string) Ctx {
	if !strings.ContainsAny(value, "\x00\r\n") {
		c.Set(HeaderAltSvc, value)
	}
	return c
}

func (c *DefaultCtx) HTML(s string) error {
	c.contentType = []byte(MIMETextHTMLCharsetUTF8)
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
	if c.contentType == nil || !isJSONContentTypeBytes(c.contentType) {
		c.contentType = jsonCT
	}
	if app, ok := v.(JSONAppender); ok {
		return c.writeJSONAppender(app)
	}
	switch vv := v.(type) {
	case map[string]string:
		return c.writeJSONMapStringString(vv)
	case Map:
		return c.writeJSONMapStringAny(map[string]any(vv))
	case map[string]any:
		if c.bodyParserMapPtr != 0 && c.bodyParserMapPtr == mapPointer(vv) && jsonLooksObjectOrArray(c.bodyParserRawJSON) {
			return c.EchoJSON()
		}
		return c.writeJSONMapStringAny(vv)
	}
	if supportsJSON(v) {
		return c.writeJSONAppender(JSONValue{v: v})
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
		return NewHTTPError(StatusInternalServerError, "TEMPLATE_ENGINE_MISSING", "fh: no template engine configured")
	}
	// Auto-inject flash data into template rendering.
	// Flash data is consumed atomically on first access.
	flash := c.FlashAll()
	if len(flash) > 0 {
		data = mergeFlashData(data, flash)
	}
	var buf bytes.Buffer
	if err := engine.Render(&buf, name, data, layout...); err != nil {
		return err
	}
	c.contentType = []byte("text/html; charset=utf-8")
	return c.writeResponse(buf.Bytes())
}

// mergeFlashData merges flash key/value pairs into the template data.
// If data is a map, flash keys are added directly and override placeholder defaults.
// If data is a struct or nil, a new map is created wrapping the original data.
func mergeFlashData(data any, flash map[string]any) any {
	if data == nil {
		return flash
	}
	if m, ok := data.(map[string]any); ok {
		maps.Copy(m, flash)
		return m
	}
	if m, ok := data.(Map); ok {
		maps.Copy(m, flash)
		return m
	}
	// For structs, wrap in a map so templates can access both.
	return map[string]any{
		"Data":  data,
		"Flash": flash,
	}
}

func (c *DefaultCtx) SendStatus(code int) error {
	c.status = code
	if code != StatusOK {
		c.flags |= ctxFlagNon200
	}
	return c.writeResponse(nil)
}

func (c *DefaultCtx) Redirect(location string, code ...int) error {
	sc := 302
	if len(code) > 0 {
		sc = code[0]
	}
	if len(location) > 8192 {
		return BadRequest("Redirect location too long")
	}
	// Block dangerous schemes
	lower := strings.ToLower(location)
	if strings.HasPrefix(lower, "javascript:") || strings.HasPrefix(lower, "data:") || strings.HasPrefix(lower, "vbscript:") {
		return BadRequest("Invalid redirect location")
	}
	// Block protocol-relative URLs
	if len(location) > 1 && location[0] == '/' && location[1] == '/' {
		return BadRequest("Invalid redirect location")
	}
	c.status = sc
	c.flags |= ctxFlagNon200
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
				cleaned := path.Clean(ref.Path)
				if !strings.HasPrefix(cleaned, "//") && !strings.Contains(cleaned, "\\") {
					ref.Path = cleaned
					location = ref.RequestURI()
				}
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
		panic("fh: flash messages require session middleware")
	}
	return store.Flash(key, value...)
}

type contextFlashAllStore interface {
	FlashAll() map[string]any
}

// FlashAll retrieves and consumes all pending flash data atomically.
// Returns nil when there is no flash data. Requires session middleware.
func (c *DefaultCtx) FlashAll() map[string]any {
	store, ok := c.Locals("session").(contextFlashAllStore)
	if !ok {
		return nil
	}
	return store.FlashAll()
}

// RedirectWithFlash sets one or more flash key/value pairs then redirects.
// Flash data is available exactly once on the next request.
func (c *DefaultCtx) RedirectWithFlash(location string, code int, flash map[string]any) error {
	for k, v := range flash {
		c.Flash(k, v)
	}
	return c.Redirect(location, code)
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
	if c.canDirectWrite200() {
		return c.writeDirect200String(s)
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
		c.writeBufPooled = true
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

	buf = c.appendSecurityHeaders(buf)

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
		buf = appendContentLengthLine(buf, len(s))
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
	if bodyAllowed && c.flags&ctxFlagHEAD == 0 {
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
	if c.canDirectWrite200() {
		if c.writeBuf == nil {
			c.writeBuf = getBytes()
			c.writeBufPooled = true
		}
		base := (*c.writeBuf)[:0]
		if cap(base) < directJSONHeaderReserve {
			base = append(base, make([]byte, directJSONHeaderReserve)...)
		} else {
			base = base[:directJSONHeaderReserve]
		}
		body := appendJSONMapStringString(base[directJSONHeaderReserve:directJSONHeaderReserve], m)
		return c.writeDirectJSONBytes200(base, body)
	}
	bp := jsonBytePool.Get().(*[]byte)
	body := appendJSONMapStringString((*bp)[:0], m)
	err := c.writeResponse(body)
	if cap(body) <= 64<<10 {
		*bp = body[:0]
		jsonBytePool.Put(bp)
	}
	return err
}

func appendJSONMapStringString(body []byte, m map[string]string) []byte {
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
	return append(body, '}')
}

func (c *DefaultCtx) writeJSONMapStringAny(m map[string]any) error {
	if c.canDirectWrite200() {
		if c.writeBuf == nil {
			c.writeBuf = getBytes()
			c.writeBufPooled = true
		}
		base := (*c.writeBuf)[:0]
		if cap(base) < directJSONHeaderReserve {
			base = append(base, make([]byte, directJSONHeaderReserve)...)
		} else {
			base = base[:directJSONHeaderReserve]
		}
		body, err := appendJSONMapStringAny(base[directJSONHeaderReserve:directJSONHeaderReserve], m)
		if err != nil {
			*c.writeBuf = base[:0]
			return err
		}
		return c.writeDirectJSONBytes200(base, body)
	}
	bp := jsonBytePool.Get().(*[]byte)
	body, err := appendJSONMapStringAny((*bp)[:0], m)
	if err != nil {
		*bp = body[:0]
		jsonBytePool.Put(bp)
		return err
	}
	err = c.writeResponse(body)
	if cap(body) <= 64<<10 {
		*bp = body[:0]
		jsonBytePool.Put(bp)
	}
	return err
}

func appendJSONMapStringAny(body []byte, m map[string]any) ([]byte, error) {
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
			return body, err
		}
		i++
	}
	return append(body, '}'), nil
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

// AppendJSONString appends s as a complete quoted JSON string.
func AppendJSONString(dst []byte, s string) []byte { return appendJSONString(dst, s) }

// AppendJSONStringContent appends s escaped for JSON string content without surrounding quotes.
// It is useful with JSONAppend when callers already wrote the opening/closing quote.
func AppendJSONStringContent(dst []byte, s string) []byte { return appendJSONStringContent(dst, s) }

// AppendJSONStringContentBytes appends b escaped for JSON string content without allocation.
func AppendJSONStringContentBytes(dst []byte, b []byte) []byte {
	return appendJSONStringContentBytes(dst, b)
}

func appendJSONStringContentBytes(dst []byte, b []byte) []byte {
	start := 0
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < 0x20 || c == '\\' || c == '"' {
			dst = append(dst, b[start:i]...)
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
	return append(dst, b[start:]...)
}

func appendJSONStringContent(dst []byte, s string) []byte {
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
	return append(dst, s[start:]...)
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
	if c.canDirectWrite200() {
		return c.writeDirectJSONAppender200(app)
	}
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

func (c *DefaultCtx) writeDirectJSONAppender200(app JSONAppender) error {
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
		c.writeBufPooled = true
	}
	base := (*c.writeBuf)[:0]
	if cap(base) < directJSONHeaderReserve {
		base = append(base, make([]byte, directJSONHeaderReserve)...)
	} else {
		base = base[:directJSONHeaderReserve]
	}
	body := base[directJSONHeaderReserve:directJSONHeaderReserve]
	out, err := app.AppendJSON(body)
	if err != nil {
		*c.writeBuf = base[:0]
		return err
	}
	return c.writeDirectJSONBytes200(base, out)
}

const directJSONHeaderReserve = 192

func (c *DefaultCtx) writeDirectJSONBytes200(base, out []byte) error {
	c.responded = true
	if len(out) < directHeaderCacheSize && (len(out) == 0 || &out[0] == &base[:directJSONHeaderReserve+1][directJSONHeaderReserve]) && !c.server.cfg.SendDateHeader &&
		!c.server.cfg.SendKeepAliveHeader && c.Header.KeepAlive && !c.forceClose {
		header := directJSON200Headers[len(out)]
		start := directJSONHeaderReserve - len(header)
		copy(base[start:directJSONHeaderReserve], header)
		full := base[:directJSONHeaderReserve+len(out)]
		*c.writeBuf = full
		return writeAll(c.conn, full[start:])
	}
	// Assemble the header in the unused prefix of the connection-owned response
	// buffer. A local [256]byte header escaped to the heap because the large-body
	// branch passed it to net.Buffers, adding one allocation to every small JSON
	// response. The body starts after directJSONHeaderReserve, so this prefix is
	// safe to use until the final backfill below.
	header := base[:0]
	header = append(header, "HTTP/1.1 200 OK\r\n"...)
	if c.server.cfg.SendDateHeader {
		header = append(header, cachedDate()...)
	}
	if c.contentType != nil {
		header = append(header, "Content-Type: "...)
		header = append(header, c.contentType...)
		header = append(header, '\r', '\n')
	}
	header = c.appendSecurityHeaders(header)
	header = appendContentLengthLine(header, len(out))
	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			header = append(header, "Connection: keep-alive\r\n\r\n"...)
		} else {
			header = append(header, "\r\n"...)
		}
	} else {
		header = append(header, "Connection: close\r\n\r\n"...)
	}
	if len(out) > 0 && &out[0] != &base[:directJSONHeaderReserve+1][directJSONHeaderReserve] {
		// An unusually large appender outgrew the pooled buffer. Preserve the
		// zero-copy large-body behavior when the body moved to a new allocation.
		// Do not retain out: a custom JSONAppender may legally return caller-owned
		// storage, which must never enter fh's response pool or connection buffer.
		*c.writeBuf = base[:0]
		return writeBuffers(c.conn, header, out)
	}
	// The direct JSON encoders write the body after reserved headroom. Backfill
	// the now-known header and send one contiguous buffer without moving the body.
	start := directJSONHeaderReserve - len(header)
	copy(base[start:directJSONHeaderReserve], header)
	full := base[:directJSONHeaderReserve+len(out)]
	*c.writeBuf = full
	return writeAll(c.conn, full[start:])
}

// writeResponse writes a response with a byte body.
func (c *DefaultCtx) writeResponse(body []byte) error {
	if c.captureResponseBody {
		c.responseBody = append(c.responseBody[:0], body...)
	} else {
		c.responseBody = c.responseBody[:0]
	}
	if c.canDirectWrite200() {
		return c.writeDirect200Bytes(body)
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
	if (c.flags & ctxFlagAutoETag) != 0 {
		var is304 bool
		body, is304 = evaluateAutoETag(c, body)
		if is304 {
			c.status = 304
		}
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
		c.writeBufPooled = true
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

	buf = c.appendSecurityHeaders(buf)

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
		buf = appendContentLengthLine(buf, len(body))
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
	if bodyAllowed && c.flags&ctxFlagHEAD == 0 {
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

func (c *DefaultCtx) canDirectWrite200() bool {
	return !c.responded && c.status == StatusOK && c.flags == 0
}

func (c *DefaultCtx) writeDirect200String(s string) error {
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
		c.writeBufPooled = true
	}
	buf := (*c.writeBuf)[:0]
	if len(s) < directHeaderCacheSize && !c.server.cfg.SendDateHeader &&
		!c.server.cfg.SendKeepAliveHeader && c.Header.KeepAlive && !c.forceClose &&
		len(c.contentType) == len(plainTextCT) && len(c.contentType) > 0 && &c.contentType[0] == &plainTextCT[0] {
		buf = append(buf, directPlain200Headers[len(s)]...)
		buf = append(buf, s...)
		*c.writeBuf = buf
		return writeAll(c.conn, buf)
	}
	buf = append(buf, "HTTP/1.1 200 OK\r\n"...)
	if c.server.cfg.SendDateHeader {
		buf = append(buf, cachedDate()...)
	}
	if c.contentType != nil {
		buf = append(buf, "Content-Type: "...)
		buf = append(buf, c.contentType...)
		buf = append(buf, '\r', '\n')
	}
	buf = c.appendSecurityHeaders(buf)
	buf = appendContentLengthLine(buf, len(s))
	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			buf = append(buf, "Connection: keep-alive\r\n\r\n"...)
		} else {
			buf = append(buf, "\r\n"...)
		}
	} else {
		buf = append(buf, "Connection: close\r\n\r\n"...)
	}
	buf = append(buf, s...)
	*c.writeBuf = buf
	return writeAll(c.conn, buf)
}

func (c *DefaultCtx) writeDirect200Bytes(body []byte) error {
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
		c.writeBufPooled = true
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
	buf = c.appendSecurityHeaders(buf)
	buf = appendContentLengthLine(buf, len(body))
	if c.Header.KeepAlive && !c.forceClose {
		if c.server.cfg.SendKeepAliveHeader {
			buf = append(buf, "Connection: keep-alive\r\n\r\n"...)
		} else {
			buf = append(buf, "\r\n"...)
		}
	} else {
		buf = append(buf, "Connection: close\r\n\r\n"...)
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

// appendSecurityHeaders appends the Server response header when configured.
// Security headers (X-Content-Type-Options, X-Frame-Options, Referrer-Policy)
// are handled by defaultHardeningMiddleware in production modes.
func (c *DefaultCtx) appendSecurityHeaders(buf []byte) []byte {
	if c.server == nil {
		return buf
	}
	if c.server.cfg.ServerHeader != "" {
		buf = append(buf, "Server: "...)
		buf = append(buf, c.server.cfg.ServerHeader...)
		buf = append(buf, '\r', '\n')
	}
	return buf
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
			func() {
				defer func() { recover() }()
				refreshDateCache()
			}()
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

func (c *DefaultCtx) Bind(v any) error                     { return Bind(c, v) }
func (c *DefaultCtx) BindJSON(v any) error                 { return BindJSON(c, v) }
func (c *DefaultCtx) BindQuery(v any) error                { return BindQuery(c, v) }
func (c *DefaultCtx) BindForm(v any) error                 { return BindForm(c, v) }
func (c *DefaultCtx) BindHeader(v any) error               { return BindHeader(c, v) }
func (c *DefaultCtx) SSEvent(event string, data any) error { return SSEvent(c, event, data) }
func (c *DefaultCtx) ProblemDetails(status int, title, detail, typeURI string) error {
	return ProblemDetails(c, status, title, detail, typeURI)
}

// ── Fiber-compatible convenience methods ─────────────────────────────────────

// QueryBool returns the query parameter as a bool.
// "true", "1", "yes" (case-insensitive) → true; everything else → false.
func (c *DefaultCtx) QueryBool(key string, def ...bool) bool {
	s := c.Query(key)
	if s == "" {
		if len(def) > 0 {
			return def[0]
		}
		return false
	}
	switch strings.ToLower(s) {
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
}

// QueryInt returns the query parameter parsed as int, or the default value.
func (c *DefaultCtx) QueryInt(key string, def ...int) int {
	s := c.Query(key)
	if s == "" {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return v
}

// QueryFloat returns the query parameter parsed as float64, or the default value.
func (c *DefaultCtx) QueryFloat(key string, def ...float64) float64 {
	s := c.Query(key)
	if s == "" {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if len(def) > 0 {
			return def[0]
		}
		return 0
	}
	return v
}

// ParamsInt returns the named route parameter parsed as int, or 0 on error.
func (c *DefaultCtx) ParamsInt(key string) (int, error) {
	return strconv.Atoi(c.Params(key))
}

// AllParams returns all named route parameters as a map.
func (c *DefaultCtx) AllParams() map[string]string {
	m := make(map[string]string, len(c.params))
	for _, p := range c.params {
		m[p.Key] = p.Value
	}
	return m
}

// CookieParser binds cookie values into a struct using the "cookie" tag.
func (c *DefaultCtx) CookieParser(v any) error {
	return BindCookie(c, v)
}

// ParamsParser binds named route parameters into a struct using the "params" tag.
func (c *DefaultCtx) ParamsParser(v any) error {
	return BindParams(c, v)
}

// JSONP sends a JSONP response wrapped in the given callback function.
// If no callback is provided, defaults to "callback".
func (c *DefaultCtx) JSONP(data any, callback ...string) error {
	cb := "callback"
	if len(callback) > 0 && callback[0] != "" {
		cb = callback[0]
	}
	b, err := (jsonCodec{}).Marshal(data)
	if err != nil {
		return err
	}
	c.Set(HeaderContentType, MIMEApplicationJavaScript)
	buf := getBytes()
	p := *buf
	p = append(p, cb...)
	p = append(p, '(')
	p = append(p, b...)
	p = append(p, ");"...)
	err = c.Send(p)
	*buf = p
	putBytes(buf)
	return err
}

// Links joins the given URIs into a Link response header field (RFC 8288).
func (c *DefaultCtx) Links(link ...string) {
	n := len(link)
	if n == 0 {
		return
	}
	var b strings.Builder
	for i, l := range link {
		if i%2 == 0 {
			b.WriteByte('<')
			b.WriteString(l)
			b.WriteByte('>')
		} else {
			b.WriteString("; rel=\"")
			b.WriteString(l)
			b.WriteByte('"')
		}
		if i < n-1 {
			b.WriteString(", ")
		}
	}
	c.Set("Link", b.String())
}

// Location sets the Location response header to the given path.
func (c *DefaultCtx) Location(path string) {
	c.Set("Location", path)
}

// Fresh checks whether the request is fresh based on ETag/If-None-Match
// and Last-Modified/If-Modified-Since headers.
func (c *DefaultCtx) Fresh() bool {
	status := c.status
	if status >= 200 && status < 300 || status == 304 {
		etag := c.GetRespHeader("ETag")
		if etag != "" {
			match := c.Get("If-None-Match")
			if match != "" && match == etag {
				return true
			}
		}
		lastMod := c.GetRespHeader("Last-Modified")
		if lastMod != "" {
			modSince := c.Get("If-Modified-Since")
			if modSince != "" {
				lastModTime, err1 := http.ParseTime(lastMod)
				modSinceTime, err2 := http.ParseTime(modSince)
				if err1 == nil && err2 == nil && !modSinceTime.Before(lastModTime) {
					return true
				}
			}
		}
	}
	return false
}

// Secure returns true when the connection uses TLS.
func (c *DefaultCtx) Secure() bool {
	if c.conn == nil {
		return false
	}
	_, ok := c.conn.(*tls.Conn)
	return ok
}

// IsFromLocal returns true when the request originates from a loopback address.
func (c *DefaultCtx) IsFromLocal() bool {
	ip := c.IP()
	if ip == "" {
		return false
	}
	parsed := net.ParseIP(ip)
	return parsed != nil && parsed.IsLoopback()
}

// ── Modern HTTP server helpers ──────────────────────────────────────────────

// Vary appends field names to the Vary response header without duplicating
// existing tokens. It is safe to call multiple times.
func (c *DefaultCtx) Vary(fields ...string) {
	for _, f := range fields {
		if f != "" {
			c.Append(HeaderVary, f)
		}
	}
}

// IsXHR returns true when the request carries X-Requested-With: XMLHttpRequest.
func (c *DefaultCtx) IsXHR() bool {
	return strings.EqualFold(c.Get(HeaderXRequestedWith), "XMLHttpRequest")
}

// Protocol returns "https" when the connection uses TLS, otherwise "http".
func (c *DefaultCtx) Protocol() string {
	if c.Secure() {
		return "https"
	}
	return "http"
}

// Subdomains returns the subdomain segments of the Host header, ordered from
// leftmost (closest to the label boundary) to rightmost.
// offset (default 2) controls how many right-hand labels (e.g. "example.com")
// are skipped. For Host=api.v2.example.com with offset=2 → ["api", "v2"].
func (c *DefaultCtx) Subdomains(offset ...int) []string {
	off := 2
	if len(offset) > 0 && offset[0] > 0 {
		off = offset[0]
	}
	host := c.Hostname()
	parts := strings.Split(host, ".")
	if len(parts) <= off {
		return nil
	}
	return parts[:len(parts)-off]
}

// BaseURL returns scheme://host with no trailing slash.
func (c *DefaultCtx) BaseURL() string {
	return c.Protocol() + "://" + string(c.Header.Host)
}

// Accepts returns the best MIME type match from the client's Accept header.
// When the header is absent or "*/*", the first offered type is returned.
// Returns "" when none of the offered types are acceptable (q=0 or no match).
func (c *DefaultCtx) Accepts(offers ...string) string {
	if len(offers) == 0 {
		return ""
	}
	return ctxNegotiateBest(c.Get(HeaderAccept), offers)
}

// AcceptsCharsets selects the best offered charset from Accept-Charset.
func (c *DefaultCtx) AcceptsCharsets(offers ...string) string {
	if len(offers) == 0 {
		return ""
	}
	return ctxNegotiateBest(c.Get(HeaderAcceptCharset), offers)
}

// AcceptsEncodings selects the best offered encoding from Accept-Encoding.
func (c *DefaultCtx) AcceptsEncodings(offers ...string) string {
	if len(offers) == 0 {
		return ""
	}
	return ctxNegotiateBest(c.Get(HeaderAcceptEncoding), offers)
}

// AcceptsLanguages selects the best offered language from Accept-Language.
func (c *DefaultCtx) AcceptsLanguages(offers ...string) string {
	if len(offers) == 0 {
		return ""
	}
	return ctxNegotiateBest(c.Get(HeaderAcceptLanguage), offers)
}

// ctxNegotiateBest picks the highest-quality offered value from an Accept-* header.
// Falls back to the first offered value when the header is absent or "*".
func ctxNegotiateBest(header string, offers []string) string {
	if header == "" || header == "*" || header == "*/*" {
		return offers[0]
	}
	best := ""
	bestQ := -1.0
	for _, offer := range offers {
		q := ctxAcceptFieldQuality(header, offer)
		if q > bestQ {
			bestQ = q
			best = offer
		}
	}
	if bestQ <= 0 {
		return ""
	}
	return best
}

// ctxAcceptFieldQuality returns the effective quality value for offer in an Accept-* header.
func ctxAcceptFieldQuality(header, offer string) float64 {
	for len(header) > 0 {
		token := header
		if idx := strings.IndexByte(header, ','); idx >= 0 {
			token = header[:idx]
			header = header[idx+1:]
		} else {
			header = ""
		}
		token = strings.TrimSpace(token)
		q := 1.0
		if semi := strings.IndexByte(token, ';'); semi >= 0 {
			params := strings.TrimSpace(token[semi+1:])
			token = strings.TrimSpace(token[:semi])
			if strings.HasPrefix(params, "q=") {
				if v, err := strconv.ParseFloat(params[2:], 64); err == nil {
					q = v
				}
			}
		}
		if strings.EqualFold(token, offer) || token == "*" || token == "*/*" {
			return q
		}
		// wildcard subtype match: "text/*" accepts "text/html"
		if strings.HasSuffix(token, "/*") {
			prefix := token[:len(token)-1] // "text/"
			if strings.HasPrefix(strings.ToLower(offer), strings.ToLower(prefix)) {
				return q
			}
		}
	}
	return 0
}

// XML encodes v to XML and sends it with Content-Type: application/xml; charset=utf-8.
func (c *DefaultCtx) XML(v any) error {
	b, err := xml.Marshal(v)
	if err != nil {
		return err
	}
	c.contentType = []byte(MIMEApplicationXMLCharsetUTF8)
	return c.writeResponse(b)
}

// Format performs content negotiation from the Accept header and dispatches to
// the matching handler. It sets Content-Type to the matched key before calling
// the handler. Falls through to Next when no handler matches or offers is empty.
func (c *DefaultCtx) Format(handlers map[string]HandlerFunc) error {
	if len(handlers) == 0 {
		return c.Next()
	}
	offers := make([]string, 0, len(handlers))
	for k := range handlers {
		offers = append(offers, k)
	}
	best := c.Accepts(offers...)
	if best == "" {
		return c.Next()
	}
	if h, ok := handlers[best]; ok {
		c.Type(best)
		return h(c)
	}
	return c.Next()
}

// Range parses the Range request header for a resource of the given total size.
// Returns nil, nil when no Range header is present (serve the full resource).
// Returns a 416 error when the header is present but unsatisfiable.
func (c *DefaultCtx) Range(size int64) ([]ByteRange, error) {
	header := c.Get(HeaderRange)
	if header == "" {
		return nil, nil
	}
	return parseRangeHeader(header, size)
}

// parseRangeHeader parses an RFC 9110 Range: bytes=... header value.
func parseRangeHeader(header string, size int64) ([]ByteRange, error) {
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "unsupported range unit")
	}
	spec := header[len(prefix):]
	var ranges []ByteRange
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		dash := strings.IndexByte(part, '-')
		if dash < 0 {
			return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "invalid range format")
		}
		startStr := strings.TrimSpace(part[:dash])
		endStr := strings.TrimSpace(part[dash+1:])
		var start, end int64
		if startStr == "" {
			// suffix-range: -N means last N bytes
			n, err := strconv.ParseInt(endStr, 10, 64)
			if err != nil || n < 0 {
				return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "invalid suffix range")
			}
			start = size - n
			if start < 0 {
				start = 0
			}
			end = size - 1
		} else {
			var err error
			start, err = strconv.ParseInt(startStr, 10, 64)
			if err != nil || start < 0 {
				return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "invalid range start")
			}
			if endStr == "" {
				end = size - 1
			} else {
				end, err = strconv.ParseInt(endStr, 10, 64)
				if err != nil || end < start {
					return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "invalid range end")
				}
			}
		}
		if start >= size {
			return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "range start beyond resource size")
		}
		if end >= size {
			end = size - 1
		}
		ranges = append(ranges, ByteRange{Start: start, End: end})
	}
	if len(ranges) == 0 {
		return nil, NewHTTPError(StatusRangeNotSatisfiable, "RANGE_NOT_SATISFIABLE", "empty range specification")
	}
	return ranges, nil
}

// ClearCookie expires cookies by name. With no arguments, all staged response
// cookies on this context are expired. To clear a browser-stored cookie the
// name (and optionally Path/Domain) must match the original Set-Cookie.
func (c *DefaultCtx) ClearCookie(name ...string) {
	if len(name) == 0 {
		for i := range c.responseCookies {
			c.responseCookies[i].MaxAge = -1
			c.responseCookies[i].Expires = time.Unix(0, 0)
		}
		return
	}
	for _, n := range name {
		c.SetCookie(&Cookie{
			Name:    n,
			Value:   "",
			MaxAge:  -1,
			Expires: time.Unix(0, 0),
		})
	}
}

// QueryMultiple returns all values for a repeated query parameter.
// For /search?tag=go&tag=web it returns ["go", "web"].
func (c *DefaultCtx) QueryMultiple(name string) []string {
	c.parseQuery()
	var out []string
	for i := 0; i < c.qcount; i++ {
		if c.queryParams[i].Key == name {
			out = append(out, c.queryParams[i].Value)
		}
	}
	return out
}

// IPs returns all IP addresses from the X-Forwarded-For header, left to right.
// Returns nil when the header is absent.
func (c *DefaultCtx) IPs() []string {
	raw := c.Get(HeaderXForwardedFor)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if ip := strings.TrimSpace(p); ip != "" {
			out = append(out, ip)
		}
	}
	return out
}

// ClearSiteData sets the Clear-Site-Data response header (W3C Clear Site Data specification).
// Valid directives: "cache", "cookies", "storage", "executionContexts", or "*" (all).
// Directives are automatically quoted according to the standard.
func (c *DefaultCtx) ClearSiteData(directives ...string) {
	if len(directives) == 0 {
		c.Set(HeaderClearSiteData, `*`)
		return
	}
	var b strings.Builder
	for i, d := range directives {
		d = strings.Trim(strings.TrimSpace(d), `"`)
		if d == "" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			b.WriteString(", ")
		}
		if d == "*" {
			b.WriteString(`"*"`)
		} else {
			b.WriteString(`"`)
			b.WriteString(d)
			b.WriteString(`"`)
		}
	}
	if b.Len() > 0 {
		c.Set(HeaderClearSiteData, b.String())
	}
}

// AcceptCH sets the Accept-CH response header for User-Agent Client Hints.
func (c *DefaultCtx) AcceptCH(hints ...string) {
	for _, h := range hints {
		if h != "" {
			c.Append("Accept-CH", h)
		}
	}
}

// CriticalCH sets the Critical-CH response header and also adds the hints to Accept-CH.
func (c *DefaultCtx) CriticalCH(hints ...string) {
	for _, h := range hints {
		if h != "" {
			c.Append("Critical-CH", h)
			c.Append("Accept-CH", h)
		}
	}
}

// StaleWhileRevalidate appends the stale-while-revalidate directive in seconds to Cache-Control.
func (c *DefaultCtx) StaleWhileRevalidate(d time.Duration) {
	secs := int(d.Seconds())
	if secs <= 0 {
		secs = 1
	}
	c.Append(HeaderCacheControl, fmt.Sprintf("stale-while-revalidate=%d", secs))
}

// SendContinue sends an intermediate HTTP/1.1 100 Continue response before the final response.
func (c *DefaultCtx) SendContinue() error {
	if c.conn == nil || c.h2 != nil {
		return nil
	}
	return writeAll(c.conn, []byte("HTTP/1.1 100 Continue\r\n\r\n"))
}
