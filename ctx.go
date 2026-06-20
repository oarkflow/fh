package fasthttp

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
)

type Ctx struct {
	conn   net.Conn
	server *App

	Header RequestHeader

	params []Param

	status          int
	customHeaders   [16]Header
	extraHeaders    []Header
	chCount         int
	body            []byte
	contentType     []byte
	responded       bool
	forceClose      bool
	upgraded        bool
	upgradeBuffered []byte
	trailers        []Header
	requestContext  context.Context
	bodyTransform   func([]byte) ([]byte, error)
	h2              *h2Response

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
	c.requestContext = context.Background()
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

func (c *Ctx) Param(name string) string {
	for i := range c.params {
		if c.params[i].Key == name {
			return c.params[i].Value
		}
	}
	return ""
}

func (c *Ctx) Query(name string) string {
	if !c.queryParsed {
		c.parseQuery()
	}
	for i := 0; i < c.qcount; i++ {
		if c.queryParams[i].Key == name {
			return c.queryParams[i].Value
		}
	}
	return ""
}

func (c *Ctx) parseQuery() {
	c.queryParsed = true
	uri := c.Header.URI
	qi := indexByte(uri, '?')
	if qi < 0 {
		return
	}
	qs := uri[qi+1:]
	for len(qs) > 0 {
		var pair []byte
		if i := indexByte(qs, '&'); i >= 0 {
			pair, qs = qs[:i], qs[i+1:]
		} else {
			pair, qs = qs, nil
		}
		key, value := pair, []byte(nil)
		if i := indexByte(pair, '='); i >= 0 {
			key, value = pair[:i], pair[i+1:]
		}
		k, v := urlDecode(key), urlDecode(value)
		if c.qcount < len(c.queryParams) {
			c.queryParams[c.qcount] = Param{Key: k, Value: v}
		} else {
			c.queryParams = append(c.queryParams, Param{Key: k, Value: v})
		}
		c.qcount++
	}
}

func (c *Ctx) Body() []byte { return c.body }

// Trailer returns a decoded chunked request trailer by name.
func (c *Ctx) Trailer(name string) string {
	for i := range c.trailers {
		if bytesEqualFold(c.trailers[i].Key, []byte(name)) {
			return string(c.trailers[i].Value)
		}
	}
	return ""
}

func (c *Ctx) BodyParser(v any) error {
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

func (c *Ctx) Get(name string) string { return c.Header.PeekStr(name) }

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
	addr := c.conn.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return strings.Trim(addr, "[]")
}

// ── Response builders ──────────────────────────────────────────────────────

func (c *Ctx) Status(code int) *Ctx {
	if code < 100 || code > 999 {
		code = 500
	}
	c.status = code
	return c
}

func (c *Ctx) Set(key, value string) {
	k := []byte(key)
	v := []byte(value)
	if !validToken(k) || strings.IndexAny(value, "\x00\r\n") >= 0 {
		return
	}
	if bytesEqualFold(k, headerContentLength) || bytesEqualFold(k, headerTransferEncoding) || bytesEqualFold(k, headerConnection) {
		return
	}
	if bytesEqualFold(k, headerContentType) {
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
	if !validToken([]byte(key)) || strings.IndexAny(value, "\x00\r\n") >= 0 {
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
	if strings.IndexAny(mime, "\x00\r\n") >= 0 {
		return c
	}
	c.contentType = []byte(mime)
	return c
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
	c.responded = true
	if c.writeBuf == nil {
		c.writeBuf = getBytes()
	}
	buf := (*c.writeBuf)[:0]

	// Status line
	buf = appendStatusLine(buf, c.status)

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

	bodyAllowed := responseBodyAllowed(c.status)
	if bodyAllowed {
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

	// Body — append string directly, no []byte conversion
	if bodyAllowed && !bytesEqualFold(c.Header.Method, methodHEAD) {
		buf = append(buf, s...)
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

	bodyAllowed := responseBodyAllowed(c.status)
	if bodyAllowed {
		buf = append(buf, "Content-Length: "...)
		buf = appendInt(buf, len(body))
		buf = append(buf, '\r', '\n')
	}

	if c.Header.KeepAlive && !c.forceClose {
		buf = append(buf, "Connection: keep-alive\r\n"...)
	} else {
		buf = append(buf, "Connection: close\r\n"...)
	}

	buf = append(buf, '\r', '\n')

	// RFC 9110: a HEAD response has the same headers as GET but no content.
	if bodyAllowed && len(body) > 0 && !bytesEqualFold(c.Header.Method, methodHEAD) {
		buf = append(buf, body...)
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
	case 200:
		return append(buf, "HTTP/1.1 200 OK\r\n"...)
	case 201:
		return append(buf, "HTTP/1.1 201 Created\r\n"...)
	case 204:
		return append(buf, "HTTP/1.1 204 No Content\r\n"...)
	case 301:
		return append(buf, "HTTP/1.1 301 Moved Permanently\r\n"...)
	case 302:
		return append(buf, "HTTP/1.1 302 Found\r\n"...)
	case 304:
		return append(buf, "HTTP/1.1 304 Not Modified\r\n"...)
	case 400:
		return append(buf, "HTTP/1.1 400 Bad Request\r\n"...)
	case 401:
		return append(buf, "HTTP/1.1 401 Unauthorized\r\n"...)
	case 403:
		return append(buf, "HTTP/1.1 403 Forbidden\r\n"...)
	case 404:
		return append(buf, "HTTP/1.1 404 Not Found\r\n"...)
	case 405:
		return append(buf, "HTTP/1.1 405 Method Not Allowed\r\n"...)
	case 409:
		return append(buf, "HTTP/1.1 409 Conflict\r\n"...)
	case 422:
		return append(buf, "HTTP/1.1 422 Unprocessable Entity\r\n"...)
	case 429:
		return append(buf, "HTTP/1.1 429 Too Many Requests\r\n"...)
	case 500:
		return append(buf, "HTTP/1.1 500 Internal Server Error\r\n"...)
	case 502:
		return append(buf, "HTTP/1.1 502 Bad Gateway\r\n"...)
	case 503:
		return append(buf, "HTTP/1.1 503 Service Unavailable\r\n"...)
	default:
		buf = append(buf, "HTTP/1.1 "...)
		buf = appendInt(buf, code)
		buf = append(buf, ' ')
		buf = append(buf, statusText(code)...)
		return append(buf, '\r', '\n')
	}
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
	if indexByte(b, '%') < 0 && indexByte(b, '+') < 0 {
		return b2s(b)
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); {
		switch b[i] {
		case '+':
			out = append(out, ' ')
			i++
		case '%':
			if i+2 < len(b) {
				h := unhex(b[i+1])
				l := unhex(b[i+2])
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

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

func statusText(code int) string {
	switch code {
	case 100:
		return "Continue"
	case 101:
		return "Switching Protocols"
	case 102:
		return "Processing"
	case 103:
		return "Early Hints"
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 202:
		return "Accepted"
	case 203:
		return "Non-Authoritative Information"
	case 204:
		return "No Content"
	case 205:
		return "Reset Content"
	case 206:
		return "Partial Content"
	case 207:
		return "Multi-Status"
	case 208:
		return "Already Reported"
	case 226:
		return "IM Used"
	case 300:
		return "Multiple Choices"
	case 301:
		return "Moved Permanently"
	case 302:
		return "Found"
	case 303:
		return "See Other"
	case 304:
		return "Not Modified"
	case 305:
		return "Use Proxy"
	case 307:
		return "Temporary Redirect"
	case 308:
		return "Permanent Redirect"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 402:
		return "Payment Required"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 406:
		return "Not Acceptable"
	case 407:
		return "Proxy Authentication Required"
	case 408:
		return "Request Timeout"
	case 409:
		return "Conflict"
	case 410:
		return "Gone"
	case 411:
		return "Length Required"
	case 412:
		return "Precondition Failed"
	case 413:
		return "Content Too Large"
	case 414:
		return "URI Too Long"
	case 415:
		return "Unsupported Media Type"
	case 416:
		return "Range Not Satisfiable"
	case 417:
		return "Expectation Failed"
	case 418:
		return "I'm a teapot"
	case 421:
		return "Misdirected Request"
	case 422:
		return "Unprocessable Entity"
	case 423:
		return "Locked"
	case 424:
		return "Failed Dependency"
	case 425:
		return "Too Early"
	case 426:
		return "Upgrade Required"
	case 428:
		return "Precondition Required"
	case 429:
		return "Too Many Requests"
	case 431:
		return "Request Header Fields Too Large"
	case 451:
		return "Unavailable For Legal Reasons"
	case 500:
		return "Internal Server Error"
	case 501:
		return "Not Implemented"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	case 504:
		return "Gateway Timeout"
	case 505:
		return "HTTP Version Not Supported"
	case 506:
		return "Variant Also Negotiates"
	case 507:
		return "Insufficient Storage"
	case 508:
		return "Loop Detected"
	case 510:
		return "Not Extended"
	case 511:
		return "Network Authentication Required"
	default:
		return "Unknown"
	}
}

var jsonCT = []byte("application/json")
var headerLocation = []byte("Location")
