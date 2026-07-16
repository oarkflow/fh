package fh

import (
	"bytes"
	"errors"
	"fmt"
	"net/textproto"
	"reflect"
	"strconv"
	"strings"
)

// Common header names, methods, and protocol strings as []byte.
// Used internally for zero-allocation comparisons. String variants (Str suffix)
// are available for user-facing APIs like ctx.Set().
var (
	slashBytes   = []byte("/")
	strHTTP11    = []byte("HTTP/1.1")
	strHTTP10    = []byte("HTTP/1.0")
	strHeaderEnd = []byte("\r\n\r\n")
)

// String variants of header/method constants for user-facing APIs.
const (
	HeaderContentTypeStr             = "Content-Type"
	HeaderContentLengthStr           = "Content-Length"
	HeaderConnectionStr              = "Connection"
	HeaderTransferEncodingStr        = "Transfer-Encoding"
	HeaderHostStr                    = "Host"
	HeaderServerStr                  = "Server"
	HeaderDateStr                    = "Date"
	HeaderCacheControlStr            = "Cache-Control"
	HeaderUserAgentStr               = "User-Agent"
	HeaderAuthorizationStr           = "Authorization"
	HeaderAcceptStr                  = "Accept"
	HeaderAcceptEncodingStr          = "Accept-Encoding"
	HeaderAcceptLanguageStr          = "Accept-Language"
	HeaderContentEncodingStr         = "Content-Encoding"
	HeaderContentDispositionStr      = "Content-Disposition"
	HeaderLocationStr                = "Location"
	HeaderSetCookieStr               = "Set-Cookie"
	HeaderCookieStr                  = "Cookie"
	HeaderETagStr                    = "ETag"
	HeaderLastModifiedStr            = "Last-Modified"
	HeaderIfNoneMatchStr             = "If-None-Match"
	HeaderIfModifiedSinceStr         = "If-Modified-Since"
	HeaderIfRangeStr                 = "If-Range"
	HeaderRangeStr                   = "Range"
	HeaderContentRangeStr            = "Content-Range"
	HeaderAcceptRangesStr            = "Accept-Ranges"
	HeaderVaryStr                    = "Vary"
	HeaderAllowStr                   = "Allow"
	HeaderWWWAuthenticateStr         = "WWW-Authenticate"
	HeaderUpgradeStr                 = "Upgrade"
	HeaderOriginStr                  = "Origin"
	HeaderRefererStr                 = "Referer"
	HeaderXRequestedWithStr          = "X-Requested-With"
	HeaderStrictTransportSecurityStr = "Strict-Transport-Security"
	HeaderXContentTypeOptionsStr     = "X-Content-Type-Options"
	HeaderXFrameOptionsStr           = "X-Frame-Options"
	HeaderXXSSProtectionStr          = "X-XSS-Protection"
	HeaderContentSecurityPolicyStr   = "Content-Security-Policy"
	HeaderReferrerPolicyStr          = "Referrer-Policy"
	HeaderPermissionsPolicyStr       = "Permissions-Policy"
	HeaderTrailerStr                 = "Trailer"
	HeaderExpectStr                  = "Expect"
	HeaderHTTP2SettingsStr           = "HTTP2-Settings"

	MethodGETStr     = "GET"
	MethodPOSTStr    = "POST"
	MethodPUTStr     = "PUT"
	MethodDELETEStr  = "DELETE"
	MethodPATCHStr   = "PATCH"
	MethodHEADStr    = "HEAD"
	MethodCONNECTStr = "CONNECT"
	MethodOPTIONSStr = "OPTIONS"
	MethodTRACEStr   = "TRACE"
	MethodQUERYStr   = "QUERY"
)

// ErrMalformedRequest is returned when the request cannot be parsed.
var ErrMalformedRequest = errors.New("malformed HTTP request")

// ErrRequestLineTooLarge is returned when the request line exceeds the limit.
var ErrRequestLineTooLarge = errors.New("request line too large")

// Header is a single HTTP header key/value.
// Both slices point into the read buffer — zero copy.
type Header struct {
	Key   []byte
	Value []byte
}

// RequestHeader holds parsed request metadata. All fields are slices
// into the underlying read buffer — no allocations during parse.
type RequestHeader struct {
	Method []byte
	URI    []byte
	// RequestTarget is the exact target from the request line. URI is kept as
	// the compatibility alias used by older handlers.
	RequestTarget []byte
	Path          []byte
	QueryString   []byte
	Proto         []byte
	Host          []byte
	// targetAuthority is set only for an absolute-form request target. When a
	// Host field is also present it must identify this same authority.
	targetAuthority             []byte
	ContentType                 []byte
	Expect                      []byte
	Upgrade                     []byte
	HTTP2Settings               []byte
	ContentLength               int
	HasContentLength            bool
	KeepAlive                   bool
	Chunked                     bool
	UnsupportedTransferEncoding bool

	headers []Header // raw headers, capped at maxHeaders
	hcount  int
}

const maxHeaders = 64

func (h *RequestHeader) reset() {
	if h.hcount > 0 {
		clear(h.headers[:h.hcount])
	}
	h.resetRetained()
}

// resetRetained resets the logical request state without clearing the bounded
// header slots. ModeFast HTTP/1 contexts live for exactly one connection, so
// retaining references into that connection's read buffer is safe and avoids
// a memclr on every keep-alive request. Pooled and production contexts continue
// to use reset so they never retain another request's buffer.
func (h *RequestHeader) resetRetained() {
	h.Method = nil
	h.URI = nil
	h.RequestTarget = nil
	h.Path = nil
	h.QueryString = nil
	h.Proto = nil
	h.Host = nil
	h.targetAuthority = nil
	h.ContentType = nil
	h.Expect = nil
	h.Upgrade = nil
	h.HTTP2Settings = nil
	h.ContentLength = 0
	h.HasContentLength = false
	h.KeepAlive = false
	h.Chunked = false
	h.UnsupportedTransferEncoding = false
	h.hcount = 0
}

func (h *RequestHeader) Init() {
	h.headers = make([]Header, maxHeaders)
	h.reset()
}

func (h *RequestHeader) SetCookie(c *DefaultCtx, name, value string) {
	cookieVal := name + "=" + value
	for i := 0; i < h.hcount; i++ {
		if bytesEqualFold(h.headers[i].Key, HeaderCookieBytes) {
			h.headers[i].Value = append(append([]byte(nil), h.headers[i].Value...), ';', ' ')
			h.headers[i].Value = append(h.headers[i].Value, []byte(cookieVal)...)
			return
		}
	}
	if h.hcount < len(h.headers) {
		h.headers[h.hcount] = Header{Key: HeaderCookieBytes, Value: []byte(cookieVal)}
		h.hcount++
	} else {
		h.headers = append(h.headers, Header{Key: HeaderCookieBytes, Value: []byte(cookieVal)})
		h.hcount++
	}
}

// Peek returns the value of a header by name (case-insensitive).
// Returns nil if not found. Zero allocation.
func (h *RequestHeader) Peek(name []byte) []byte {
	for i := 0; i < h.hcount; i++ {
		if bytesEqualFold(h.headers[i].Key, name) {
			return h.headers[i].Value
		}
	}
	if bytesEqualFold(name, HeaderHostBytes) {
		return h.Host
	}
	return nil
}

// PeekStr returns header value as string (allocates once for the string return).
func (h *RequestHeader) PeekStr(name string) string {
	v := h.peekStr(name)
	if v == nil {
		return ""
	}
	return string(v)
}

// Get is the string-based, Fiber-style request header accessor.
func (h *RequestHeader) Get(name string, defaults ...string) string {
	value := h.PeekStr(name)
	if value == "" && len(defaults) != 0 {
		return defaults[0]
	}
	return value
}

// Values returns every value stored for a request header.
func (h *RequestHeader) Values(name string) []string {
	values := make([]string, 0, 1)
	for i := 0; i < h.hcount; i++ {
		if strBytesEqualFold(name, h.headers[i].Key) {
			values = append(values, string(h.headers[i].Value))
		}
	}
	return values
}

// GetHeaders returns all request headers and preserves repeated fields.
func (h *RequestHeader) GetHeaders() map[string][]string {
	headers := make(map[string][]string, h.hcount)
	for i := 0; i < h.hcount; i++ {
		key := textproto.CanonicalMIMEHeaderKey(string(h.headers[i].Key))
		headers[key] = append(headers[key], string(h.headers[i].Value))
	}
	if len(h.Host) != 0 && len(headers[HeaderHostStr]) == 0 {
		headers[HeaderHostStr] = []string{string(h.Host)}
	}
	if len(h.ContentType) != 0 && len(headers[HeaderContentTypeStr]) == 0 {
		headers[HeaderContentTypeStr] = []string{string(h.ContentType)}
	}
	return headers
}

// Set replaces all existing values for name with value.
func (h *RequestHeader) Set(name, value string) {
	h.Del(name)
	h.Add(name, value)
}

// Add appends a request header value without replacing existing values.
func (h *RequestHeader) Add(name, value string) {
	key := []byte(name)
	if !validToken(key) || stringsContainsCTL(value) {
		return
	}
	if h.hcount >= maxHeaders {
		return
	}
	if h.hcount == len(h.headers) {
		h.headers = append(h.headers, Header{})
	}
	h.headers[h.hcount] = Header{Key: key, Value: []byte(value)}
	h.hcount++
	h.syncKnownHeader(name, value)
}

// Del removes every value for a request header.
func (h *RequestHeader) Del(name string) {
	write := 0
	for i := 0; i < h.hcount; i++ {
		if strBytesEqualFold(name, h.headers[i].Key) {
			continue
		}
		h.headers[write] = h.headers[i]
		write++
	}
	clear(h.headers[write:h.hcount])
	h.hcount = write
	switch {
	case strings.EqualFold(name, HeaderHostStr):
		h.Host = nil
	case strings.EqualFold(name, HeaderContentTypeStr):
		h.ContentType = nil
	case strings.EqualFold(name, HeaderContentLengthStr):
		h.ContentLength, h.HasContentLength = 0, false
	}
}

func (h *RequestHeader) syncKnownHeader(name, value string) {
	switch {
	case strings.EqualFold(name, HeaderHostStr):
		h.Host = []byte(value)
	case strings.EqualFold(name, HeaderContentTypeStr):
		h.ContentType = []byte(value)
	case strings.EqualFold(name, HeaderContentLengthStr):
		if n, err := strconv.Atoi(value); err == nil && n >= 0 {
			h.ContentLength, h.HasContentLength = n, true
		}
	}
}

func stringsContainsCTL(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] == '\r' || value[i] == '\n' || value[i] == 0 {
			return true
		}
	}
	return false
}

func (h *RequestHeader) peekStr(name string) []byte {
	for i := 0; i < h.hcount; i++ {
		if strBytesEqualFold(name, h.headers[i].Key) {
			return h.headers[i].Value
		}
	}
	if len(name) == 4 && (name[0]|0x20 == 'h') && (name[1]|0x20 == 'o') && (name[2]|0x20 == 's') && (name[3]|0x20 == 't') {
		return h.Host
	}
	return nil
}

func strBytesEqualFold(a string, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca == cb {
			continue
		}
		if ca >= 'A' && ca <= 'Z' {
			ca |= 0x20
		}
		if cb >= 'A' && cb <= 'Z' {
			cb |= 0x20
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// parseRequestLine parses "METHOD URI HTTP/1.x\r\n".
// Returns bytes consumed or error.
func parseRequestLine(buf []byte, h *RequestHeader, maxLineSize int) (int, error) {
	// Manual request-line parser. It avoids three bytes.Index calls and validates
	// the target while scanning once. This is on every request, including the
	// benchmark hot path.
	limit := len(buf)
	if maxLineSize > 0 && limit > maxLineSize+2 {
		limit = maxLineSize + 2
	}
	// bytes.IndexByte uses vectorized runtime routines; a bare LF (not preceded
	// by CR) is rejected as malformed rather than skipped, which every later
	// component validation would reject anyway.
	nl := bytes.IndexByte(buf[:limit], '\n')
	if nl < 0 {
		if maxLineSize > 0 && len(buf) > maxLineSize {
			return 0, ErrRequestLineTooLarge
		}
		return 0, ErrMalformedRequest
	}
	if nl == 0 || buf[nl-1] != '\r' {
		return 0, ErrMalformedRequest
	}
	lineEnd := nl - 1
	if maxLineSize > 0 && lineEnd > maxLineSize {
		return 0, ErrRequestLineTooLarge
	}
	line := buf[:lineEnd]
	sp1 := bytes.IndexByte(line, ' ')
	if sp1 <= 0 {
		return 0, ErrMalformedRequest
	}
	method := line[:sp1]
	if !validToken(method) {
		return 0, ErrMalformedRequest
	}
	sp2 := -1
	if rel := bytes.IndexByte(line[sp1+1:], ' '); rel >= 0 {
		sp2 = sp1 + 1 + rel
	}
	if sp2 <= sp1+1 || sp2 == len(line)-1 {
		return 0, ErrMalformedRequest
	}
	target := line[sp1+1 : sp2]
	proto := line[sp2+1:]
	if !bytes.Equal(proto, strHTTP11) && !bytes.Equal(proto, strHTTP10) {
		return 0, NewHTTPError(505, "HTTP_VERSION_NOT_SUPPORTED", "HTTP version not supported")
	}
	routeTarget := target
	sameRouteTarget := true
	validTarget := target[0] == '/'
	if !validTarget {
		if methodIs(method, 'O', 'P', 'T', 'I', 'O', 'N', 'S') && len(target) == 1 && target[0] == '*' {
			validTarget = true
		} else if methodIs(method, 'C', 'O', 'N', 'N', 'E', 'C', 'T') && bytes.IndexByte(target, '/') < 0 {
			// CONNECT uses authority-form. Keep it as the request target; routing to
			// CONNECT handlers remains explicit instead of silently tunnelling.
			validTarget = true
			h.targetAuthority = target
			h.Host = target
		} else if absIdx := bytes.Index(target, []byte("://")); absIdx > 0 && absIdx+3 < len(target) {
			validTarget = true
			for _, c := range target[:absIdx] {
				if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.') {
					validTarget = false
					break
				}
			}
			if validTarget {
				sameRouteTarget = false
				authStart := absIdx + 3
				pathStart := authStart
				for pathStart < len(target) && target[pathStart] != '/' && target[pathStart] != '?' {
					pathStart++
				}
				if pathStart < len(target) && target[pathStart] == '/' {
					routeTarget = target[pathStart:]
				} else if pathStart < len(target) && target[pathStart] == '?' {
					routeTarget = target[pathStart:pathStart]
				} else {
					routeTarget = target[len(target):]
				}
				if pathStart > authStart {
					h.targetAuthority = target[authStart:pathStart]
					h.Host = h.targetAuthority
				}
			}
		}
	}
	if !validTarget {
		return 0, ErrMalformedRequest
	}
	queryAt := -1
	for i, c := range target {
		if c <= 0x20 || c == 0x7f || c == '#' {
			return 0, ErrMalformedRequest
		}
		if c == '?' && queryAt < 0 {
			queryAt = i
		}
	}
	routeQueryAt := -1
	if sameRouteTarget {
		// Origin-form (the common case): routeTarget is target, so the query
		// position is already known from the validation scan above.
		routeQueryAt = queryAt
	} else {
		for i, c := range routeTarget {
			if c == '?' {
				routeQueryAt = i
				break
			}
		}
	}
	h.Method, h.URI, h.RequestTarget, h.Proto = method, routeTarget, target, proto
	if len(routeTarget) == 0 {
		h.Path = slashBytes
		if queryAt >= 0 && queryAt+1 < len(target) {
			h.QueryString = target[queryAt+1:]
		} else {
			h.QueryString = nil
		}
	} else if routeQueryAt >= 0 {
		h.Path = routeTarget[:routeQueryAt]
		if routeQueryAt+1 < len(routeTarget) {
			h.QueryString = routeTarget[routeQueryAt+1:]
		} else {
			h.QueryString = routeTarget[:0]
		}
	} else {
		h.Path = routeTarget
		h.QueryString = nil
	}
	h.KeepAlive = len(proto) == len(strHTTP11) && bytes.Equal(proto, strHTTP11)
	return lineEnd + 2, nil
}

func methodIs(m []byte, a byte, rest ...byte) bool {
	if len(m) != 1+len(rest) {
		return false
	}
	if m[0] != a {
		return false
	}
	for i, c := range rest {
		if m[i+1] != c {
			return false
		}
	}
	return true
}

// parseHeaders parses all headers until the blank line.
// All slices point into src — zero allocation.
func parseHeaders(src []byte, h *RequestHeader) (int, error) {
	return parseHeadersWithLimit(src, h, maxHeaders)
}

func parseHeadersWithLimit(src []byte, h *RequestHeader, maxCount int) (int, error) {
	return parseHeadersWithLimitStrict(src, h, maxCount, true)
}

func parseHeadersWithLimitStrict(src []byte, h *RequestHeader, maxCount int, strictValueValidation bool) (int, error) {
	_ = strictValueValidation // retained for source compatibility; RFC field-value validation is unconditional.
	if maxCount <= 0 {
		maxCount = maxHeaders
	}
	if cap(h.headers) < maxCount {
		h.headers = make([]Header, maxCount)
	}
	h.hcount = 0
	seenHost, seenTransferEncoding := false, false
	pos := 0
	for {
		if pos >= len(src) {
			return 0, ErrMalformedRequest
		}
		// blank line = end of headers
		if src[pos] == '\r' && pos+1 < len(src) && src[pos+1] == '\n' {
			pos += 2
			break
		}
		// RFC 9112 §5.5: reject obsolete line folding.
		if src[pos] == ' ' || src[pos] == '\t' {
			return 0, ErrMalformedRequest
		}

		// RFC 9112 requires CRLF. Accepting LF-only header lines creates parser
		// differentials with upstream proxies and is a request-smuggling hazard.
		nl := bytes.IndexByte(src[pos:], '\n')
		if nl < 0 {
			return 0, ErrMalformedRequest
		}
		lineStart := pos
		lineEnd := pos + nl
		pos = lineEnd + 1
		if lineEnd == lineStart || src[lineEnd-1] != '\r' {
			return 0, ErrMalformedRequest
		}
		lineEnd--
		line := src[lineStart:lineEnd]
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return 0, ErrMalformedRequest
		}
		key := line[:colon]
		if !validToken(key) {
			return 0, ErrMalformedRequest
		}
		valueStart := colon + 1
		for valueStart < len(line) && (line[valueStart] == ' ' || line[valueStart] == '\t') {
			valueStart++
		}
		val := trimOWSRight(line[valueStart:])
		for _, c := range val {
			if (c < 0x20 && c != '\t') || c == 0x7f {
				return 0, ErrMalformedRequest
			}
		}

		switch knownHeader(key) {
		case knownHost:
			if seenHost {
				return 0, ErrMalformedRequest
			}
			seenHost = true
			if !validHostField(val) {
				return 0, ErrMalformedRequest
			}
			if len(h.targetAuthority) != 0 {
				if !bytes.EqualFold(h.targetAuthority, val) {
					return 0, ErrMalformedRequest
				}
				h.Host = h.targetAuthority
			} else {
				h.Host = val
			}
		case knownContentType:
			h.ContentType = val
		case knownContentLength:
			n, ok := parseContentLength(val)
			if !ok || (h.HasContentLength && n != h.ContentLength) {
				return 0, ErrMalformedRequest
			}
			h.ContentLength, h.HasContentLength = n, true
		case knownConnection:
			applyConnectionHeader(h, val)
		case knownTransferEncoding:
			if seenTransferEncoding {
				return 0, ErrMalformedRequest
			}
			seenTransferEncoding = true
			chunked, ok := parseTransferCoding(val)
			if !ok {
				return 0, ErrMalformedRequest
			}
			h.Chunked = chunked
			h.UnsupportedTransferEncoding = !chunked
		case knownExpect:
			h.Expect = val
		case knownUpgrade:
			h.Upgrade = val
		case knownHTTP2Settings:
			h.HTTP2Settings = val
		}

		if h.hcount >= maxCount {
			return 0, ErrMalformedRequest
		}
		h.headers[h.hcount] = Header{Key: key, Value: val}
		h.hcount++
	}
	if h.Chunked && h.HasContentLength {
		return 0, ErrMalformedRequest
	}
	if bytes.Equal(h.Proto, strHTTP11) && !seenHost {
		return 0, ErrMalformedRequest
	}
	return pos, nil
}

func validHostField(host []byte) bool {
	if len(host) == 0 || len(host) > 255 {
		return false
	}
	for _, c := range host {
		switch c {
		case ' ', '\t', ',', '/', '\\', '@', '#', '?':
			return false
		}
		if c < 0x21 || c == 0x7f {
			return false
		}
	}
	if host[0] == '[' {
		close := bytes.IndexByte(host, ']')
		if close <= 1 {
			return false
		}
		rest := host[close+1:]
		return len(rest) == 0 || len(rest) > 1 && rest[0] == ':' && validPort(rest[1:])
	}
	if bytes.ContainsAny(host, "[]") || bytes.Count(host, []byte{':'}) > 1 {
		return false
	}
	if colon := bytes.LastIndexByte(host, ':'); colon >= 0 {
		if colon == 0 || !validPort(host[colon+1:]) {
			return false
		}
	}
	return true
}

func validPort(port []byte) bool {
	if len(port) == 0 || len(port) > 5 {
		return false
	}
	n := 0
	for _, c := range port {
		if c < '0' || c > '9' {
			return false
		}
		n = n*10 + int(c-'0')
	}
	return n <= 65535
}

type knownHeaderKind uint8

const (
	knownNone knownHeaderKind = iota
	knownHost
	knownContentType
	knownContentLength
	knownConnection
	knownTransferEncoding
	knownExpect
	knownUpgrade
	knownHTTP2Settings
)

func knownHeader(k []byte) knownHeaderKind {
	// Lowercase OR comparisons avoid bytesEqualFold calls against every common
	// header. Bench requests usually contain Host, Connection, Content-Type and
	// Content-Length, so this switch removes a lot of branchy byte loops.
	switch len(k) {
	case 4:
		if lower4(k, 'h', 'o', 's', 't') {
			return knownHost
		}
	case 10:
		if lowerN(k, "connection") {
			return knownConnection
		}
	case 12:
		if lowerN(k, "content-type") {
			return knownContentType
		}
	case 6:
		if lowerN(k, "expect") {
			return knownExpect
		}
	case 7:
		if lowerN(k, "upgrade") {
			return knownUpgrade
		}
	case 14:
		if lowerN(k, "content-length") {
			return knownContentLength
		}
		if lowerN(k, "http2-settings") {
			return knownHTTP2Settings
		}
	case 17:
		if lowerN(k, "transfer-encoding") {
			return knownTransferEncoding
		}
	}
	return knownNone
}

func applyConnectionHeader(h *RequestHeader, val []byte) {
	// The benchmark and the common browser/client hot path are exact
	// "keep-alive" or "close". Avoid comma-token parsing unless needed.
	switch len(val) {
	case 5:
		if lower5(val, 'c', 'l', 'o', 's', 'e') {
			h.KeepAlive = false
		}
		return
	case 10:
		if lowerN(val, "keep-alive") {
			if bytes.Equal(h.Proto, strHTTP10) {
				h.KeepAlive = true
			}
			return
		}
	}
	if hasHeaderToken(val, "close") {
		h.KeepAlive = false
	} else if bytes.Equal(h.Proto, strHTTP10) && hasHeaderToken(val, "keep-alive") {
		h.KeepAlive = true
	}
}

func lower5(b []byte, a, c, d, e, f byte) bool {
	return (b[0]|0x20) == a && (b[1]|0x20) == c && (b[2]|0x20) == d && (b[3]|0x20) == e && (b[4]|0x20) == f
}

func lower4(b []byte, a, c, d, e byte) bool {
	return (b[0]|0x20) == a && (b[1]|0x20) == c && (b[2]|0x20) == d && (b[3]|0x20) == e
}

func lowerN(b []byte, s string) bool {
	for i := 0; i < len(s); i++ {
		if (b[i] | 0x20) != s[i] {
			return false
		}
	}
	return true
}

func TrimOWS(b []byte) []byte {
	return trimOWS(b)
}

func trimOWS(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	return trimRight(b)
}

func ValidToken(b []byte) bool {
	return validToken(b)
}

func validToken(b []byte) bool {
	for _, c := range b {
		if c <= 0x20 || c >= 0x7f || forbiddenTokenByte(c) {
			return false
		}
	}
	return len(b) > 0
}

func forbiddenTokenByte(c byte) bool {
	switch c {
	case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}':
		return true
	}
	return false
}

// DecodeHeaders populates v from the parsed request headers.
//
// Supported target types:
//   - *map[string]string  — first value per header, canonical key
//   - *map[string][]string — all values per header, canonical key
//   - struct pointer      — each exported field is matched by the header name
//     specified in its `header:"name"` tag (case-insensitive). When the tag
//     is absent the lower-cased field name is used.  Supported field kinds:
//     string, []string, int, int64, uint64, float64, bool, *string.
func DecodeHeaders(h *RequestHeader, v any) error {
	dst, ok := v.(*map[string]string)
	if ok {
		if *dst == nil {
			*dst = make(map[string]string, h.hcount)
		}
		for i := 0; i < h.hcount; i++ {
			key := textproto.CanonicalMIMEHeaderKey(string(h.headers[i].Key))
			(*dst)[key] = string(h.headers[i].Value)
		}
		if len(h.Host) != 0 && (*dst)[HeaderHostStr] == "" {
			(*dst)[HeaderHostStr] = string(h.Host)
		}
		if len(h.ContentType) != 0 && (*dst)[HeaderContentTypeStr] == "" {
			(*dst)[HeaderContentTypeStr] = string(h.ContentType)
		}
		return nil
	}

	dstSlice, ok := v.(*map[string][]string)
	if ok {
		if *dstSlice == nil {
			*dstSlice = make(map[string][]string, h.hcount)
		}
		for i := 0; i < h.hcount; i++ {
			key := textproto.CanonicalMIMEHeaderKey(string(h.headers[i].Key))
			(*dstSlice)[key] = append((*dstSlice)[key], string(h.headers[i].Value))
		}
		if len(h.Host) != 0 && len((*dstSlice)[HeaderHostStr]) == 0 {
			(*dstSlice)[HeaderHostStr] = []string{string(h.Host)}
		}
		if len(h.ContentType) != 0 && len((*dstSlice)[HeaderContentTypeStr]) == 0 {
			(*dstSlice)[HeaderContentTypeStr] = []string{string(h.ContentType)}
		}
		return nil
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("header: unsupported target %T; use *map[string]string, *map[string][]string, or *struct", v)
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("header: unsupported target %T; use *map[string]string, *map[string][]string, or *struct", v)
	}

	// Build lookup of available headers (lower-cased key → first value).
	lookup := make(map[string]string, h.hcount)
	for i := 0; i < h.hcount; i++ {
		lk := strings.ToLower(string(h.headers[i].Key))
		if _, exists := lookup[lk]; !exists {
			lookup[lk] = string(h.headers[i].Value)
		}
	}
	if len(h.Host) != 0 {
		lk := strings.ToLower(HeaderHostStr)
		if _, exists := lookup[lk]; !exists {
			lookup[lk] = string(h.Host)
		}
	}
	if len(h.ContentType) != 0 {
		lk := strings.ToLower(HeaderContentTypeStr)
		if _, exists := lookup[lk]; !exists {
			lookup[lk] = string(h.ContentType)
		}
	}

	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}
		fv := rv.Field(i)
		if !fv.CanSet() {
			continue
		}
		tag := field.Tag.Get("header")
		if tag == "" {
			tag = strings.ToLower(field.Name)
		}
		raw, exists := lookup[strings.ToLower(tag)]
		if !exists {
			continue
		}
		if err := setHeaderField(fv, raw); err != nil {
			return fmt.Errorf("header: field %q: %w", field.Name, err)
		}
	}
	return nil
}

func setHeaderField(fv reflect.Value, raw string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return err
		}
		fv.SetFloat(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case reflect.Slice:
		if fv.Type().Elem().Kind() == reflect.String {
			fv.Set(reflect.ValueOf([]string{raw}))
		}
	case reflect.Ptr:
		if fv.Type().Elem().Kind() == reflect.String {
			fv.Set(reflect.ValueOf(&raw))
		}
	}
	return nil
}

func HasHeaderToken(value []byte, token string) bool {
	return hasHeaderToken(value, token)
}

func hasHeaderToken(value []byte, token string) bool {
	for len(value) > 0 {
		end := bytes.IndexByte(value, ',')
		var part []byte
		if end < 0 {
			part, value = value, nil
		} else {
			part, value = value[:end], value[end+1:]
		}
		for len(part) > 0 && (part[0] == ' ' || part[0] == '\t') {
			part = part[1:]
		}
		part = trimRight(part)
		if strEqFold(part, token) {
			return true
		}
	}
	return false
}

func StrEqFold(b []byte, s string) bool {
	return strEqFold(b, s)
}

func strEqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i, c := range b {
		d := s[i]
		if c >= 'A' && c <= 'Z' {
			c |= 0x20
		}
		if d >= 'A' && d <= 'Z' {
			d |= 0x20
		}
		if c != d {
			return false
		}
	}
	return true
}

func parseContentLength(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	maxInt := int(^uint(0) >> 1)
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		d := int(c - '0')
		if n > (maxInt-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

// trimRight removes trailing whitespace/CR.
// trimOWSRight trims trailing optional whitespace (SP / HTAB per RFC 9110).
// Unlike trimRight it does not trim CR: the header line splitter strips exactly
// one CR before the LF, so any other CR must stay visible to the validators.
func trimOWSRight(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}

func trimRight(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\r' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}

// parseTransferCoding parses a Transfer-Encoding header value per RFC 9112.
// It returns (true, true) if "chunked" is the final coding in the stack,
// (false, true) if the stack is syntactically valid but unsupported,
// and (false, false) if the value is malformed.
func parseTransferCoding(val []byte) (chunked bool, ok bool) {
	count := 0
	soleChunked := false
	start := 0
	for i := 0; i <= len(val); i++ {
		if i == len(val) || val[i] == ',' {
			part := trimOWS(val[start:i])
			if len(part) == 0 {
				return false, false
			}
			for _, c := range part {
				if c <= 0x20 || c >= 0x7f || forbiddenTokenByte(c) {
					return false, false
				}
			}
			count++
			soleChunked = strEqFold(part, "chunked")
			start = i + 1
		}
	}
	if count == 0 {
		return false, false
	}
	// fh currently decodes chunk framing only. Accepting a stacked coding such
	// as gzip, chunked without decoding gzip gives intermediaries a different
	// message interpretation. Repeated chunked is forbidden as well.
	if count == 1 && soleChunked {
		return true, true
	}
	return false, true
}

func BytesEqualFold(a, b []byte) bool {
	return bytesEqualFold(a, b)
}

// bytesEqualFold reports whether a and b are equal under ASCII case folding.
func bytesEqualFold(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca == cb {
			continue
		}
		if ca >= 'A' && ca <= 'Z' {
			ca |= 0x20
		}
		if cb >= 'A' && cb <= 'Z' {
			cb |= 0x20
		}
		if ca != cb {
			return false
		}
	}
	return true
}
