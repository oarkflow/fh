package fh

import (
	"bytes"
	"errors"
	"net/textproto"
	"strconv"
	"strings"
)

// Common header names, methods, and protocol strings as []byte.
// Used internally for zero-allocation comparisons. String variants (Str suffix)
// are available for user-facing APIs like ctx.Set().
var (
	strHTTP11     = []byte("HTTP/1.1")
	strHTTP10     = []byte("HTTP/1.0")
	strCRLF       = []byte("\r\n")
	strColonSpace = []byte(": ")
	strHeaderEnd  = []byte("\r\n\r\n")
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
	Method                      []byte
	URI                         []byte
	Proto                       []byte
	Host                        []byte
	ContentType                 []byte
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
	h.Method = nil
	h.URI = nil
	h.Proto = nil
	h.Host = nil
	h.ContentType = nil
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

func (h *RequestHeader) SetCookie(c *Ctx, name, value string) {
	c.Header.headers[0] = Header{Key: []byte("Cookie"), Value: []byte(name + "=" + value)}
	c.Header.hcount = 1
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
	// Find end of request line
	lineEnd := bytes.Index(buf, strCRLF)
	if lineEnd < 0 {
		if len(buf) > maxLineSize {
			return 0, ErrRequestLineTooLarge
		}
		return 0, ErrMalformedRequest
	}
	if lineEnd > maxLineSize {
		return 0, ErrRequestLineTooLarge
	}
	line := buf[:lineEnd]
	i := bytes.IndexByte(line, ' ')
	if i < 0 {
		return 0, ErrMalformedRequest
	}
	h.Method = line[:i]
	if len(h.Method) == 0 || !validToken(h.Method) {
		return 0, ErrMalformedRequest
	}
	line = line[i+1:]

	j := bytes.IndexByte(line, ' ')
	if j < 0 {
		return 0, ErrMalformedRequest
	}
	h.URI = line[:j]
	for _, c := range h.URI {
		if c <= 0x20 || c == 0x7f || c == '#' {
			return 0, ErrMalformedRequest
		}
	}
	validTarget := len(h.URI) > 0 && h.URI[0] == '/'
	if bytesEqualFold(h.Method, MethodOPTIONSBytes) && bytes.Equal(h.URI, []byte("*")) {
		validTarget = true
	}
	if bytesEqualFold(h.Method, MethodCONNECTBytes) && len(h.URI) > 0 && bytes.IndexByte(h.URI, '/') < 0 {
		validTarget = true
	}
	if !validTarget {
		// RFC 9112 §3.2.2: absolute-form (e.g., GET http://example.com/path HTTP/1.1)
		if absIdx := bytes.Index(h.URI, []byte("://")); absIdx > 0 {
			scheme := h.URI[:absIdx]
			ok := len(scheme) > 0
			for _, c := range scheme {
				if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.') {
					ok = false
					break
				}
			}
			if ok && absIdx+3 < len(h.URI) {
				validTarget = true
			}
		}
	}
	if !validTarget {
		return 0, ErrMalformedRequest
	}
	line = line[j+1:]

	h.Proto = line
	if !bytes.Equal(h.Proto, strHTTP11) && !bytes.Equal(h.Proto, strHTTP10) {
		return 0, NewHTTPError(505, "HTTP_VERSION_NOT_SUPPORTED", "HTTP version not supported")
	}
	h.KeepAlive = bytes.Equal(h.Proto, strHTTP11)
	return lineEnd + 2, nil
}

// parseHeaders parses all headers until the blank line.
// All slices point into src — zero allocation.
func parseHeaders(src []byte, h *RequestHeader) (int, error) {
	if cap(h.headers) < maxHeaders {
		h.headers = make([]Header, maxHeaders)
	}
	h.hcount = 0
	seenHost, seenTransferEncoding := false, false
	pos := 0
	for {
		if pos >= len(src) {
			return 0, ErrMalformedRequest
		}
		// RFC 9112 §5.5: reject obsolete line folding
		if src[pos] == ' ' || src[pos] == '\t' {
			return 0, ErrMalformedRequest
		}
		// blank line = end of headers
		if src[pos] == '\r' && pos+1 < len(src) && src[pos+1] == '\n' {
			pos += 2
			break
		}
		if src[pos] == '\n' {
			pos++
			break
		}

		// find colon
		colon := bytes.IndexByte(src[pos:], ':')
		if colon < 0 {
			return 0, ErrMalformedRequest
		}
		key := src[pos : pos+colon]
		if len(key) == 0 || !validToken(key) {
			return 0, ErrMalformedRequest
		}
		pos += colon + 1

		// skip optional space
		for pos < len(src) && (src[pos] == ' ' || src[pos] == '\t') {
			pos++
		}

		// find end of value
		end := bytes.Index(src[pos:], strCRLF)
		var val []byte
		if end < 0 {
			// LF only
			end = bytes.IndexByte(src[pos:], '\n')
			if end < 0 {
				return 0, ErrMalformedRequest
			}
			val = trimRight(src[pos : pos+end])
			pos += end + 1
		} else {
			val = trimRight(src[pos : pos+end])
			pos += end + 2
		}
		for _, c := range val {
			if (c < 0x20 && c != '\t') || c == 0x7f {
				return 0, ErrMalformedRequest
			}
		}

		// store well-known headers directly
		switch {
		case bytesEqualFold(key, HeaderHostBytes):
			if seenHost {
				return 0, ErrMalformedRequest
			}
			seenHost = true
			h.Host = val
		case bytesEqualFold(key, HeaderContentTypeBytes):
			h.ContentType = val
		case bytesEqualFold(key, HeaderContentLengthBytes):
			n, ok := parseContentLength(val)
			if !ok || (h.HasContentLength && n != h.ContentLength) {
				return 0, ErrMalformedRequest
			}
			h.ContentLength, h.HasContentLength = n, true
		case bytesEqualFold(key, HeaderConnectionBytes):
			if hasHeaderToken(val, "close") {
				h.KeepAlive = false
			} else if bytes.Equal(h.Proto, strHTTP10) && hasHeaderToken(val, "keep-alive") {
				h.KeepAlive = true
			}
		case bytesEqualFold(key, HeaderTransferEncodingBytes):
			if seenTransferEncoding {
				return 0, ErrMalformedRequest
			}
			seenTransferEncoding = true
			// RFC 9112 allows stacked transfer codings (e.g. "gzip, chunked")
			// with "chunked" as the final coding.
			chunked, ok := parseTransferCoding(val)
			if !ok {
				return 0, ErrMalformedRequest
			}
			h.Chunked = chunked
			h.UnsupportedTransferEncoding = !chunked
		}

		if h.hcount >= maxHeaders {
			return 0, ErrMalformedRequest
		}
		h.headers[h.hcount] = Header{Key: key, Value: val}
		h.hcount++
	}
	if h.Chunked && h.HasContentLength {
		return 0, ErrMalformedRequest
	}
	if bytes.Equal(h.Proto, strHTTP11) && len(h.Host) == 0 {
		return 0, ErrMalformedRequest
	}
	return pos, nil
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
	last := ""
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
			last = b2s(part)
			start = i + 1
		}
	}
	if last == "" {
		return false, false
	}
	if strEqFold([]byte(last), "chunked") {
		return true, true
	}
	return false, true
}

// parseIntFast parses a decimal integer from bytes without allocation.
func parseIntFast(b []byte) int {
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
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
