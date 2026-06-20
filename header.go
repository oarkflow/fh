package fasthttp

import (
	"bytes"
	"errors"
)

// Common header names as byte slices — avoids repeated string→[]byte conversions.
var (
	HeaderContentType      = []byte("Content-Type")
	HeaderContentLength    = []byte("Content-Length")
	HeaderConnection       = []byte("Connection")
	HeaderTransferEncoding = []byte("Transfer-Encoding")
	HeaderHost             = []byte("Host")

	MethodGET     = []byte("GET")
	MethodPOST    = []byte("POST")
	MethodPUT     = []byte("PUT")
	MethodDELETE  = []byte("DELETE")
	MethodPATCH   = []byte("PATCH")
	MethodHEAD    = []byte("HEAD")
	MethodCONNECT = []byte("CONNECT")
	MethodOPTIONS = []byte("OPTIONS")

	strHTTP11     = []byte("HTTP/1.1")
	strHTTP10     = []byte("HTTP/1.0")
	strCRLF       = []byte("\r\n")
	strColonSpace = []byte(": ")
)

// ErrMalformedRequest is returned when the request cannot be parsed.
var ErrMalformedRequest = errors.New("malformed HTTP request")

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
	if bytesEqualFold(name, HeaderHost) {
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
func parseRequestLine(buf []byte, h *RequestHeader) (int, error) {
	i := bytes.IndexByte(buf, ' ')
	if i < 0 {
		return 0, ErrMalformedRequest
	}
	h.Method = buf[:i]
	if len(h.Method) == 0 || !validToken(h.Method) {
		return 0, ErrMalformedRequest
	}
	buf = buf[i+1:]

	j := bytes.IndexByte(buf, ' ')
	if j < 0 {
		return 0, ErrMalformedRequest
	}
	h.URI = buf[:j]
	for _, c := range h.URI {
		if c <= 0x20 || c == 0x7f || c == '#' {
			return 0, ErrMalformedRequest
		}
	}
	validTarget := len(h.URI) > 0 && h.URI[0] == '/'
	if bytesEqualFold(h.Method, MethodOPTIONS) && bytes.Equal(h.URI, []byte("*")) {
		validTarget = true
	}
	if bytesEqualFold(h.Method, []byte("CONNECT")) && len(h.URI) > 0 && bytes.IndexByte(h.URI, '/') < 0 {
		validTarget = true
	}
	if !validTarget {
		return 0, ErrMalformedRequest
	}
	buf = buf[j+1:]

	k := bytes.Index(buf, strCRLF)
	if k < 0 {
		return 0, ErrMalformedRequest
	}
	h.Proto = buf[:k]
	if !bytes.Equal(h.Proto, strHTTP11) && !bytes.Equal(h.Proto, strHTTP10) {
		return 0, ErrMalformedRequest
	}
	h.KeepAlive = bytes.Equal(h.Proto, strHTTP11)
	return i + 1 + j + 1 + k + 2, nil
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
		case bytesEqualFold(key, HeaderHost):
			if seenHost {
				return 0, ErrMalformedRequest
			}
			seenHost = true
			h.Host = val
		case bytesEqualFold(key, HeaderContentType):
			h.ContentType = val
		case bytesEqualFold(key, HeaderContentLength):
			n, ok := parseContentLength(val)
			if !ok || (h.HasContentLength && n != h.ContentLength) {
				return 0, ErrMalformedRequest
			}
			h.ContentLength, h.HasContentLength = n, true
		case bytesEqualFold(key, HeaderConnection):
			if hasHeaderToken(val, "close") {
				h.KeepAlive = false
			} else if bytes.Equal(h.Proto, strHTTP10) && hasHeaderToken(val, "keep-alive") {
				h.KeepAlive = true
			}
		case bytesEqualFold(key, HeaderTransferEncoding):
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
		end := indexByte(value, ',')
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
