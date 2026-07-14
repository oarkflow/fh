package fh

import (
	"net"
	"strconv"
)

// ResponseConn wraps a net.Conn and provides WriteHeader, Write, and SetHeader
// methods for writing HTTP responses cleanly. It satisfies the net.Conn interface.
type ResponseConn struct {
	net.Conn
	wroteHeader bool
	statusCode  int
	headers     []Header
	prefix      []byte // buffered data from the request
	err         error  // sticky write error
}

// Write writes body data to the connection. If WriteHeader has not been called,
// it sends a 200 OK response first.
func (rc *ResponseConn) Write(p []byte) (int, error) {
	if rc.err != nil {
		return 0, rc.err
	}
	if !rc.wroteHeader {
		rc.WriteHeader(StatusOK)
	}
	if rc.err != nil {
		return 0, rc.err
	}
	n, err := rc.Conn.Write(p)
	if err != nil {
		rc.err = err
	}
	return n, err
}

// WriteHeader sends the HTTP status line and any buffered headers.
// It must be called before Write for custom status codes and headers.
func (rc *ResponseConn) WriteHeader(statusCode int) {
	if rc.wroteHeader {
		return
	}
	rc.statusCode = statusCode
	b := make([]byte, 0, 256)
	b = append(b, "HTTP/1.1 "...)
	b = strconv.AppendInt(b, int64(statusCode), 10)
	b = append(b, ' ')
	b = append(b, StatusReason(statusCode)...)
	b = append(b, "\r\n"...)
	for _, h := range rc.headers {
		b = append(b, h.Key...)
		b = append(b, ": "...)
		b = append(b, h.Value...)
		b = append(b, "\r\n"...)
	}
	b = append(b, "\r\n"...)
	_, err := rc.Conn.Write(b)
	if err != nil {
		rc.err = err
	}
	rc.wroteHeader = true
}

// SetHeader buffers a header to be written with the next WriteHeader call.
// If the header already exists, its value is updated.
func (rc *ResponseConn) SetHeader(key, value []byte) {
	if rc.wroteHeader {
		return
	}
	for i := range rc.headers {
		if bytesEqualFold(rc.headers[i].Key, key) {
			rc.headers[i].Value = value
			return
		}
	}
	rc.headers = append(rc.headers, Header{Key: key, Value: value})
}

// StatusCode returns the status code set by WriteHeader, or 0 if not yet set.
func (rc *ResponseConn) StatusCode() int {
	return rc.statusCode
}

// Read reads from the underlying connection, serving any buffered prefix first.
func (rc *ResponseConn) Read(p []byte) (int, error) {
	if len(rc.prefix) > 0 {
		n := copy(p, rc.prefix)
		rc.prefix = rc.prefix[n:]
		return n, nil
	}
	return rc.Conn.Read(p)
}
