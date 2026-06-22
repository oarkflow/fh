package fh

import (
	"bytes"
	"strconv"
	"testing"
)

func BenchmarkParseRequestLine(b *testing.B) {
	line := []byte("GET /users/42/posts/7 HTTP/1.1")
	var h RequestHeader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.reset()
		parseRequestLine(line, &h, 8192)
	}
}

func BenchmarkParseRequestLineOptions(b *testing.B) {
	line := []byte("OPTIONS * HTTP/1.1")
	var h RequestHeader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.reset()
		parseRequestLine(line, &h, 8192)
	}
}

var (
	benchMethod = []byte("POST")
	benchPath   = []byte("/path")
	benchGet    = []byte("GET")
	benchRoot   = []byte("/")
)

func BenchmarkParseHeaders(b *testing.B) {
	src := []byte("Host: example.com\r\nContent-Type: application/json\r\nContent-Length: 42\r\nAccept: */*\r\nUser-Agent: test\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n")
	var h RequestHeader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.reset()
		h.Method = benchMethod
		h.URI = benchPath
		h.Proto = strHTTP11
		parseHeaders(src, &h)
	}
}

func BenchmarkParseHeadersMany(b *testing.B) {
	var buf bytes.Buffer
	for i := 0; i < 20; i++ {
		buf.WriteString("X-Header-")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString(": value\r\n")
	}
	buf.WriteString("\r\n")
	src := buf.Bytes()
	var h RequestHeader
	h.Method = benchGet
	h.URI = benchRoot
	h.Proto = strHTTP11
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.reset()
		h.Method = benchGet
		h.URI = benchRoot
		h.Proto = strHTTP11
		parseHeaders(src, &h)
	}
}

func BenchmarkParseContentLength(b *testing.B) {
	vals := []byte("1234567890")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseContentLength(vals)
	}
}

func BenchmarkParseContentLengthMax(b *testing.B) {
	vals := []byte("9223372036854775807")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseContentLength(vals)
	}
}

func BenchmarkParseTransferCodingChunked(b *testing.B) {
	val := []byte("chunked")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseTransferCoding(val)
	}
}

func BenchmarkParseTransferCodingStacked(b *testing.B) {
	val := []byte("gzip, chunked")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseTransferCoding(val)
	}
}

func BenchmarkHasHeaderToken(b *testing.B) {
	val := []byte("keep-alive, upgrade")
	token := "upgrade"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasHeaderToken(val, token)
	}
}

func BenchmarkHasHeaderTokenAbsent(b *testing.B) {
	val := []byte("keep-alive, close")
	token := "upgrade"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasHeaderToken(val, token)
	}
}

func BenchmarkStrEqFold(b *testing.B) {
	a := []byte("Content-Type")
	s := "content-type"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strEqFold(a, s)
	}
}

func BenchmarkValidToken(b *testing.B) {
	tok := []byte("X-Custom-Header-Name")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validToken(tok)
	}
}

func BenchmarkTrimOWS(b *testing.B) {
	v := []byte("  text/html; charset=utf-8  ")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trimOWS(v)
	}
}

func BenchmarkBytesEqualFoldMatch(b *testing.B) {
	a := []byte("Content-Type")
	bv := []byte("content-type")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BytesEqualFold(a, bv)
	}
}

func BenchmarkBytesEqualFoldNoMatch(b *testing.B) {
	a := []byte("Content-Type")
	bv := []byte("content-length")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BytesEqualFold(a, bv)
	}
}
