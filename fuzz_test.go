package fh

import (
	"testing"
)

// FuzzParseRequestLine fuzzes the HTTP request line parser
func FuzzParseRequestLine(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("GET / HTTP/1.1\r\n"))
	f.Add([]byte("POST /api/v1/users HTTP/1.1\r\n"))
	f.Add([]byte("GET /path?query=value HTTP/1.0\r\n"))
	f.Add([]byte("OPTIONS * HTTP/1.1\r\n"))
	f.Add([]byte("CONNECT example.com:443 HTTP/1.1\r\n"))
	f.Add([]byte("\r\n"))
	f.Add([]byte(""))
	f.Add([]byte("INVALID"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var h RequestHeader
		h.Init()
		// Should not panic regardless of input
		parseRequestLine(data, &h, 8192)
	})
}

// FuzzParseHeaders fuzzes the HTTP header parser
func FuzzParseHeaders(f *testing.F) {
	f.Add([]byte("Host: example.com\r\nContent-Type: text/html\r\n\r\n"))
	f.Add([]byte("Host: example.com\r\n\r\n"))
	f.Add([]byte("\r\n"))
	f.Add([]byte(""))
	f.Add([]byte("X-Long-Header: " + string(make([]byte, 4096)) + "\r\n\r\n"))
	f.Add([]byte(": empty-key\r\n\r\n"))
	f.Add([]byte("Key: \r\n\r\n"))
	// Fuzz with binary garbage
	f.Add([]byte{0x00, 0x01, 0xff, 0xfe})

	f.Fuzz(func(t *testing.T, data []byte) {
		var h RequestHeader
		h.Init()
		parseHeaders(data, &h)
	})
}

// FuzzParseCookie fuzzes cookie parsing
func FuzzParseCookie(f *testing.F) {
	f.Add("session=abc123; token=xyz789")
	f.Add("a=1; b=2; c=3")
	f.Add("")
	f.Add("no-value")
	f.Add("key=")
	f.Add("=value")
	f.Add("key with spaces=value")
	f.Add("key=value; extra")
	// Binary
	f.Add(string([]byte{0x00, 0xff, 0x01}))

	f.Fuzz(func(t *testing.T, data string) {
		ParseCookie(data)
	})
}

// FuzzURLDecode fuzzes URL percent-decoding
func FuzzURLDecode(f *testing.F) {
	f.Add("hello%20world")
	f.Add("%00%01%02")
	f.Add("100%25")
	f.Add("%E4%B8%AD%E6%96%87")
	f.Add("%")
	f.Add("%2")
	f.Add("%GG")
	f.Add("a%0Ab") // newline injection
	f.Add("a%0Db") // CR injection

	f.Fuzz(func(t *testing.T, data string) {
		urlDecode([]byte(data))
	})
}

// FuzzValidCookieValue fuzzes cookie value validation
func FuzzValidCookieValue(f *testing.F) {
	f.Add("simple")
	f.Add("with space")
	f.Add("with\"quote")
	f.Add("with;semi")
	f.Add("with,comma")
	f.Add("with\\backslash")
	f.Add(string([]byte{0x00}))
	f.Add(string([]byte{0x7f}))
	f.Add(string([]byte{0x20}))

	f.Fuzz(func(t *testing.T, data string) {
		validCookieValue(data)
	})
}

// FuzzValidToken fuzzes HTTP token validation
func FuzzValidToken(f *testing.F) {
	f.Add([]byte("GET"))
	f.Add([]byte("Content-Type"))
	f.Add([]byte(""))
	f.Add([]byte("with space"))
	f.Add([]byte("with\"quote"))
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		validToken(data)
	})
}

// FuzzCleanLookupPath fuzzes path normalization
func FuzzCleanLookupPath(f *testing.F) {
	f.Add([]byte("/api/v1/users"))
	f.Add([]byte("//double/slash"))
	f.Add([]byte("/trailing/slash/"))
	f.Add([]byte(""))
	f.Add([]byte("/"))
	f.Add([]byte("/../../../etc/passwd"))
	f.Add([]byte{0x00, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		cleanLookupPath(data)
	})
}

// FuzzRedactSecrets fuzzes the secret redaction function
func FuzzRedactSecrets(f *testing.F) {
	f.Add("password=secret123")
	f.Add("Authorization: Bearer token123")
	f.Add("api_key=abc")
	f.Add("no secrets here")
	f.Add("token%3Dsecret%26other=data")

	f.Fuzz(func(t *testing.T, data string) {
		RedactSecrets(data)
	})
}

// FuzzAppendJSONString fuzzes JSON string encoding
func FuzzAppendJSONString(f *testing.F) {
	f.Add("hello")
	f.Add("with\"quote")
	f.Add("with\nnewline")
	f.Add("with\ttab")
	f.Add(string([]byte{0x00}))
	f.Add(string([]byte{0x1f}))
	f.Add(string([]byte{0x7f}))
	f.Add("unicode: \u00e9\u00e8")
	f.Add("backslash: \\n\\t\\r")

	f.Fuzz(func(t *testing.T, data string) {
		buf := make([]byte, 0, 1024)
		appendJSONString(buf, data)
	})
}

// FuzzEscapeJSONString fuzzes JSON string escaping
func FuzzEscapeJSONString(f *testing.F) {
	f.Add("simple")
	f.Add("with\"quote")
	f.Add("with\\backslash")
	f.Add(string([]byte{0x00}))

	f.Fuzz(func(t *testing.T, data string) {
		escapeJSONString(data)
	})
}

// FuzzValidCookieDomain fuzzes cookie domain validation
func FuzzValidCookieDomain(f *testing.F) {
	f.Add("example.com")
	f.Add(".example.com")
	f.Add("-example.com")
	f.Add("a.b.c.d.example.com")
	f.Add("")
	f.Add("localhost")
	f.Add("127.0.0.1")
	f.Add(string([]byte{0x00}))
	f.Add("example.com:8080")

	f.Fuzz(func(t *testing.T, data string) {
		validCookieDomain(data)
	})
}

// FuzzParseContentLength fuzzes content-length parsing
func FuzzParseContentLength(f *testing.F) {
	f.Add([]byte("0"))
	f.Add([]byte("12345"))
	f.Add([]byte("-1"))
	f.Add([]byte("abc"))
	f.Add([]byte("99999999999999999999"))
	f.Add([]byte(""))
	f.Add([]byte(" 123 "))
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		parseContentLength(data)
	})
}
