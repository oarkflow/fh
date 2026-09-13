package logger

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestNewMiddlewareDefaultConfig(t *testing.T) {
	m := NewMiddleware()
	if m == nil {
		t.Fatal("expected non-nil middleware")
	}
	if m.cfg.TimeFormat != time.RFC3339 {
		t.Errorf("expected RFC3339 time format, got %q", m.cfg.TimeFormat)
	}
	if m.cfg.QueueSize != 4096 {
		t.Errorf("expected queue size 4096, got %d", m.cfg.QueueSize)
	}
	if m.cfg.MaxLineBytes != 4096 {
		t.Errorf("expected max line bytes 4096, got %d", m.cfg.MaxLineBytes)
	}
	if !m.cfg.SkipDefaultStatic {
		t.Error("expected SkipDefaultStatic to be true")
	}
}

func TestNewMiddlewareWithCustomConfig(t *testing.T) {
	m := NewMiddleware(Config{
		FormatName:   "json",
		QueueSize:    1024,
		DisableAsync: true,
	})
	if !m.json {
		t.Error("expected json mode for FormatName=json")
	}
	if m.cfg.QueueSize != 1024 {
		t.Errorf("expected queue size 1024, got %d", m.cfg.QueueSize)
	}
}

func TestNewMiddlewareFormatNames(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{"default", formatDefault},
		{"common", formatCommon},
		{"combined", formatCombined},
		{"tiny", formatTiny},
	}
	for _, tt := range tests {
		m := NewMiddleware(Config{FormatName: tt.name})
		if m.cfg.Format != tt.format {
			t.Errorf("FormatName=%q: expected format %q, got %q", tt.name, tt.format, m.cfg.Format)
		}
	}
}

func TestParseLogFormat(t *testing.T) {
	tokens := parseLogFormat("${method} ${path} ${status}")
	if len(tokens) != 5 {
		t.Fatalf("expected 5 tokens, got %d", len(tokens))
	}
	if tokens[0].typ != logMethod {
		t.Errorf("expected logMethod, got %d", tokens[0].typ)
	}
	if tokens[1].typ != logText {
		t.Errorf("expected logText, got %d", tokens[1].typ)
	}
	if tokens[2].typ != logPath {
		t.Errorf("expected logPath, got %d", tokens[2].typ)
	}
	if tokens[3].typ != logText {
		t.Errorf("expected logText, got %d", tokens[3].typ)
	}
	if tokens[4].typ != logStatus {
		t.Errorf("expected logStatus, got %d", tokens[4].typ)
	}
}

func TestParseLogFormatAllTokens(t *testing.T) {
	format := "${time} ${ip} ${method} ${path} ${query} ${uri} ${status} ${latency} ${error} ${request_id}"
	tokens := parseLogFormat(format)
	expected := []logTokenType{logTime, logIP, logMethod, logPath, logQuery, logURI, logStatus, logLatency, logError, logRequestID}
	if len(tokens) != len(expected)*2-1 {
		t.Fatalf("expected %d tokens, got %d", len(expected)*2-1, len(tokens))
	}
	j := 0
	for i := 0; i < len(tokens); i++ {
		if tokens[i].typ == logText {
			continue
		}
		if j >= len(expected) {
			t.Fatalf("too many non-text tokens")
		}
		if tokens[i].typ != expected[j] {
			t.Errorf("token %d: expected %d, got %d", j, expected[j], tokens[i].typ)
		}
		j++
	}
}

func TestParseLogFormatUnknownToken(t *testing.T) {
	tokens := parseLogFormat("${unknown}")
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].typ != logText {
		t.Errorf("expected logText for unknown token, got %d", tokens[0].typ)
	}
}

func TestParseLogFormatUnclosedBrace(t *testing.T) {
	tokens := parseLogFormat("${unclosed")
	if len(tokens) != 1 {
		t.Fatalf("expected 1 token, got %d", len(tokens))
	}
	if tokens[0].typ != logText {
		t.Errorf("expected logText, got %d", tokens[0].typ)
	}
}

func TestSkipMatcherPre(t *testing.T) {
	s := newSkipMatcher(Config{
		SkipMethods:  []string{"OPTIONS"},
		SkipPaths:    []string{"/health"},
		SkipPrefixes: []string{"/static/"},
	})
	if !s.pre([]byte("OPTIONS"), []byte("/test")) {
		t.Error("expected OPTIONS to be skipped")
	}
	if !s.pre([]byte("GET"), []byte("/health")) {
		t.Error("expected /health to be skipped")
	}
	if !s.pre([]byte("GET"), []byte("/static/app.js")) {
		t.Error("expected /static/ prefix to be skipped")
	}
	if s.pre([]byte("GET"), []byte("/api/data")) {
		t.Error("expected /api/data to not be skipped")
	}
}

func TestSkipMatcherPost(t *testing.T) {
	s := newSkipMatcher(Config{
		SkipStatusCodes: []int{404, 500},
	})
	if !s.post(404) {
		t.Error("expected 404 to be skipped")
	}
	if !s.post(500) {
		t.Error("expected 500 to be skipped")
	}
	if s.post(200) {
		t.Error("expected 200 to not be skipped")
	}
}

func TestSkipMatcherExtensions(t *testing.T) {
	s := newSkipMatcher(Config{
		SkipDefaultStatic: true,
	})
	if !s.pre([]byte("GET"), []byte("/app.css")) {
		t.Error("expected .css to be skipped")
	}
	if !s.pre([]byte("GET"), []byte("/bundle.js")) {
		t.Error("expected .js to be skipped")
	}
	if s.pre([]byte("GET"), []byte("/api/data")) {
		t.Error("expected /api/data to not be skipped")
	}
}

func TestNormalizeExt(t *testing.T) {
	tests := []struct {
		ext  string
		want string
	}{
		{"css", ".css"},
		{".CSS", ".css"},
		{"", ""},
		{".Js", ".js"},
	}
	for _, tt := range tests {
		got := normalizeExt(tt.ext)
		if got != tt.want {
			t.Errorf("normalizeExt(%q) = %q, want %q", tt.ext, got, tt.want)
		}
	}
}

func TestURIPathAndQuery(t *testing.T) {
	tests := []struct {
		uri        string
		wantPath   string
		wantQuery  string
		includeAll bool
	}{
		{"/api/data?foo=bar", "/api/data", "foo=bar", false},
		{"/api/data", "/api/data", "", false},
		{"/api/data?foo=bar", "/api/data?foo=bar", "foo=bar", true},
	}
	for _, tt := range tests {
		gotPath := string(uriPath([]byte(tt.uri), tt.includeAll))
		if gotPath != tt.wantPath {
			t.Errorf("uriPath(%q, %v) = %q, want %q", tt.uri, tt.includeAll, gotPath, tt.wantPath)
		}
		gotQuery := string(uriQuery([]byte(tt.uri)))
		if gotQuery != tt.wantQuery {
			t.Errorf("uriQuery(%q) = %q, want %q", tt.uri, gotQuery, tt.wantQuery)
		}
	}
}

func TestHasAnyExt(t *testing.T) {
	exts := []string{".css", ".js"}
	if !hasAnyExt([]byte("/app.css"), exts) {
		t.Error("expected .css to match")
	}
	if hasAnyExt([]byte("/api/data"), exts) {
		t.Error("expected no match for no extension")
	}
	if hasAnyExt([]byte("/file.txt"), exts) {
		t.Error("expected .txt to not match")
	}
}

func TestASCIIEqualBytesString(t *testing.T) {
	if !asciiEqualBytesString([]byte("GET"), "GET") {
		t.Error("expected equal")
	}
	if !asciiEqualBytesString([]byte("get"), "GET") {
		t.Error("expected case-insensitive equal")
	}
	if asciiEqualBytesString([]byte("POST"), "GET") {
		t.Error("expected not equal")
	}
	if asciiEqualBytesString([]byte("GE"), "GET") {
		t.Error("expected different lengths to not be equal")
	}
}

func TestClose(t *testing.T) {
	m := NewMiddleware(Config{DisableAsync: true})
	if err := m.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestDropped(t *testing.T) {
	m := NewMiddleware(Config{DisableAsync: true})
	if got := m.Dropped(); got != 0 {
		t.Errorf("expected 0 dropped, got %d", got)
	}
}

func TestNewReturnsHandlerFunc(t *testing.T) {
	handler := New()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestSlogOutput(t *testing.T) {
	var buf strings.Builder
	sl := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m := NewMiddleware(Config{
		Slog:         sl,
		DisableAsync: true,
	})
	if !m.slogOn {
		t.Error("expected slogOn to be true")
	}
	_ = m
}
