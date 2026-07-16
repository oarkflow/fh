package fh

import (
	"encoding/binary"
	"testing"

	"github.com/oarkflow/fh/pkg/hpack"
)

func TestHTTP1AbsoluteFormNormalizesRoutePathAndPreservesTarget(t *testing.T) {
	var h RequestHeader
	line := []byte("GET http://example.com/api/users?active=1 HTTP/1.1\r\n")
	if _, err := parseRequestLine(line, &h, 8192); err != nil {
		t.Fatalf("parseRequestLine error: %v", err)
	}
	if string(h.RequestTarget) != "http://example.com/api/users?active=1" {
		t.Fatalf("request target not preserved: %q", h.RequestTarget)
	}
	if string(h.URI) != "/api/users?active=1" {
		t.Fatalf("URI not normalized for routing: %q", h.URI)
	}
	if string(h.Path) != "/api/users" {
		t.Fatalf("path not normalized: %q", h.Path)
	}
	if string(h.QueryString) != "active=1" {
		t.Fatalf("query not normalized: %q", h.QueryString)
	}
}

func TestHTTP1HeaderLimitUsesConfiguredLimit(t *testing.T) {
	var h RequestHeader
	_, err := parseRequestLine([]byte("GET / HTTP/1.1\r\n"), &h, 8192)
	if err != nil {
		t.Fatal(err)
	}
	head := []byte("Host: example.com\r\nA: 1\r\nB: 2\r\n\r\n")
	if _, err := parseHeadersWithLimit(head, &h, 2); err == nil {
		t.Fatal("expected configured header-count limit to reject request")
	}
}

func TestHTTP2ClientSettingsDoNotMutateAppConfig(t *testing.T) {
	app := NewFast(WithMaxConcurrentStreams(128))
	h := newH2Conn(app, nil)
	var payload [6]byte
	binary.BigEndian.PutUint16(payload[0:2], 3) // SETTINGS_MAX_CONCURRENT_STREAMS
	binary.BigEndian.PutUint32(payload[2:6], 1)
	if err := h.applySettings(payload[:]); err != nil {
		t.Fatalf("applySettings: %v", err)
	}
	if app.cfg.MaxConcurrentStreams != 128 {
		t.Fatalf("client SETTINGS mutated global app config: got %d", app.cfg.MaxConcurrentStreams)
	}
	if h.localMaxConcurrentStreams != 128 {
		t.Fatalf("client SETTINGS mutated local inbound limit: got %d", h.localMaxConcurrentStreams)
	}
	if got := h.peerMaxConcurrentStreams.Load(); got != 1 {
		t.Fatalf("peer setting not stored per-connection: got %d", got)
	}
}

func TestHTTP2InboundFrameLimitIsLocal(t *testing.T) {
	app := NewFast()
	h := newH2Conn(app, nil)
	var payload [6]byte
	binary.BigEndian.PutUint16(payload[0:2], 5) // SETTINGS_MAX_FRAME_SIZE sent by client
	binary.BigEndian.PutUint32(payload[2:6], 1<<20)
	if err := h.applySettings(payload[:]); err != nil {
		t.Fatalf("applySettings: %v", err)
	}
	if got := h.peerMaxFrame.Load(); got != 1<<20 {
		t.Fatalf("peer send max frame not updated: %d", got)
	}
	if got := h.localMaxFrame.Load(); got != h2DefaultFrame {
		t.Fatalf("local receive max frame changed from server-advertised default: %d", got)
	}
}

func TestHTTP2HeaderCountRejected(t *testing.T) {
	s := &h2Stream{}
	fields := []hpackHeaderFieldForTest{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "example.com"},
		{Name: ":path", Value: "/"},
		{Name: "x-a", Value: "1"},
		{Name: "x-b", Value: "2"},
	}
	converted := makeHpackFieldsForTest(fields)
	if err := validateRequestFields(s, converted, 1); err == nil {
		t.Fatal("expected h2 header-count limit to reject request")
	}
}

type hpackHeaderFieldForTest struct{ Name, Value string }

func makeHpackFieldsForTest(in []hpackHeaderFieldForTest) []hpack.HeaderField {
	out := make([]hpack.HeaderField, len(in))
	for i := range in {
		out[i] = hpack.HeaderField{Name: in[i].Name, Value: in[i].Value}
	}
	return out
}
