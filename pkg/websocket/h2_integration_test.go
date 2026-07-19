package websocket

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/hpack"
)

// h2ClientPreface mirrors the unexported constant of the same name in the fh
// package (RFC 9113 §3.4) — duplicated here since it isn't exported and this
// is an external test package.
var h2ClientPreface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

const (
	h2FrameHeaders  = uint8(1)
	h2FrameData     = uint8(0)
	h2FrameSettings = uint8(4)

	h2FlagEndStream  = uint8(0x1)
	h2FlagAck        = uint8(0x1)
	h2FlagEndHeaders = uint8(0x4)
)

type h2TestFrame struct {
	typ, flags uint8
	streamID   uint32
	payload    []byte
}

func writeH2TestFrame(conn net.Conn, typ, flags uint8, streamID uint32, payload []byte) error {
	var head [9]byte
	n := len(payload)
	head[0], head[1], head[2] = byte(n>>16), byte(n>>8), byte(n)
	head[3], head[4] = typ, flags
	binary.BigEndian.PutUint32(head[5:9], streamID)
	if _, err := conn.Write(head[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := conn.Write(payload)
	return err
}

func readH2TestFrame(conn net.Conn) (h2TestFrame, error) {
	var head [9]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return h2TestFrame{}, err
	}
	n := int(head[0])<<16 | int(head[1])<<8 | int(head[2])
	f := h2TestFrame{typ: head[3], flags: head[4], streamID: binary.BigEndian.Uint32(head[5:9]), payload: make([]byte, n)}
	_, err := io.ReadFull(conn, f.payload)
	return f, err
}

// TestUpgradeAndEchoOverHTTP2 proves the same websocket.New handler, wired
// through fh's RFC 8441 extended CONNECT support, serves an HTTP/2 client
// identically to the HTTP/1.1 path already covered by TestUpgradeAndEcho —
// no application code changes between the two transports.
func TestUpgradeAndEchoOverHTTP2(t *testing.T) {
	app := fh.New()
	app.Get("/ws", New(func(ws *Conn) error {
		opcode, message, err := ws.ReadMessage()
		if err != nil {
			return err
		}
		return ws.WriteMessage(opcode, message)
	}))
	client := runPipeApp(t, app)
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := client.Write(h2ClientPreface); err != nil {
		t.Fatal(err)
	}
	serverSettings, err := readH2TestFrame(client)
	if err != nil || serverSettings.typ != h2FrameSettings || serverSettings.streamID != 0 {
		t.Fatalf("expected server SETTINGS, got %#v err=%v", serverSettings, err)
	}
	sawConnectProtocol := false
	for i := 0; i+6 <= len(serverSettings.payload); i += 6 {
		id := binary.BigEndian.Uint16(serverSettings.payload[i : i+2])
		val := binary.BigEndian.Uint32(serverSettings.payload[i+2 : i+6])
		if id == 8 && val == 1 {
			sawConnectProtocol = true
		}
	}
	if !sawConnectProtocol {
		t.Fatal("server did not advertise SETTINGS_ENABLE_CONNECT_PROTOCOL")
	}
	if err := writeH2TestFrame(client, h2FrameSettings, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	ack, err := readH2TestFrame(client)
	if err != nil || ack.typ != h2FrameSettings || ack.flags&h2FlagAck == 0 {
		t.Fatalf("missing settings ack: %#v %v", ack, err)
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, f := range []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "local"},
		{Name: ":path", Value: "/ws"},
		{Name: "sec-websocket-version", Value: "13"},
	} {
		if err := enc.WriteField(f); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeH2TestFrame(client, h2FrameHeaders, h2FlagEndHeaders, 1, block.Bytes()); err != nil {
		t.Fatal(err)
	}

	resp, err := readH2TestFrame(client)
	if err != nil {
		t.Fatal(err)
	}
	if resp.typ != h2FrameHeaders || resp.streamID != 1 {
		t.Fatalf("expected HEADERS on stream 1, got %#v", resp)
	}
	if resp.flags&h2FlagEndStream != 0 {
		t.Fatal("extended CONNECT response must not carry END_STREAM")
	}
	decoder := hpack.NewDecoder(4096, func(hpack.HeaderField) {})
	fields, err := decoder.DecodeFull(resp.payload)
	if err != nil {
		t.Fatal(err)
	}
	status := ""
	for _, f := range fields {
		if f.Name == ":status" {
			status = f.Value
		}
	}
	if status != "200" {
		t.Fatalf("status = %q, want 200", status)
	}

	// A real WebSocket text frame, masked per RFC 6455, carried as HTTP/2 DATA.
	wsFrame := maskedFrame(Text, []byte("hello"))
	if err := writeH2TestFrame(client, h2FrameData, 0, 1, wsFrame); err != nil {
		t.Fatal(err)
	}

	var body []byte
	for {
		f, err := readH2TestFrame(client)
		if err != nil {
			t.Fatal(err)
		}
		if f.streamID != 1 || f.typ != h2FrameData {
			continue
		}
		body = append(body, f.payload...)
		if f.flags&h2FlagEndStream != 0 {
			break
		}
	}
	if len(body) < 2 {
		t.Fatalf("echoed websocket frame too short: %x", body)
	}
	if body[0] != 0x80|Text {
		t.Fatalf("bad opcode/fin byte: %#x", body[0])
	}
	length := int(body[1])
	if length != len("hello") {
		t.Fatalf("bad payload length byte: %d", length)
	}
	payload := body[2 : 2+length]
	if string(payload) != "hello" {
		t.Fatalf("echoed payload = %q, want %q", payload, "hello")
	}
}
