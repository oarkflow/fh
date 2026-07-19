package fh

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/hpack"
)

func encodeTestH2ExtendedConnectHeaders(t *testing.T, protocol, path string) []byte {
	t.Helper()
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	fields := []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":protocol", Value: protocol},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "local"},
		{Name: ":path", Value: path},
	}
	for _, f := range fields {
		if err := enc.WriteField(f); err != nil {
			t.Fatal(err)
		}
	}
	return block.Bytes()
}

// h2TestClientHandshake drives the client side of the HTTP/2 connection
// preface and settings exchange shared by every test below, returning the
// server's parsed initial SETTINGS payload for inspection.
func h2TestClientHandshake(t *testing.T, client net.Conn, clientSettings []byte) map[uint16]uint32 {
	t.Helper()
	if _, err := client.Write(h2ClientPreface); err != nil {
		t.Fatal(err)
	}
	serverSettings, err := readTestH2Frame(client)
	if err != nil || serverSettings.typ != h2Settings || serverSettings.streamID != 0 {
		t.Fatalf("expected server SETTINGS, got %#v err=%v", serverSettings, err)
	}
	got := map[uint16]uint32{}
	for i := 0; i+6 <= len(serverSettings.payload); i += 6 {
		id := binary.BigEndian.Uint16(serverSettings.payload[i : i+2])
		val := binary.BigEndian.Uint32(serverSettings.payload[i+2 : i+6])
		got[id] = val
	}
	if err := writeTestH2Frame(client, h2Settings, 0, 0, clientSettings); err != nil {
		t.Fatal(err)
	}
	ack, err := readTestH2Frame(client)
	if err != nil || ack.typ != h2Settings || ack.flags&h2FlagAck == 0 {
		t.Fatalf("missing settings ack: %#v %v", ack, err)
	}
	return got
}

func TestH2SettingsAdvertisesExtendedConnectProtocol(t *testing.T) {
	app := New()
	client := runPipeApp(t, app)
	settings := h2TestClientHandshake(t, client, nil)
	if v, ok := settings[8]; !ok || v != 1 {
		t.Fatalf("SETTINGS_ENABLE_CONNECT_PROTOCOL (id 8) = %v, ok=%v; want 1, true", v, ok)
	}
}

func TestH2ClassicConnectStillRejectsSchemeAndPath(t *testing.T) {
	s := &h2Stream{}
	err := validateRequestFields(s, []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":authority", Value: "example.com:443"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/should-not-be-here"},
	})
	if err == nil {
		t.Fatal("expected classic CONNECT with :scheme/:path to be rejected")
	}
}

func TestH2ProtocolPseudoHeaderRequiresConnect(t *testing.T) {
	s := &h2Stream{}
	err := validateRequestFields(s, []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":authority", Value: "example.com"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/ws"},
	})
	if err == nil {
		t.Fatal("expected :protocol on a non-CONNECT request to be rejected")
	}
}

func TestH2ExtendedConnectValidation(t *testing.T) {
	s := &h2Stream{}
	err := validateRequestFields(s, []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":authority", Value: "example.com"},
		{Name: ":scheme", Value: "https"},
		{Name: ":path", Value: "/ws"},
	})
	if err != nil {
		t.Fatalf("valid extended CONNECT rejected: %v", err)
	}
	if s.protocol != "websocket" || s.path != "/ws" || s.scheme != "https" {
		t.Fatalf("unexpected stream fields: %+v", s)
	}

	missing := &h2Stream{}
	err = validateRequestFields(missing, []hpack.HeaderField{
		{Name: ":method", Value: "CONNECT"},
		{Name: ":protocol", Value: "websocket"},
		{Name: ":authority", Value: "example.com"},
	})
	if err == nil {
		t.Fatal("expected extended CONNECT missing :scheme/:path to be rejected")
	}
}

// readTunnelDataUntilEnd reads frames for streamID until it sees END_STREAM,
// concatenating DATA payloads and reporting whether an ended HEADERS or DATA
// frame terminated the stream.
func readTunnelDataUntilEnd(t *testing.T, client net.Conn, streamID uint32) []byte {
	t.Helper()
	var body []byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		_ = client.SetReadDeadline(deadline)
		f, err := readTestH2Frame(client)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if f.streamID != streamID {
			continue
		}
		if f.typ == h2Data {
			body = append(body, f.payload...)
		}
		if f.flags&h2FlagEndStream != 0 {
			return body
		}
	}
}

func TestH2ExtendedConnectHandshakeAndEcho(t *testing.T) {
	app := New()
	app.Get("/tunnel", func(c Ctx) error {
		if got := c.ConnectProtocol(); got != "chat" {
			t.Errorf("ConnectProtocol() = %q, want %q", got, "chat")
		}
		return c.Upgrade("chat", func(conn net.Conn) error {
			buf := make([]byte, 16)
			n, err := conn.Read(buf)
			if err != nil {
				return err
			}
			_, err = conn.Write(bytes.ToUpper(buf[:n]))
			return err
		})
	})
	client := runPipeApp(t, app)
	h2TestClientHandshake(t, client, nil)

	block := encodeTestH2ExtendedConnectHeaders(t, "chat", "/tunnel")
	if err := writeTestH2Frame(client, h2Headers, h2FlagEndHeaders, 1, block); err != nil {
		t.Fatal(err)
	}

	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := readTestH2Frame(client)
	if err != nil {
		t.Fatal(err)
	}
	if resp.typ != h2Headers || resp.streamID != 1 {
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

	if err := writeTestH2Frame(client, h2Data, 0, 1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	body := readTunnelDataUntilEnd(t, client, 1)
	if string(body) != "HELLO" {
		t.Fatalf("echoed body = %q, want %q", body, "HELLO")
	}
}

func TestH2ExtendedConnectEmptyTunnelYieldsEOF(t *testing.T) {
	readResult := make(chan error, 1)
	app := New()
	app.Get("/empty", func(c Ctx) error {
		return c.Upgrade("chat", func(conn net.Conn) error {
			buf := make([]byte, 4)
			_, err := conn.Read(buf)
			readResult <- err
			return nil
		})
	})
	client := runPipeApp(t, app)
	h2TestClientHandshake(t, client, nil)

	block := encodeTestH2ExtendedConnectHeaders(t, "chat", "/empty")
	if err := writeTestH2Frame(client, h2Headers, h2FlagEndHeaders|h2FlagEndStream, 1, block); err != nil {
		t.Fatal(err)
	}
	readTunnelDataUntilEnd(t, client, 1)

	select {
	case err := <-readResult:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read() on an immediately half-closed tunnel = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never observed EOF")
	}
}

func TestH2ExtendedConnectRSTUnblocksHandler(t *testing.T) {
	readResult := make(chan error, 1)
	started := make(chan struct{})
	app := New()
	app.Get("/rst", func(c Ctx) error {
		return c.Upgrade("chat", func(conn net.Conn) error {
			close(started)
			buf := make([]byte, 4)
			_, err := conn.Read(buf)
			readResult <- err
			return err
		})
	})
	client := runPipeApp(t, app)
	h2TestClientHandshake(t, client, nil)

	block := encodeTestH2ExtendedConnectHeaders(t, "chat", "/rst")
	if err := writeTestH2Frame(client, h2Headers, h2FlagEndHeaders, 1, block); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if resp, err := readTestH2Frame(client); err != nil || resp.typ != h2Headers {
		t.Fatalf("expected HEADERS response, got %#v err=%v", resp, err)
	}
	<-started

	var code [4]byte
	binary.BigEndian.PutUint32(code[:], h2Cancel)
	if err := writeTestH2Frame(client, h2RSTStream, 0, 1, code[:]); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-readResult:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Read() after RST_STREAM = %v, want net.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler Read() never unblocked after RST_STREAM")
	}
}

func TestH2ExtendedConnectLazyWindowCredit(t *testing.T) {
	const total = 40000
	app := New()
	app.Get("/flow", func(c Ctx) error {
		return c.Upgrade("chat", func(conn net.Conn) error {
			buf := make([]byte, 4096)
			read := 0
			for read < total {
				n, err := conn.Read(buf)
				read += n
				if err != nil {
					return err
				}
			}
			_, err := conn.Write([]byte("done"))
			return err
		})
	})
	client := runPipeApp(t, app)
	h2TestClientHandshake(t, client, nil)

	block := encodeTestH2ExtendedConnectHeaders(t, "chat", "/flow")
	if err := writeTestH2Frame(client, h2Headers, h2FlagEndHeaders, 1, block); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if resp, err := readTestH2Frame(client); err != nil || resp.typ != h2Headers {
		t.Fatalf("expected HEADERS response, got %#v err=%v", resp, err)
	}

	payload := bytes.Repeat([]byte{'x'}, total)
	go func() {
		for off := 0; off < len(payload); {
			n := minInt(len(payload)-off, int(h2DefaultFrame))
			if err := writeTestH2Frame(client, h2Data, 0, 1, payload[off:off+n]); err != nil {
				return
			}
			off += n
		}
	}()

	sawStreamWindowUpdate := false
	var reply []byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		_ = client.SetReadDeadline(deadline)
		f, err := readTestH2Frame(client)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if f.typ == h2WindowUpdate && f.streamID == 1 {
			sawStreamWindowUpdate = true
		}
		if f.streamID != 1 || f.typ != h2Data {
			continue
		}
		reply = append(reply, f.payload...)
		if f.flags&h2FlagEndStream != 0 {
			break
		}
	}
	if string(reply) != "done" {
		t.Fatalf("reply = %q, want %q", reply, "done")
	}
	if !sawStreamWindowUpdate {
		t.Fatal("expected a stream-level WINDOW_UPDATE once the handler consumed data past the threshold")
	}
}
