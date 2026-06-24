package fh

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/pkg/hpack"
)

type singleListener struct {
	conn net.Conn
	done chan struct{}
	once sync.Once
}

func newSingleListener(conn net.Conn) *singleListener {
	return &singleListener{conn: conn, done: make(chan struct{})}
}
func (l *singleListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		c := l.conn
		l.conn = nil
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *singleListener) Close() error { l.once.Do(func() { close(l.done) }); return nil }
func (*singleListener) Addr() net.Addr { return testAddr("pipe") }

func runPipeApp(t *testing.T, app *App) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	ln := newSingleListener(server)
	go func() { _ = app.Serve(ln) }()
	t.Cleanup(func() { _ = client.Close(); _ = app.ShutdownWithTimeout(time.Second) })
	return client
}

func TestChunkedRequestAndTrailers(t *testing.T) {
	app := New()
	app.Post("/upload", func(c Ctx) error {
		return c.SendString(string(c.Body()) + ":" + c.Trailer("X-Checksum"))
	})
	client := runPipeApp(t, app)
	req := "POST /upload HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n" +
		"4;foo=bar\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Checksum: good\r\n\r\n"
	go func() { _, _ = io.WriteString(client, req) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("Wikipedia:good")) {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestExpect100Continue(t *testing.T) {
	app := New()
	app.Post("/expect", func(c Ctx) error { return c.SendString(string(c.Body())) })
	client := runPipeApp(t, app)
	if _, err := io.WriteString(client, "POST /expect HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(client)
	line, err := r.ReadString('\n')
	if err != nil || line != "HTTP/1.1 100 Continue\r\n" {
		t.Fatalf("missing interim response: %q %v", line, err)
	}
	if _, err := r.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.WriteString(client, "body") }()
	resp, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("body")) {
		t.Fatalf("unexpected response: %q", resp)
	}
}

func TestPipelinedRequestAfterFixedBody(t *testing.T) {
	app := New()
	app.Post("/echo", func(c Ctx) error { return c.SendString(string(c.Body())) })
	app.Get("/next", func(c Ctx) error { return c.SendString("next") })
	client := runPipeApp(t, app)
	requests := "POST /echo HTTP/1.1\r\nHost: local\r\nContent-Length: 4\r\n\r\nbody" +
		"GET /next HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
	go func() { _, _ = io.WriteString(client, requests) }()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(resp, []byte("HTTP/1.1 200 OK")) != 2 || !bytes.Contains(resp, []byte("bodyHTTP/1.1")) || !bytes.HasSuffix(resp, []byte("next")) {
		t.Fatalf("pipelined responses corrupted: %q", resp)
	}
}

func TestStreamingChunkedResponse(t *testing.T) {
	app := New()
	app.Get("/stream", func(c Ctx) error {
		return c.Stream(func(w *StreamWriter) error {
			if _, err := w.Write([]byte("alpha")); err != nil {
				return err
			}
			_, err := w.Write([]byte("beta"))
			return err
		})
	})
	client := runPipeApp(t, app)
	go func() {
		_, _ = io.WriteString(client, "GET /stream HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	}()
	resp, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp, []byte("Transfer-Encoding: chunked\r\n")) ||
		!bytes.Contains(resp, []byte("5\r\nalpha\r\n4\r\nbeta\r\n0\r\n\r\n")) {
		t.Fatalf("unexpected streamed response: %q", resp)
	}
}

func TestUpgradePreservesBufferedBytes(t *testing.T) {
	app := New()
	app.Get("/upgrade", func(c Ctx) error {
		return c.Upgrade("echo", func(conn net.Conn) error {
			var p [4]byte
			if _, err := io.ReadFull(conn, p[:]); err != nil {
				return err
			}
			return writeAll(conn, bytes.ToUpper(p[:]))
		})
	})
	client := runPipeApp(t, app)
	request := "GET /upgrade HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\nping"
	go func() { _, _ = io.WriteString(client, request) }()
	r := bufio.NewReader(client)
	var head strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		head.WriteString(line)
		if line == "\r\n" {
			break
		}
	}
	if !strings.Contains(head.String(), "101 Switching Protocols") {
		t.Fatalf("bad upgrade: %q", head.String())
	}
	var pong [4]byte
	if _, err := io.ReadFull(r, pong[:]); err != nil {
		t.Fatal(err)
	}
	if string(pong[:]) != "PING" {
		t.Fatalf("got %q", pong[:])
	}
}

func TestHTTP2PriorKnowledgeRequest(t *testing.T) {
	app := New()
	app.Get("/h2", func(c Ctx) error {
		if string(c.RequestHeader().Proto) != "HTTP/2.0" {
			t.Errorf("unexpected protocol %q", c.RequestHeader().Proto)
		}
		return c.SendString("hello-h2")
	})
	client := runPipeApp(t, app)
	if _, err := client.Write(h2ClientPreface); err != nil {
		t.Fatal(err)
	}
	serverSettings, err := readTestH2Frame(client)
	if err != nil {
		t.Fatal(err)
	}
	if serverSettings.typ != h2Settings || serverSettings.streamID != 0 {
		t.Fatalf("expected SETTINGS, got %#v", serverSettings)
	}
	if err := writeTestH2Frame(client, h2Settings, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	settingsAck, err := readTestH2Frame(client)
	if err != nil || settingsAck.typ != h2Settings || settingsAck.flags&h2FlagAck == 0 {
		t.Fatalf("missing settings ACK: %#v %v", settingsAck, err)
	}

	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: "local"},
		{Name: ":path", Value: "/h2"},
	} {
		if err := enc.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeTestH2Frame(client, h2Headers, h2FlagEndHeaders|h2FlagEndStream, 1, block.Bytes()); err != nil {
		t.Fatal(err)
	}

	decoder := hpack.NewDecoder(4096, func(hpack.HeaderField) {})
	gotStatus, gotBody, ended := "", []byte(nil), false
	for !ended {
		frame, err := readTestH2Frame(client)
		if err != nil {
			t.Fatal(err)
		}
		switch frame.typ {
		case h2Headers:
			fields, err := decoder.DecodeFull(frame.payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				if field.Name == ":status" {
					gotStatus = field.Value
				}
			}
			ended = frame.flags&h2FlagEndStream != 0
		case h2Data:
			gotBody = append(gotBody, frame.payload...)
			ended = frame.flags&h2FlagEndStream != 0
		}
	}
	if gotStatus != "200" || string(gotBody) != "hello-h2" {
		t.Fatalf("status=%q body=%q", gotStatus, gotBody)
	}
}

func TestHTTP2CleartextUpgradeRequestBecomesStreamOne(t *testing.T) {
	app := New()
	app.Post("/upgrade-h2", func(c Ctx) error {
		if string(c.RequestHeader().Proto) != "HTTP/2.0" {
			t.Fatalf("unexpected protocol %q", c.RequestHeader().Proto)
		}
		return c.SendString("upgraded:" + string(c.Body()))
	})
	client := runPipeApp(t, app)
	settings := "AAMAAABk" // SETTINGS_MAX_CONCURRENT_STREAMS = 100.
	request := "POST /upgrade-h2 HTTP/1.1\r\nHost: local\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: " + settings + "\r\nContent-Length: 4\r\n\r\nping"
	if _, err := io.WriteString(client, request); err != nil {
		t.Fatal(err)
	}
	const switching = "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n"
	got := make([]byte, len(switching))
	if _, err := io.ReadFull(client, got); err != nil || string(got) != switching {
		t.Fatalf("upgrade response %q: %v", got, err)
	}
	prefaceWritten := make(chan error, 1)
	go func() {
		_, err := client.Write(h2ClientPreface)
		prefaceWritten <- err
	}()
	serverSettings, err := readTestH2Frame(client)
	if err != nil || serverSettings.typ != h2Settings {
		t.Fatalf("server settings: %#v %v", serverSettings, err)
	}
	if err := <-prefaceWritten; err != nil {
		t.Fatal(err)
	}
	if err := writeTestH2Frame(client, h2Settings, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	ack, err := readTestH2Frame(client)
	if err != nil || ack.typ != h2Settings || ack.flags&h2FlagAck == 0 {
		t.Fatalf("settings ack: %#v %v", ack, err)
	}

	decoder := hpack.NewDecoder(4096, func(hpack.HeaderField) {})
	status, body, ended := "", []byte(nil), false
	for !ended {
		frame, err := readTestH2Frame(client)
		if err != nil {
			t.Fatal(err)
		}
		if frame.streamID != 1 {
			continue
		}
		switch frame.typ {
		case h2Headers:
			fields, err := decoder.DecodeFull(frame.payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				if field.Name == ":status" {
					status = field.Value
				}
			}
		case h2Data:
			body = append(body, frame.payload...)
		}
		ended = frame.flags&h2FlagEndStream != 0
	}
	if status != "200" || string(body) != "upgraded:ping" {
		t.Fatalf("status=%q body=%q", status, body)
	}
}

func TestHTTP2GracefulGoAway(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	app := New()
	app.Get("/slow-h2", func(c Ctx) error { close(started); <-release; return c.SendString("done") })
	client := runPipeApp(t, app)
	if _, err := client.Write(h2ClientPreface); err != nil {
		t.Fatal(err)
	}
	if _, err := readTestH2Frame(client); err != nil {
		t.Fatal(err)
	}
	if err := writeTestH2Frame(client, h2Settings, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := readTestH2Frame(client); err != nil {
		t.Fatal(err)
	}
	block := encodeTestH2Headers(t, "GET", "/slow-h2")
	if err := writeTestH2Frame(client, h2Headers, h2FlagEndHeaders|h2FlagEndStream, 1, block); err != nil {
		t.Fatal(err)
	}
	<-started
	done := make(chan error, 1)
	go func() { done <- app.Shutdown() }()
	goAway, err := readTestH2Frame(client)
	if err != nil {
		t.Fatal(err)
	}
	if goAway.typ != h2GoAway || binary.BigEndian.Uint32(goAway.payload[0:4]) != 1 || binary.BigEndian.Uint32(goAway.payload[4:8]) != h2NoError {
		t.Fatalf("unexpected GOAWAY: %#v", goAway)
	}
	select {
	case <-done:
		t.Fatal("shutdown returned before active h2 stream")
	default:
	}
	close(release)
	for {
		frame, err := readTestH2Frame(client)
		if err != nil {
			break
		}
		if frame.streamID == 1 && frame.flags&h2FlagEndStream != 0 {
			break
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("h2 drain did not finish")
	}
}

func encodeTestH2Headers(t *testing.T, method, path string) []byte {
	t.Helper()
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{{Name: ":method", Value: method}, {Name: ":scheme", Value: "http"}, {Name: ":authority", Value: "local"}, {Name: ":path", Value: path}} {
		if err := enc.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	return block.Bytes()
}

func TestHTTP2TLSInteroperability(t *testing.T) {
	cert := testTLSCertificate(t)
	app := New()
	app.Get("/interop", func(c Ctx) error { return c.SendString("native-h2") })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = app.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	transport := &stdhttp.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport, Timeout: 3 * time.Second}
	resp, err := client.Get("https://" + ln.Addr().String() + "/interop")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 || resp.StatusCode != 200 || string(body) != "native-h2" {
		t.Fatalf("proto=%s status=%d body=%q", resp.Proto, resp.StatusCode, body)
	}
}

func TestHTTP2FlowControlAndMultiplexing(t *testing.T) {
	cert := testTLSCertificate(t)
	large := bytes.Repeat([]byte("flow-control-"), 20000)
	app := New()
	app.Get("/large", func(c Ctx) error { return c.SendBytes(large) })
	app.Post("/echo", func(c Ctx) error { return c.SendBytes(c.Body()) })
	app.Get("/parallel/:id", func(c Ctx) error { return c.SendString(c.Param("id")) })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = app.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	transport := &stdhttp.Transport{ForceAttemptHTTP2: true, MaxConnsPerHost: 1, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport, Timeout: 5 * time.Second}
	base := "https://" + ln.Addr().String()

	resp, err := client.Get(base + "/large")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || !bytes.Equal(got, large) {
		t.Fatalf("large response: len=%d err=%v", len(got), err)
	}

	requestBody := bytes.Repeat([]byte("request-window-"), 10000)
	resp, err = client.Post(base+"/echo", "application/octet-stream", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || !bytes.Equal(got, requestBody) {
		t.Fatalf("large request: len=%d err=%v", len(got), err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		id := strconv.Itoa(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(base + "/parallel/" + id)
			if err != nil {
				errs <- err
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				errs <- readErr
				return
			}
			if string(body) != id {
				errs <- errors.New("multiplexed response mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func testTLSCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writeTestH2Frame(conn net.Conn, typ, flags uint8, streamID uint32, payload []byte) error {
	var head [9]byte
	n := len(payload)
	head[0], head[1], head[2] = byte(n>>16), byte(n>>8), byte(n)
	head[3], head[4] = typ, flags
	binary.BigEndian.PutUint32(head[5:9], streamID)
	if err := writeAll(conn, head[:]); err != nil {
		return err
	}
	return writeAll(conn, payload)
}

func readTestH2Frame(conn net.Conn) (h2Frame, error) {
	var head [9]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return h2Frame{}, err
	}
	n := int(head[0])<<16 | int(head[1])<<8 | int(head[2])
	f := h2Frame{typ: head[3], flags: head[4], streamID: binary.BigEndian.Uint32(head[5:9]), payload: make([]byte, n)}
	_, err := io.ReadFull(conn, f.payload)
	return f, err
}

func TestGracefulShutdownWaitsForActiveRequest(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	app := New()
	app.Get("/slow", func(c Ctx) error {
		close(started)
		<-release
		return c.SendString("done")
	})
	client := runPipeApp(t, app)
	go func() { _, _ = io.WriteString(client, "GET /slow HTTP/1.1\r\nHost: local\r\n\r\n") }()
	responseDone := make(chan struct{})
	go func() { _, _ = io.ReadAll(client); close(responseDone) }()
	<-started
	done := make(chan error, 1)
	go func() { done <- app.Shutdown() }()
	select {
	case <-done:
		t.Fatal("shutdown returned while request was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful shutdown did not finish")
	}
	<-responseDone
}

func TestAutomaticHEADOptionsAndMethodNotAllowed(t *testing.T) {
	makeApp := func() *App {
		app := New()
		app.Get("/resource", func(c Ctx) error { return c.SendString("payload") })
		return app
	}
	tests := []struct {
		name, method string
		wantStatus   string
		want         string
		notWant      string
	}{
		{"head fallback", "HEAD", "200 OK", "Content-Length: 7", "payload"},
		{"automatic options", "OPTIONS", "204 No Content", "Allow: GET", ""},
		{"method not allowed", "POST", "405 Method Not Allowed", "Allow: GET", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := runPipeApp(t, makeApp())
			go func() {
				_, _ = io.WriteString(client, tt.method+" /resource HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
			}()
			resp, err := io.ReadAll(client)
			if err != nil {
				t.Fatal(err)
			}
			s := string(resp)
			if !strings.Contains(s, tt.wantStatus) || !strings.Contains(s, tt.want) {
				t.Fatalf("unexpected response: %q", s)
			}
			if tt.notWant != "" {
				bodyAt := strings.Index(s, "\r\n\r\n")
				if bodyAt >= 0 && strings.Contains(s[bodyAt+4:], tt.notWant) {
					t.Fatalf("unexpected response body: %q", s)
				}
			}
		})
	}
}

func TestConfigurableFallbackHandlers(t *testing.T) {
	makeApp := func() *App {
		app := NewWithConfig(Config{
			NotFoundHandler: func(c Ctx) error {
				return c.Status(StatusTeapot).SendString("custom missing")
			},
			MethodNotAllowed: func(c Ctx, allowed []string) error {
				return c.Status(StatusMethodNotAllowed).SendString("custom methods: " + strings.Join(allowed, "|"))
			},
			OptionsHandler: func(c Ctx, allowed []string) error {
				c.Set("X-Allowed-Count", strconv.Itoa(len(allowed)))
				return c.Status(StatusOK).SendString("custom options")
			},
		})
		app.Get("/resource", func(c Ctx) error { return c.SendString("payload") })
		return app
	}

	tests := []struct {
		name, method, path string
		want               []string
	}{
		{
			name:   "custom not found",
			method: "GET",
			path:   "/missing",
			want:   []string{"418 I'm a teapot", "custom missing"},
		},
		{
			name:   "custom method not allowed",
			method: "POST",
			path:   "/resource",
			want:   []string{"405 Method Not Allowed", "Allow: GET", "custom methods: GET|HEAD|OPTIONS"},
		},
		{
			name:   "custom options",
			method: "OPTIONS",
			path:   "/resource",
			want:   []string{"200 OK", "Allow: GET", "X-Allowed-Count: 3", "custom options"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := runPipeApp(t, makeApp())
			go func() {
				_, _ = io.WriteString(client, tt.method+" "+tt.path+" HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
			}()
			resp, err := io.ReadAll(client)
			if err != nil {
				t.Fatal(err)
			}
			s := string(resp)
			for _, want := range tt.want {
				if !strings.Contains(s, want) {
					t.Fatalf("response missing %q: %q", want, s)
				}
			}
		})
	}
}
