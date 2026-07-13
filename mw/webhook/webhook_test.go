package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func testServer(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Shutdown() })
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	return ln.Addr().String()
}

func doRequest(t *testing.T, addr, method, path, body string, headers map[string]string) (statusCode int, respBody string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := fmt.Sprintf("%s %s HTTP/1.1\r\nHost: localhost\r\n", method, path)
	for k, v := range headers {
		req += k + ": " + v + "\r\n"
	}
	if body != "" {
		req += fmt.Sprintf("Content-Length: %d\r\n", len(body))
	}
	req += "\r\n" + body

	conn.Write([]byte(req))
	conn.(*net.TCPConn).CloseWrite()

	resp, err := io.ReadAll(conn)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(resp), "\r\n", 2)
	if len(parts) < 1 {
		t.Fatal("empty response")
	}
	var proto, status string
	fmt.Sscan(parts[0], &proto, &status)
	fmt.Sscan(status, &statusCode)

	idx := strings.Index(string(resp), "\r\n\r\n")
	if idx >= 0 {
		respBody = string(resp)[idx+4:]
	}
	return
}

func sign(secret []byte, ts, body string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts + "." + body))
	return hex.EncodeToString(mac.Sum(nil))
}

func TestReplayIsRejectedByDefault(t *testing.T) {
	secret := []byte("s3cr3t")
	app := fh.New()
	app.Use(New(Config{Secret: secret}))
	app.Post("/hook", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	body := `{"event":"ping"}`
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := sign(secret, ts, body)
	headers := map[string]string{"X-Signature": sig, "X-Timestamp": ts}

	code, _ := doRequest(t, addr, "POST", "/hook", body, headers)
	if code != 200 {
		t.Fatalf("expected first delivery to succeed, got %d", code)
	}

	code, _ = doRequest(t, addr, "POST", "/hook", body, headers)
	if code == 200 {
		t.Fatal("expected replayed webhook delivery to be rejected by default")
	}
}

func TestDistinctDeliveriesAreNotTreatedAsReplay(t *testing.T) {
	secret := []byte("s3cr3t")
	app := fh.New()
	app.Use(New(Config{Secret: secret}))
	app.Post("/hook", func(c fh.Ctx) error { return c.SendString("ok") })
	addr := testServer(t, app)

	for i := 0; i < 2; i++ {
		body := fmt.Sprintf(`{"event":"ping","n":%d}`, i)
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		sig := sign(secret, ts, body)
		headers := map[string]string{"X-Signature": sig, "X-Timestamp": ts}
		code, _ := doRequest(t, addr, "POST", "/hook", body, headers)
		if code != 200 {
			t.Fatalf("delivery %d: expected 200, got %d", i, code)
		}
	}
}
