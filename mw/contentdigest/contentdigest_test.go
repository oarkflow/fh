package contentdigest

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestFormatAndVerify(t *testing.T) {
	field, err := Format(SHA256, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha-256=:LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=:"
	if field != want {
		t.Fatalf("field=%q", field)
	}
	if err := Verify("crc32=:AAAAAA==:, "+field, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := Verify(field, []byte("changed")); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err=%v", err)
	}
}

func TestWants(t *testing.T) {
	if !Wants("sha-512=1, sha-256=3", SHA256) {
		t.Fatal("sha-256 should be wanted")
	}
	if Wants("sha-256=0", SHA256) {
		t.Fatal("zero preference must not be wanted")
	}
}

func TestMiddlewareVerifiesRequestAndAddsResponse(t *testing.T) {
	app := fh.NewFast(fh.WithDisableHTTP2(true), fh.WithDisablePanicRecovery(true))
	app.Post("/echo", New(Config{RequireRequest: true, Response: ResponseAlways}), func(c fh.Ctx) error {
		return c.SendBytes(c.Body())
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Serve(ln) }()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })

	body := []byte("integrity")
	field, _ := Format(SHA256, body)
	req, err := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/echo", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Digest", field)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fh.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("status=%d body=%q", resp.StatusCode, got)
	}
	if err := Verify(resp.Header.Get("Content-Digest"), got); err != nil {
		t.Fatalf("response digest: %v", err)
	}
}
