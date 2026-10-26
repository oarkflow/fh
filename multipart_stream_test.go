package fh

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"mime/multipart"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func multipartFixture(t *testing.T) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.WriteField("name", "fh"); err != nil {
		t.Fatal(err)
	}
	f, err := w.CreateFormFile("file", "upload.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(f, "payload"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return body.Bytes(), w.FormDataContentType()
}

func TestStreamMultipartHTTP1AndLimits(t *testing.T) {
	body, contentType := multipartFixture(t)
	for _, tc := range []struct {
		name string
		opts MultipartStreamOptions
		want string
	}{
		{"valid", MultipartStreamOptions{MaxParts: 2, MaxFieldBytes: 2, MaxFileBytes: 7, MaxTotalBytes: int64(len(body))}, "200 OK"},
		{"parts", MultipartStreamOptions{MaxParts: 1}, "413 Payload Too Large"},
		{"field", MultipartStreamOptions{MaxFieldBytes: 1}, "413 Payload Too Large"},
		{"file", MultipartStreamOptions{MaxFileBytes: 6}, "413 Payload Too Large"},
		{"total", MultipartStreamOptions{MaxTotalBytes: int64(len(body) - 1)}, "413 Payload Too Large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := New(WithStreamRequestBody(true))
			app.Post("/upload", func(c Ctx) error {
				var result strings.Builder
				err := StreamMultipart(c, tc.opts, func(p MultipartStreamPart) error {
					result.WriteString(p.FieldName + "=")
					_, err := io.Copy(&result, p.Reader)
					return err
				})
				if err != nil {
					return err
				}
				return c.SendString(result.String())
			})
			request := "POST /upload HTTP/1.1\r\nHost: local\r\nContent-Type: " + contentType + "\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nConnection: close\r\n\r\n" + string(body)
			response := pipeRequest(t, app, request)
			if !strings.Contains(response, tc.want) {
				t.Fatalf("response = %q, want %s", response, tc.want)
			}
			if tc.name == "valid" && !strings.HasSuffix(response, "name=fhfile=payload") {
				t.Fatalf("unexpected parts: %q", response)
			}
		})
	}
}

func TestStreamMultipartHTTP2(t *testing.T) {
	body, contentType := multipartFixture(t)
	app := New(WithStreamRequestBody(true))
	app.Post("/upload", func(c Ctx) error {
		var result strings.Builder
		opts := MultipartStreamOptions{}
		if c.Query("limit") == "1" {
			opts.MaxFileBytes = 6
		}
		if err := StreamMultipart(c, opts, func(p MultipartStreamPart) error {
			_, err := io.Copy(&result, p.Reader)
			return err
		}); err != nil {
			return err
		}
		return c.SendString(result.String())
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cert := testTLSCertificate(t)
	go func() {
		_ = app.ServeTLS(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}()
	t.Cleanup(func() { _ = app.ShutdownWithTimeout(time.Second) })
	transport := &stdhttp.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport, Timeout: 3 * time.Second}
	resp, err := client.Post("https://"+ln.Addr().String()+"/upload", contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || resp.ProtoMajor != 2 || resp.StatusCode != 200 || string(got) != "fhpayload" {
		t.Fatalf("proto=%s status=%d body=%q err=%v", resp.Proto, resp.StatusCode, got, err)
	}
	resp, err = client.Post("https://"+ln.Addr().String()+"/upload?limit=1", contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != 413 {
		t.Fatalf("oversized HTTP/2 upload: proto=%s status=%d", resp.Proto, resp.StatusCode)
	}
}

type canceledMultipartCtx struct {
	Ctx
	ctx         context.Context
	body        []byte
	contentType string
}

func (c canceledMultipartCtx) Context() context.Context     { return c.ctx }
func (c canceledMultipartCtx) Get(string, ...string) string { return c.contentType }
func (c canceledMultipartCtx) StreamBody(fn func(io.Reader) error) error {
	return fn(bytes.NewReader(c.body))
}

func TestStreamMultipartCancellation(t *testing.T) {
	body, contentType := multipartFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := StreamMultipart(canceledMultipartCtx{ctx: ctx, body: body, contentType: contentType}, MultipartStreamOptions{}, func(MultipartStreamPart) error { t.Fatal("callback ran after cancellation"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}
