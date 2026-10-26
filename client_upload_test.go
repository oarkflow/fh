package fh

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientMultipartStreamsAndReportsErrors(t *testing.T) {
	var reads atomic.Int32
	reader := &countingReader{reader: strings.NewReader("upload payload"), reads: &reads}
	var calls atomic.Int32
	client := NewClient(ClientConfig{Retry: RetryPolicy{MaxAttempts: 2, RetryMethods: map[string]bool{http.MethodPost: true}, RetryStatuses: map[int]bool{503: true}}, Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		mr := multipart.NewReader(req.Body, strings.TrimPrefix(req.Header.Get("Content-Type"), "multipart/form-data; boundary="))
		form, err := mr.ReadForm(1024)
		if err != nil {
			return nil, err
		}
		if form.Value["name"][0] != "fh" {
			t.Fatalf("unexpected field: %#v", form.Value)
		}
		file, err := form.File["file"][0].Open()
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(file)
		_ = file.Close()
		if err != nil || string(data) != "upload payload" {
			t.Fatalf("unexpected upload: %q, %v", data, err)
		}
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("retry")), Header: make(http.Header)}, nil
	})})
	defer client.Close()
	request := client.R().Multipart(map[string]string{"name": "fh"}, File("file", "a.txt", reader))
	if reads.Load() != 0 {
		t.Fatal("multipart upload read before Do")
	}
	response, err := request.Post(context.Background(), "http://example.test/upload")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Close()
	if calls.Load() != 1 {
		t.Fatalf("one-shot upload retried %d times", calls.Load())
	}

	bad := NewClient(ClientConfig{Retry: NoRetry(), Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		_, err := io.Copy(io.Discard, req.Body)
		return nil, err
	})})
	defer bad.Close()
	_, err = bad.R().Multipart(nil, FilePath("file", "missing.txt")).Post(context.Background(), "http://example.test/upload")
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file error, got %v", err)
	}
}

func TestClientBodyLimitRejectsInsteadOfTruncating(t *testing.T) {
	var calls atomic.Int32
	client := NewClient(ClientConfig{BodyLimit: 3, Retry: NoRetry(), Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		_, err := io.Copy(io.Discard, req.Body)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
	})})
	defer client.Close()
	_, err := client.R().Bytes([]byte("four")).Post(context.Background(), "http://example.test/upload")
	if !errors.Is(err, ErrClientBodyTooLarge) || calls.Load() != 0 {
		t.Fatalf("known oversized body: err=%v calls=%d", err, calls.Load())
	}
	_, err = client.R().Reader(bytes.NewBufferString("four")).Post(context.Background(), "http://example.test/upload")
	if !errors.Is(err, ErrClientBodyTooLarge) || calls.Load() != 1 {
		t.Fatalf("streamed oversized body: err=%v calls=%d", err, calls.Load())
	}
	res, err := client.R().Reader(bytes.NewBufferString("abc")).Post(context.Background(), "http://example.test/upload")
	if err != nil {
		t.Fatalf("body exactly at limit was rejected: %v", err)
	}
	_ = res.Close()

	middlewareClient := NewClient(ClientConfig{Retry: NoRetry(), Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		_, readErr := io.Copy(io.Discard, req.Body)
		return nil, readErr
	})})
	middlewareClient.Use(ClientBodyLimit(3))
	defer middlewareClient.Close()
	_, err = middlewareClient.R().Reader(bytes.NewBufferString("four")).Post(context.Background(), "http://example.test/upload")
	if !errors.Is(err, ErrClientBodyTooLarge) {
		t.Fatalf("middleware allowed oversized stream: %v", err)
	}
}

func TestClientMultipartFilePathReplaysOnRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upload.txt")
	if err := os.WriteFile(path, []byte("same on retry"), 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	client := NewClient(ClientConfig{Retry: RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond, RetryMethods: map[string]bool{http.MethodPost: true}, RetryStatuses: map[int]bool{503: true}}, Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		mr := multipart.NewReader(req.Body, strings.TrimPrefix(req.Header.Get("Content-Type"), "multipart/form-data; boundary="))
		form, err := mr.ReadForm(1024)
		if err != nil {
			return nil, err
		}
		file, err := form.File["file"][0].Open()
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(file)
		_ = file.Close()
		if err != nil || string(data) != "same on retry" {
			t.Fatalf("attempt %d: body=%q err=%v", calls.Load()+1, data, err)
		}
		code := 200
		if calls.Add(1) == 1 {
			code = 503
		}
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
	})})
	defer client.Close()
	res, err := client.R().Multipart(nil, FilePath("file", path)).Post(context.Background(), "http://example.test/upload")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	if calls.Load() != 2 || res.StatusCode() != 200 {
		t.Fatalf("calls=%d status=%d", calls.Load(), res.StatusCode())
	}
}

func TestRouterPatternMatchesRouteTemplates(t *testing.T) {
	router := NewRouter()
	router.Add("GET", "/users/:id", func(Ctx) error { return nil })
	router.Add("GET", "/users/me", func(Ctx) error { return nil })
	router.Add("GET", "/assets/*path", func(Ctx) error { return nil })
	for _, tc := range []struct{ method, path, want string }{
		{"GET", "/users/42", "/users/:id"},
		{"GET", "/users/me", "/users/me"},
		{"HEAD", "/users/42", "/users/:id"},
		{"GET", "/assets/a/b", "/assets/*path"},
		{"GET", "/missing", ""},
	} {
		if got := router.Pattern(tc.method, tc.path); got != tc.want {
			t.Fatalf("Pattern(%q, %q) = %q, want %q", tc.method, tc.path, got, tc.want)
		}
	}
}

type countingReader struct {
	reader io.Reader
	reads  *atomic.Int32
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.reader.Read(p)
}
