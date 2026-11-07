package fh

import (
	"errors"
	"strings"
	"testing"
)

func TestStreamCallbackErrorDoesNotFinishChunkedBody(t *testing.T) {
	app := New()
	app.Get("/", func(c Ctx) error {
		return c.Stream(func(w *StreamWriter) error {
			if _, err := w.Write([]byte("partial")); err != nil {
				return err
			}
			return errors.New("source failed")
		})
	})
	raw := pipeRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(raw, "\r\n7\r\npartial\r\n") {
		t.Fatalf("missing partial data: %q", raw)
	}
	if strings.Contains(raw, "\r\n0\r\n\r\n") {
		t.Fatalf("partial response was marked complete: %q", raw)
	}
}
