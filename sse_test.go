package fh

import (
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSSEStreaming(t *testing.T) {
	app := New()

	app.Get("/events", func(c Ctx) error {
		return c.SSE(func(stream *SSE) error {
			_ = stream.SetRetry(5 * time.Second)
			_ = stream.Comment("connection established")
			_ = stream.WriteEvent(SSEEvent{
				ID:    "101",
				Event: "greeting",
				Data:  Map{"msg": "hello from sse"},
			})
			_ = stream.Ping()
			return nil
		})
	})

	app.Get("/last-id", func(c Ctx) error {
		return c.SendString("last=" + c.LastEventID())
	})

	t.Run("EventFormatting", func(t *testing.T) {
		client, server := net.Pipe()
		ln := newPipeListener(server)

		go func() {
			_ = app.Serve(ln)
		}()
		defer app.ShutdownWithTimeout(time.Second)

		go func() {
			req := "GET /events HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
			_, _ = client.Write([]byte(req))
		}()

		respBytes, err := io.ReadAll(client)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}

		respStr := string(respBytes)
		if !strings.Contains(respStr, "text/event-stream") {
			t.Errorf("expected text/event-stream header, got %q", respStr)
		}
		if !strings.Contains(respStr, "retry: 5000") {
			t.Errorf("expected retry directive, got %q", respStr)
		}
		if !strings.Contains(respStr, ": connection established") {
			t.Errorf("expected comment, got %q", respStr)
		}
		if !strings.Contains(respStr, "id: 101") || !strings.Contains(respStr, "event: greeting") {
			t.Errorf("expected id and event fields, got %q", respStr)
		}
		if !strings.Contains(respStr, ": ping") {
			t.Errorf("expected ping comment, got %q", respStr)
		}
	})

	t.Run("LastEventID", func(t *testing.T) {
		app2 := New()
		app2.Get("/last-id", func(c Ctx) error {
			return c.SendString("last=" + c.LastEventID())
		})

		req := httptest.NewRequest("GET", "/last-id", nil)
		req.Header.Set("Last-Event-ID", "msg_999")
		resp, err := app2.Test(req)
		if err != nil {
			t.Fatalf("Test error: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if string(body) != "last=msg_999" {
			t.Errorf("expected 'last=msg_999', got %q", string(body))
		}
	})
}
