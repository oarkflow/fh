package fh

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"
)

// SSEEvent represents an individual Server-Sent Event frame.
type SSEEvent struct {
	ID      string
	Event   string
	Data    any
	Retry   time.Duration
	Comment string
}

// SSE is a streaming controller for Server-Sent Events connections.
type SSE struct {
	ctx *DefaultCtx
	sw  *StreamWriter
}

// WriteEvent formats and sends a single SSE event frame to the stream.
func (s *SSE) WriteEvent(ev SSEEvent) error {
	var buf bytes.Buffer

	if ev.Comment != "" {
		for _, line := range strings.Split(ev.Comment, "\n") {
			buf.WriteString(": ")
			buf.WriteString(line)
			buf.WriteString("\n")
		}
	}

	if ev.ID != "" {
		buf.WriteString("id: ")
		buf.WriteString(ev.ID)
		buf.WriteString("\n")
	}

	if ev.Event != "" {
		buf.WriteString("event: ")
		buf.WriteString(ev.Event)
		buf.WriteString("\n")
	}

	if ev.Retry > 0 {
		buf.WriteString(fmt.Sprintf("retry: %d\n", ev.Retry.Milliseconds()))
	}

	if ev.Data != nil {
		var payload string
		switch v := ev.Data.(type) {
		case string:
			payload = v
		case []byte:
			payload = string(v)
		default:
			b, err := JSONMarshal(v)
			if err != nil {
				return err
			}
			payload = string(b)
		}

		for _, line := range strings.Split(payload, "\n") {
			buf.WriteString("data: ")
			buf.WriteString(line)
			buf.WriteString("\n")
		}
	}

	buf.WriteString("\n")
	_, err := s.sw.Write(buf.Bytes())
	return err
}

// Send sends data with no custom event type (defaults to standard "message" event).
func (s *SSE) Send(data any) error {
	return s.WriteEvent(SSEEvent{Data: data})
}

// SendEvent sends data with a specific event type.
func (s *SSE) SendEvent(event string, data any) error {
	return s.WriteEvent(SSEEvent{Event: event, Data: data})
}

// Comment sends a comment line (ignored by client JavaScript EventSource, useful for heartbeats/pings).
func (s *SSE) Comment(comment string) error {
	return s.WriteEvent(SSEEvent{Comment: comment})
}

// Ping sends an empty comment ping (`: ping\n\n`) to keep the connection alive through proxy timeouts.
func (s *SSE) Ping() error {
	return s.Comment("ping")
}

// SetRetry informs the client how long to wait before attempting to reconnect.
func (s *SSE) SetRetry(d time.Duration) error {
	return s.WriteEvent(SSEEvent{Retry: d})
}

// Context returns the request context, which is canceled when the client disconnects.
func (s *SSE) Context() context.Context {
	return s.ctx.Context()
}

// Done returns a channel that is closed when the client connection terminates.
func (s *SSE) Done() <-chan struct{} {
	return s.ctx.Done()
}

// SSEvent sends a standalone Server-Sent Event (SSE) to the client with proper headers.
func SSEvent(c Ctx, event string, data any) error {
	c.Set("Content-Type", "text/event-stream; charset=utf-8")
	c.Set("Cache-Control", "no-cache, no-transform")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	var payload string
	switch v := data.(type) {
	case string:
		payload = v
	case []byte:
		payload = string(v)
	default:
		b, err := JSONMarshal(v)
		if err != nil {
			return err
		}
		payload = string(b)
	}

	var msg string
	if event != "" {
		msg = fmt.Sprintf("event: %s\ndata: %s\n\n", event, payload)
	} else {
		msg = fmt.Sprintf("data: %s\n\n", payload)
	}

	return c.SendString(msg)
}

// SSE starts a persistent Server-Sent Events stream, setting standard headers and
// passing an *SSE stream controller to fn.
func (c *DefaultCtx) SSE(fn func(*SSE) error) error {
	c.Set("Content-Type", "text/event-stream; charset=utf-8")
	c.Set("Cache-Control", "no-cache, no-transform")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	return c.Stream(func(sw *StreamWriter) error {
		sse := &SSE{
			ctx: c,
			sw:  sw,
		}
		return fn(sse)
	})
}

// LastEventID returns the Last-Event-ID header sent by the client when reconnecting,
// or the ?lastEventId= query parameter as a fallback.
func (c *DefaultCtx) LastEventID() string {
	if id := c.Get(HeaderLastEventID); id != "" {
		return id
	}
	if id := c.Get("Last-Event-ID"); id != "" {
		return id
	}
	if id := c.Query("lastEventId"); id != "" {
		return id
	}
	return c.Query("last_event_id")
}
