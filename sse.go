package fh

import (
	"fmt"
)

// SSEvent sends a Server-Sent Event (SSE) to the client with proper headers and event formatting.
func SSEvent(c Ctx, event string, data any) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
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
