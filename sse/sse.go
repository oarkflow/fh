package sse

import (
	"encoding/json"
	"fmt"
	"github.com/oarkflow/fh"
	"strconv"
	"strings"
	"time"
)

type Event struct {
	ID    string
	Event string
	Retry time.Duration
	Data  any
}

func Send(c fh.Ctx, ev Event) error {
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")
	c.Type("text/event-stream")
	return c.SendString(Format(ev))
}
func Format(ev Event) string {
	var b strings.Builder
	if ev.ID != "" {
		b.WriteString("id: ")
		b.WriteString(clean(ev.ID))
		b.WriteByte('\n')
	}
	if ev.Event != "" {
		b.WriteString("event: ")
		b.WriteString(clean(ev.Event))
		b.WriteByte('\n')
	}
	if ev.Retry > 0 {
		b.WriteString("retry: ")
		b.WriteString(strconv.Itoa(int(ev.Retry.Milliseconds())))
		b.WriteByte('\n')
	}
	data := stringify(ev.Data)
	for _, line := range strings.Split(data, "\n") {
		b.WriteString("data: ")
		b.WriteString(clean(line))
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}
func Heartbeat() string { return ": heartbeat\n\n" }
func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(x)
		}
		return string(b)
	}
}
func clean(s string) string { return strings.NewReplacer("\r", "", "\x00", "").Replace(s) }
