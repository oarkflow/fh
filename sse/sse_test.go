package sse

import (
	"strings"
	"testing"
	"time"
)

func TestFormatEvent(t *testing.T) {
	out := Format(Event{ID: "1", Event: "ready", Retry: time.Second, Data: map[string]any{"ok": true}})
	for _, want := range []string{"id: 1", "event: ready", "retry: 1000", "data:"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in %q", want, out)
		}
	}
}
