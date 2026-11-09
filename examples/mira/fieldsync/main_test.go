package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestFieldSyncSurvivesCrashWithoutLossOrDuplicates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out, t.TempDir()); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		"replayed 4 events in order, checkpoint now 4: [temp=21.5C temp=22.5C temp=23.5C temp=24.5C]",
		"retry of reading-0002 returned the same cursor",
		"second replay delivered 0 events",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}
