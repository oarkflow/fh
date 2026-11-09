package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestQuickstart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := run(ctx, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "reply: echo: hello over MIRA") {
		t.Fatalf("unexpected output:\n%s", out.String())
	}
}
