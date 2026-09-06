package web

import (
	"bytes"
	"strings"
	"testing"
)

func TestSPLTemplatesRender(t *testing.T) {
	engine := NewEngine()
	var out bytes.Buffer
	if err := engine.Render(&out, "index.html", map[string]any{"Name": "demo"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "<h1>demo</h1>") {
		t.Fatalf("unexpected SPL output: %s", out.String())
	}
	out.Reset()
	if err := engine.Render(&out, "form.html", map[string]any{"Token": "csrf-test-token"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `content="csrf-test-token"`) {
		t.Fatalf("unexpected form output: %s", out.String())
	}
	if !strings.Contains(out.String(), `action="/form?csrf_token=csrf-test-token"`) || strings.Contains(out.String(), "<script") {
		t.Fatalf("form is not SPL secure-mode compatible: %s", out.String())
	}
}
