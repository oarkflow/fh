package pprof

import (
	"testing"

	"github.com/oarkflow/fh"
)

func TestTrim(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/prefix", "/prefix"},
		{"/prefix/", "/prefix"},
		{"/prefix///", "/prefix"},
		{"/", "/"},
		{"", ""},
	}
	for _, tt := range tests {
		got := trim(tt.input)
		if got != tt.want {
			t.Errorf("trim(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"0", 0},
		{"42", 42},
		{"-1", -1},
		{"abc", 0},
		{"", 0},
	}
	for _, tt := range tests {
		got := parseInt(tt.input)
		if got != tt.want {
			t.Errorf("parseInt(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestStaticToken(t *testing.T) {
	auth := StaticToken("X-Token", "secret123")
	if auth == nil {
		t.Fatal("expected non-nil auth func")
	}
}

func TestStaticTokenRejectsEmptyArgs(t *testing.T) {
	auth := StaticToken("", "")
	if auth(nil) {
		t.Error("expected false for empty header and token")
	}
}

func TestEnableNilApp(t *testing.T) {
	result := Enable(nil, Config{})
	if result != nil {
		t.Error("expected nil for nil app")
	}
}

func TestEnableSetsDefaultPrefix(t *testing.T) {
	app := fh.New()
	result := Enable(app, Config{Auth: func(c fh.Ctx) bool { return true }})
	if result == nil {
		t.Fatal("expected non-nil app")
	}
}

func TestEnableCustomPrefix(t *testing.T) {
	app := fh.New()
	result := Enable(app, Config{
		Prefix: "/custom/pprof/",
		Auth:   func(c fh.Ctx) bool { return true },
	})
	if result == nil {
		t.Fatal("expected non-nil app")
	}
}
