package bodylimit

import (
	"testing"

	"github.com/oarkflow/fh"
)

func TestNewRejectsZeroLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero limit")
		}
	}()
	New(0)
}

func TestNewRejectsNegativeLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for negative limit")
		}
	}()
	New(-1)
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0 byte"},
		{1, "1 byte"},
		{1023, "1023 byte"},
		{1024, "1 KiB"},
		{2048, "2 KiB"},
		{1048576, "1 MiB"},
	}
	for _, tt := range tests {
		got := formatBytes(tt.n)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestWithConfigRejectsZeroLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero limit in WithConfig")
		}
	}()
	WithConfig(Config{Limit: 0})
}

func TestNewDoesNotPanic(t *testing.T) {
	_ = New(1024)
}

func TestWithConfigDoesNotPanic(t *testing.T) {
	_ = WithConfig(Config{Limit: 1024})
}

func TestBodyLimitSkippedWhenNextReturnsTrue(t *testing.T) {
	handler := WithConfig(Config{
		Limit: 5,
		Next:  func(_ fh.Ctx) bool { return true },
	})
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{123, "123"},
		{9999, "9999"},
	}
	for _, tt := range tests {
		got := itoa(tt.n)
		if got != tt.want {
			t.Errorf("itoa(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
