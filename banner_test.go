package fh

import (
	"bytes"
	"io"
	"net"
	"strings"
	"testing"
)

type bannerAddr string

func (a bannerAddr) Network() string { return "tcp" }
func (a bannerAddr) String() string  { return string(a) }

type bannerListener struct{ addr net.Addr }

func (l bannerListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (l bannerListener) Close() error              { return nil }
func (l bannerListener) Addr() net.Addr            { return l.addr }

func TestRenderStartupBanner(t *testing.T) {
	out := RenderStartupBanner(StartupBannerConfig{ASCIIArt: "-"}, StartupBannerData{
		Name:      "fh",
		Version:   "v1.0.0",
		URL:       "http://127.0.0.1:3000",
		Routes:    3,
		GoVersion: "go1.test",
		PID:       123,
		HTTP2:     true,
		Mode:      ModeProduction,
	})
	for _, want := range []string{"+", "Name", "fh v1.0.0", "URL", "Routes", "3", "HTTP/2", "enabled"} {
		if !strings.Contains(out, want) {
			t.Fatalf("banner missing %q:\n%s", want, out)
		}
	}
}

func TestRenderStartupBannerColor(t *testing.T) {
	out := RenderStartupBanner(StartupBannerConfig{ASCIIArt: "-", Color: true}, StartupBannerData{
		Name:      "fh",
		URL:       "http://127.0.0.1:3000",
		Routes:    3,
		GoVersion: "go1.test",
		PID:       123,
		HTTP2:     true,
		Mode:      ModeDevelopment,
	})
	for _, want := range []string{"\033[", "\033]8;;http://127.0.0.1:3000", "enabled", "development"} {
		if !strings.Contains(out, want) {
			t.Fatalf("colored banner missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, startupReset) {
		t.Fatalf("colored banner missing reset sequence:\n%s", out)
	}
}

func TestRenderStartupBannerColorAlignsRightBorder(t *testing.T) {
	out := RenderStartupBanner(StartupBannerConfig{ASCIIArt: "-", Color: true}, StartupBannerData{
		Name:      "fh",
		URL:       "http://127.0.0.1:3000",
		Routes:    4,
		GoVersion: "go1.26.5",
		PID:       68816,
		HTTP2:     true,
		Mode:      ModeProduction,
	})

	var want int
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "|") && !strings.Contains(line, "+") {
			continue
		}
		got := startupVisibleWidth(line)
		if want == 0 {
			want = got
			continue
		}
		if got != want {
			t.Fatalf("misaligned banner line width=%d want=%d:\n%s", got, want, out)
		}
	}
}

func TestRenderStartupBannerPlainHasNoANSI(t *testing.T) {
	out := RenderStartupBanner(StartupBannerConfig{ASCIIArt: "-"}, StartupBannerData{
		Name:      "fh",
		URL:       "http://127.0.0.1:3000",
		Routes:    3,
		GoVersion: "go1.test",
		PID:       123,
		HTTP2:     true,
		Mode:      ModeProduction,
	})
	if strings.Contains(out, "\033[") || strings.Contains(out, "\033]8;;") {
		t.Fatalf("plain banner should not contain ANSI escapes:\n%q", out)
	}
}

func TestStartupBannerWritesToConfiguredWriter(t *testing.T) {
	var buf bytes.Buffer
	app := New(WithStartupBanner(StartupBannerConfig{Writer: &buf, ASCIIArt: "-", Name: "demo", Version: "v1"}))
	app.Get("/", func(c Ctx) error { return c.SendString("ok") })
	app.printStartupBanner(bannerListener{addr: bannerAddr(":3000")})
	out := buf.String()
	if !strings.Contains(out, "demo v1") || !strings.Contains(out, "http://127.0.0.1:3000") || !strings.Contains(out, "Routes") {
		t.Fatalf("unexpected banner output:\n%s", out)
	}
}

func TestStartupBannerColorDefaultsOnAndCanBeDisabled(t *testing.T) {
	colored := New(WithStartupBannerOutput(io.Discard))
	if !colored.cfg.StartupBanner.Color {
		t.Fatal("startup banner color should default to enabled")
	}

	plain := New(WithStartupBannerOutput(io.Discard), WithStartupBannerColor(false))
	if plain.cfg.StartupBanner.Color {
		t.Fatal("WithStartupBannerColor(false) should disable startup banner color")
	}
}

func TestStartupBannerDisabled(t *testing.T) {
	var buf bytes.Buffer
	app := New(WithStartupBanner(StartupBannerConfig{Writer: &buf, Disabled: true}))
	app.printStartupBanner(bannerListener{addr: bannerAddr(":3000")})
	if buf.Len() != 0 {
		t.Fatalf("expected disabled banner to write nothing, got %q", buf.String())
	}
}

func TestStartupURLNormalizesWildcardAddresses(t *testing.T) {
	cases := map[string]string{
		":8080":        "http://127.0.0.1:8080",
		"0.0.0.0:9000": "http://127.0.0.1:9000",
		"[::]:7000":    "http://127.0.0.1:7000",
	}
	for in, want := range cases {
		if got := startupURL("http", in); got != want {
			t.Fatalf("startupURL(%q)=%q want %q", in, got, want)
		}
	}
}
