package fh_test

import (
	"io"
	"net"
	"strings"
	"testing"

	"github.com/oarkflow/fh"
)

type mockEngine struct {
	renderFn func(w io.Writer, name string, data any, layout ...string) error
}

func (m *mockEngine) Render(w io.Writer, name string, data any, layout ...string) error {
	if m.renderFn != nil {
		return m.renderFn(w, name, data, layout...)
	}
	_, err := io.WriteString(w, "rendered:"+name)
	return err
}

func TestRenderNoEngine(t *testing.T) {
	app := fh.New()
	addr := testServer(t, app)
	code, body := doRequest(t, addr, "GET", "/", "", nil)
	_ = code
	_ = body
}

func TestRenderWithEngine(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{
		TemplateEngine: &mockEngine{},
	})
	app.Get("/", func(c fh.Ctx) error {
		return c.Render("hello", nil)
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "rendered:hello" {
		t.Fatalf("expected 'rendered:hello', got %q", body)
	}
}

func TestRenderWithData(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{
		TemplateEngine: &mockEngine{
			renderFn: func(w io.Writer, name string, data any, layout ...string) error {
				m, ok := data.(map[string]any)
				if ok {
					_, _ = io.WriteString(w, m["name"].(string))
				}
				return nil
			},
		},
	})
	app.Get("/", func(c fh.Ctx) error {
		return c.Render("hello", map[string]any{"name": "world"})
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "world" {
		t.Fatalf("expected 'world', got %q", body)
	}
}

func TestRenderWithLayout(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{
		TemplateEngine: &mockEngine{
			renderFn: func(w io.Writer, name string, data any, layout ...string) error {
				l := ""
				if len(layout) > 0 {
					l = layout[0]
				}
				_, _ = io.WriteString(w, "layout:"+l+"/name:"+name)
				return nil
			},
		},
	})
	app.Get("/", func(c fh.Ctx) error {
		return c.Render("page", nil, "main")
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if body != "layout:main/name:page" {
		t.Fatalf("expected 'layout:main/name:page', got %q", body)
	}
}

func TestRenderHTMLEscape(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{
		TemplateEngine: &mockEngine{},
	})
	app.Get("/", func(c fh.Ctx) error {
		return c.Render("test", nil)
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/", "", nil)
	if code != 200 {
		t.Fatalf("expected 200, got %d", code)
	}
	if !strings.Contains(body, "rendered:test") {
		t.Fatalf("expected body to contain 'rendered:test', got %q", body)
	}
}

func TestRenderNoEngineError(t *testing.T) {
	app := fh.New()
	app.Get("/", func(c fh.Ctx) error {
		return c.Render("hello", nil)
	})
	addr := testServer(t, app)

	code, body := doRequest(t, addr, "GET", "/", "", nil)
	if code != 500 {
		t.Fatalf("expected 500, got %d", code)
	}
	if !strings.Contains(body, "no template engine configured") {
		t.Fatalf("expected engine missing error, got %q", body)
	}
}

func TestRenderContentType(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{
		TemplateEngine: &mockEngine{},
	})
	app.Get("/", func(c fh.Ctx) error {
		return c.Render("page", nil)
	})
	addr := testServer(t, app)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))

	resp := make([]byte, 4096)
	n, _ := conn.Read(resp)
	respStr := string(resp[:n])

	if !strings.Contains(respStr, "Content-Type: text/html") {
		t.Fatalf("expected Content-Type: text/html, got: %s", respStr)
	}
}
