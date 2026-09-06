package fh

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

func TestStreamHonorsDateHeaderConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		date bool
	}{
		{name: "disabled", date: false},
		{name: "enabled", date: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var app *App
			if tc.date {
				app = New()
			} else {
				app = NewFast()
			}
			app.Get("/", func(c Ctx) error {
				return c.Stream(func(w *StreamWriter) error {
					_, err := w.Write([]byte("streamed"))
					return err
				})
			})
			resp := pipeRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
			hasDate := strings.Contains(resp, "\r\nDate: ")
			if hasDate != tc.date {
				t.Fatalf("Date header present = %v, want %v: %q", hasDate, tc.date, resp)
			}
		})
	}
}

func TestStreamHonorsKeepAliveHeaderConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		keep bool
		want string
	}{
		{name: "implicit", keep: false, want: ""},
		{name: "explicit", keep: true, want: "Connection: keep-alive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := New(WithSendKeepAliveHeader(tc.keep))
			app.Get("/", func(c Ctx) error {
				return c.StreamLength(1, func(w *StreamWriter) error {
					_, err := w.Write([]byte("x"))
					return err
				})
			})
			raw := readStreamHeaders(t, app, "Connection: keep-alive")
			if strings.Contains(raw, "Connection: keep-alive") != tc.keep {
				t.Fatalf("keep-alive header mismatch: %q", raw)
			}
			if tc.want != "" && !strings.Contains(raw, tc.want) {
				t.Fatalf("response missing %q: %q", tc.want, raw)
			}
		})
	}
	closeApp := New()
	closeApp.Get("/", func(c Ctx) error {
		return c.StreamLength(1, func(w *StreamWriter) error {
			_, err := w.Write([]byte("x"))
			return err
		})
	})
	raw := readStreamHeaders(t, closeApp, "Connection: close")
	if !strings.Contains(raw, "Connection: close") {
		t.Fatalf("stream response did not reflect request close: %q", raw)
	}
}

func readStreamHeaders(t *testing.T, app *App, connection string) string {
	t.Helper()
	client := runPipeApp(t, app)
	go func() {
		_, _ = io.WriteString(client, "GET / HTTP/1.1\r\nHost: local\r\n"+connection+"\r\n\r\n")
	}()
	reader := bufio.NewReader(client)
	var header strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		header.WriteString(line)
		if line == "\r\n" {
			break
		}
	}
	if _, err := io.ReadFull(reader, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	return header.String()
}
