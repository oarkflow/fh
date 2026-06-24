package fh

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func pipeRequest(t *testing.T, app *App, request string) string {
	t.Helper()
	client := runPipeApp(t, app)
	go func() {
		_, _ = io.WriteString(client, request)
	}()
	response, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	return string(response)
}

func TestMultipartFormFileAndAtomicSave(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "saved.txt")
	app := New()
	app.Post("/upload", func(c Ctx) error {
		file, err := c.FormFile("document")
		if err != nil {
			return err
		}
		if file.FileName != "hello.txt" || file.Size != 7 {
			return errors.New("bad upload metadata")
		}
		if err := c.SaveFile(file, dst); err != nil {
			return err
		}
		return c.SendString("saved")
	})
	body := "--b\r\nContent-Disposition: form-data; name=\"document\"; filename=\"hello.txt\"\r\nContent-Type: text/plain\r\n\r\npayload\r\n--b--\r\n"
	resp := pipeRequest(t, app, "POST /upload HTTP/1.1\r\nHost: local\r\nContent-Type: multipart/form-data; boundary=b\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\nConnection: close\r\n\r\n"+body)
	if !strings.Contains(resp, "200 OK") || !strings.HasSuffix(resp, "saved") {
		t.Fatalf("unexpected response: %s", resp)
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "payload" {
		t.Fatalf("saved file = %q, %v", got, err)
	}
	info, _ := os.Stat(dst)
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
}

func TestDownloadAndRange(t *testing.T) {
	file := filepath.Join(t.TempDir(), "source.txt")
	if err := os.WriteFile(file, []byte("0123456789"), 0644); err != nil {
		t.Fatal(err)
	}
	app := New()
	app.Get("/download", func(c Ctx) error { return c.Download(file, "report 2026.txt") })
	resp := pipeRequest(t, app, "GET /download HTTP/1.1\r\nHost: local\r\nRange: bytes=2-5\r\nConnection: close\r\n\r\n")
	for _, want := range []string{"206 Partial Content", "Content-Disposition: attachment; filename=\"report 2026.txt\"", "Content-Range: bytes 2-5/10", "\r\n\r\n2345"} {
		if !strings.Contains(resp, want) {
			t.Fatalf("missing %q in %s", want, resp)
		}
	}
}

func TestTypedAndValidationProblemResponses(t *testing.T) {
	app := New()
	app.Get("/typed", func(Ctx) error {
		e := Conflict("Version already exists")
		e.Headers = map[string]string{"Retry-After": "2"}
		return e
	})
	resp := pipeRequest(t, app, "GET /typed HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "409 Conflict") || !strings.Contains(resp, "application/problem+json") || !strings.Contains(resp, `"code":"CONFLICT"`) || !strings.Contains(resp, "Retry-After: 2") {
		t.Fatalf("unexpected typed response: %s", resp)
	}
	if app.ErrorCount("CONFLICT") != 1 {
		t.Fatalf("conflict metric = %d", app.ErrorCount("CONFLICT"))
	}
	validationApp := New()
	validationApp.Get("/validation", func(Ctx) error {
		return &ValidationError{Fields: []FieldError{{Field: "email", Code: "FORMAT", Message: "must be an email"}}}
	})
	resp = pipeRequest(t, validationApp, "GET /validation HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "422 Unprocessable Entity") || !strings.Contains(resp, `"errors":[{"field":"email"`) {
		t.Fatalf("unexpected validation response: %s", resp)
	}
}

func TestInternalErrorsMaskedUnlessDebug(t *testing.T) {
	secret := "database password is secret"
	for _, tc := range []struct{ debug, exposed bool }{{false, false}, {true, true}} {
		app := NewWithConfig(Config{Debug: tc.debug})
		app.Get("/", func(Ctx) error { return errors.New(secret) })
		resp := pipeRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
		if bytes.Contains([]byte(resp), []byte(secret)) != tc.exposed {
			t.Fatalf("debug=%v response=%s", tc.debug, resp)
		}
	}
}
