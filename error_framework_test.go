package fh_test

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestProblemDetailsProductionMasksInternalError(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{Environment: fh.EnvProduction})
	app.Get("/", func(c fh.Ctx) error { return errors.New("database password=secret") })
	resp := rawRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "500 Internal Server Error") || !strings.Contains(resp, "application/problem+json") {
		t.Fatalf("unexpected response: %s", resp)
	}
	if strings.Contains(resp, "secret") || strings.Contains(resp, "password") {
		t.Fatalf("production response leaked private detail: %s", resp)
	}
	if !strings.Contains(resp, "INTERNAL_ERROR") {
		t.Fatalf("missing stable error code: %s", resp)
	}
}

func TestProblemDetailsDevelopmentIncludesRedactedDebug(t *testing.T) {
	app := fh.NewWithConfig(fh.Config{Environment: fh.EnvDevelopment, ErrorOptions: fh.ErrorOptions{Environment: fh.EnvDevelopment, ExposeCauses: true}})
	app.Get("/", func(c fh.Ctx) error { return errors.New("database password=secret") })
	resp := rawRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "debug") || !strings.Contains(resp, "[REDACTED]") {
		t.Fatalf("debug response missing redacted diagnostic detail: %s", resp)
	}
	if strings.Contains(resp, "password=secret") {
		t.Fatalf("debug response leaked raw secret: %s", resp)
	}
}

func TestValidationProblemDetails(t *testing.T) {
	app := fh.New()
	app.Get("/", func(c fh.Ctx) error {
		return &fh.ValidationError{Fields: []fh.FieldError{{Field: "email", Code: "required", Message: "email is required"}}}
	})
	resp := rawRequest(t, app, "GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
	if !strings.Contains(resp, "422 Unprocessable Entity") || !strings.Contains(resp, "VALIDATION_FAILED") || !strings.Contains(resp, "email") {
		t.Fatalf("unexpected validation response: %s", resp)
	}
}

func rawRequest(t *testing.T, app *fh.App, request string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer app.Shutdown()
	go app.Serve(ln)
	time.Sleep(10 * time.Millisecond)
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	conn.(*net.TCPConn).CloseWrite()
	b, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
