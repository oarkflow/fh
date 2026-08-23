package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"

	"github.com/oarkflow/fh"
)

func startTestApp(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Shutdown() })
	go func() { _ = app.Serve(ln) }()
	return "http://" + ln.Addr().String()
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestFlashExampleRendersAndShowsFlash(t *testing.T) {
	app := newApp()
	baseURL := startTestApp(t, app)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(baseURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK || strings.Contains(body, "Internal Server Error") {
		t.Fatalf("index did not render cleanly: status=%d body=%s", resp.StatusCode, body)
	}

	resp, err = client.Post(baseURL+"/items?name=Gadget", "application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = readBody(t, resp)
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("create item status=%d, want %d", resp.StatusCode, http.StatusFound)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "session_id" && cookie.Secure {
			t.Fatal("flash demo sets a Secure session cookie while serving plain HTTP")
		}
	}

	resp, err = client.Get(baseURL + "/items")
	if err != nil {
		t.Fatal(err)
	}
	body = readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("items status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `Item &#34;Gadget&#34; created successfully!`) && !strings.Contains(body, `Item "Gadget" created successfully!`) {
		t.Fatalf("flash message missing from items page: %s", body)
	}
}

func TestFlashExampleShowsDebugErrors(t *testing.T) {
	app := newApp()
	app.Get("/broken", func(c fh.Ctx) error {
		return errors.New("forced internal failure")
	})
	baseURL := startTestApp(t, app)

	resp, err := http.Get(baseURL + "/broken")
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"debug"`) {
		t.Fatalf("debug diagnostics missing from response: %s", body)
	}
}
