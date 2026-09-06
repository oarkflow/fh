package app

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type pipeHarness struct {
	listener *pipeListener
	app      interface {
		Serve(net.Listener) error
		ShutdownWithTimeout(time.Duration) error
	}
}

type pipeListener struct {
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
}

func newPipeHarness(application interface {
	Serve(net.Listener) error
	ShutdownWithTimeout(time.Duration) error
}) *pipeHarness {
	listener := &pipeListener{connections: make(chan net.Conn), done: make(chan struct{})}
	harness := &pipeHarness{listener: listener, app: application}
	go func() { _ = application.Serve(listener) }()
	return harness
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}
func (l *pipeListener) Addr() net.Addr { return &net.IPAddr{} }

func (h *pipeHarness) Do(request *http.Request) (*http.Response, error) {
	client, server := net.Pipe()
	select {
	case h.listener.connections <- server:
	case <-h.listener.done:
		return nil, net.ErrClosed
	}
	request.Close = true
	if request.Header.Get("Host") == "" {
		request.Header.Set("Host", "localhost")
	}
	if err := request.Write(client); err != nil {
		_ = client.Close()
		return nil, err
	}
	response, err := http.ReadResponse(bufio.NewReader(client), request)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	_ = client.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}

func (h *pipeHarness) Close() {
	_ = h.listener.Close()
	_ = h.app.ShutdownWithTimeout(time.Second)
}

func TestCompleteApplicationFlow(t *testing.T) {
	t.Setenv("APP_BOOTSTRAP_PASSWORD", "correct horse battery staple")
	t.Setenv("APP_DATABASE_DSN", "file:"+filepath.Join(t.TempDir(), "app.db")+"?_pragma=foreign_keys(1)")
	t.Setenv("APP_ALLOWED_HOSTS", "localhost,127.0.0.1,::1")
	server, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	harness := newPipeHarness(server.app)
	defer harness.Close()

	cookies := make(map[string]*http.Cookie)
	do := func(method, path, body string, headers map[string]string, withCookies bool) (int, []byte, http.Header) {
		t.Helper()
		req := httptest.NewRequest(method, "http://localhost"+path, strings.NewReader(body))
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		if withCookies {
			for _, cookie := range cookies {
				req.AddCookie(cookie)
			}
		}
		resp, err := harness.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		for _, cookie := range resp.Cookies() {
			if cookie.MaxAge < 0 {
				delete(cookies, cookie.Name)
			} else {
				cookies[cookie.Name] = cookie
			}
		}
		return resp.StatusCode, data, resp.Header
	}

	status, body, _ := do(http.MethodGet, "/healthz", "", nil, false)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"status":"UP"`)) {
		t.Fatalf("health: status=%d body=%s", status, body)
	}
	status, body, _ = do(http.MethodGet, "/readyz", "", nil, false)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"status":"ready"`)) {
		t.Fatalf("readiness: status=%d body=%s", status, body)
	}
	status, body, headers := do(http.MethodGet, "/v1/protected", "", nil, false)
	if status != http.StatusUnauthorized || headers.Get("X-Request-ID") == "" || !bytes.Contains(body, []byte(`"request_id"`)) {
		t.Fatalf("anonymous protection: status=%d body=%s", status, body)
	}

	status, body, _ = do(http.MethodGet, "/v1/auth/csrf", "", nil, true)
	var csrfResponse struct {
		Token string `json:"csrf_token"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &csrfResponse) != nil || csrfResponse.Token == "" {
		t.Fatalf("csrf bootstrap: status=%d body=%s", status, body)
	}
	csrfHeaders := map[string]string{"Content-Type": "application/json", "X-CSRF-Token": csrfResponse.Token, "Origin": "http://localhost"}
	status, body, _ = do(http.MethodPost, "/v1/auth/login", `{"username":"admin","password":"correct horse battery staple"}`, csrfHeaders, true)
	var login struct {
		Tokens struct {
			AccessToken string `json:"access_token"`
		} `json:"tokens"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &login) != nil || login.Tokens.AccessToken == "" {
		t.Fatalf("login: status=%d body=%s", status, body)
	}
	status, body, _ = do(http.MethodGet, "/v1/auth/me", "", nil, true)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"roles":["developer"]`)) {
		t.Fatalf("session identity: status=%d body=%s", status, body)
	}
	status, body, _ = do(http.MethodGet, "/v1/protected", "", nil, true)
	if status != http.StatusOK {
		t.Fatalf("session RBAC: status=%d body=%s", status, body)
	}
	status, body, _ = do(http.MethodGet, "/v1/protected", "", map[string]string{"Authorization": "Bearer " + login.Tokens.AccessToken}, false)
	if status != http.StatusOK {
		t.Fatalf("bearer RBAC: status=%d body=%s", status, body)
	}

	status, body, _ = do(http.MethodPost, "/v1/example", `{"name":"persisted"}`, map[string]string{"Content-Type": "application/json"}, false)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"name":"persisted"`)) {
		t.Fatalf("database insert: status=%d body=%s", status, body)
	}
	status, body, _ = do(http.MethodGet, "/v1/example", "", nil, false)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"name":"persisted"`)) {
		t.Fatalf("database list: status=%d body=%s", status, body)
	}

	status, body, _ = do(http.MethodGet, "/form", "", nil, true)
	formToken := ""
	if cookie := cookies["csrf_token"]; cookie != nil {
		formToken = cookie.Value
	}
	if status != http.StatusOK || formToken == "" || !bytes.Contains(body, []byte("csrf_token="+formToken)) {
		t.Fatalf("form render: status=%d body=%s", status, body)
	}
	status, body, _ = do(http.MethodPost, "/form?csrf_token="+url.QueryEscape(formToken), "name=browser", map[string]string{"Content-Type": "application/x-www-form-urlencoded", "Origin": "http://localhost"}, true)
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"ok":true`)) {
		t.Fatalf("form submit: status=%d body=%s", status, body)
	}

	status, body, _ = do(http.MethodPost, "/v1/auth/logout", "", csrfHeaders, true)
	if status != http.StatusOK {
		t.Fatalf("logout: status=%d body=%s", status, body)
	}
	status, _, _ = do(http.MethodGet, "/v1/auth/me", "", nil, true)
	if status != http.StatusUnauthorized {
		t.Fatalf("session remained authenticated after logout: status=%d", status)
	}
}
