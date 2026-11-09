package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/oarkflow/fh/mira"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBrowserBidirectionalBridge(t *testing.T) {
	key := bytes.Repeat([]byte{18}, 32)
	backend, err := mira.Listen("127.0.0.1:0", key, mira.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s, err := backend.Accept(ctx)
		if err != nil {
			done <- err
			return
		}
		defer s.Close(context.Background())
		ch, err := s.AcceptChannel(ctx)
		if err != nil {
			done <- err
			return
		}
		for {
			msg, err := ch.Receive(ctx)
			if err != nil {
				done <- nil
				return
			}
			if _, err := ch.Send(ctx, append([]byte("echo:"), msg.Payload...)); err != nil {
				done <- err
				return
			}
		}
	}()
	gw, err := New(Config{Authenticate: func(r *http.Request) (string, error) {
		v := r.Header.Get("X-User")
		if v != "person-1" {
			return "", errors.New("denied")
		}
		return v, nil
	}, DialBackend: func(ctx context.Context, owner string) (*mira.Session, error) {
		return mira.Dial(ctx, backend.Addr().String(), key, mira.Options{})
	}, History: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()
	h := httptest.NewTLSServer(gw.Handler())
	defer h.Close()
	client := h.Client()
	client.Timeout = 6 * time.Second
	// A real client verifies the gateway certificate; httptest trusts its own TLS test cert.
	_ = tls.VersionTLS13
	request := func(method, path, body string, user string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, h.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-User", user)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := request("POST", "/v1/connect", "{}", "person-1")
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("connect %d %s", res.StatusCode, b)
	}
	var connected struct {
		Session string `json:"session"`
	}
	if err := json.NewDecoder(res.Body).Decode(&connected); err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if connected.Session == "" {
		t.Fatal("empty token")
	}
	token := connected.Session
	payload := base64.StdEncoding.EncodeToString([]byte("hello browser"))
	body, _ := json.Marshal(map[string]string{"session": token, "data": payload})
	res = request("POST", "/v1/send", string(body), "person-1")
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("send %d %s", res.StatusCode, b)
	}
	_ = res.Body.Close()
	res = request("GET", "/v1/pull?session="+token+"&cursor=0", "", "person-1")
	if res.StatusCode != 200 {
		t.Fatalf("pull %d", res.StatusCode)
	}
	var events struct {
		Events []Event `json:"events"`
	}
	if err := json.NewDecoder(res.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if len(events.Events) != 1 {
		t.Fatalf("events %+v", events.Events)
	}
	raw, err := base64.StdEncoding.DecodeString(events.Events[0].Data)
	if err != nil || string(raw) != "echo:hello browser" {
		t.Fatalf("response %q %v", raw, err)
	}
	res = request("GET", "/v1/pull?session="+token+"&cursor=0", "", "evil")
	if res.StatusCode != 401 {
		t.Fatalf("principal isolation %d", res.StatusCode)
	}
	_ = res.Body.Close()
	res = request("DELETE", "/v1/connect?session="+token, "", "person-1")
	if res.StatusCode != 204 {
		t.Fatalf("disconnect %d", res.StatusCode)
	}
	_ = res.Body.Close()
}

func TestOriginPolicyAndPerPrincipalQuota(t *testing.T) {
	gw, err := New(Config{
		Authenticate: func(r *http.Request) (string, error) { return "person-1", nil },
		DialBackend: func(ctx context.Context, owner string) (*mira.Session, error) {
			return nil, errors.New("not reachable")
		},
		AllowedOrigins:          []string{"https://app.example.test"},
		AllowPlaintextLoopback:  true,
		MaxSessionsPerPrincipal: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()
	handler := gw.Handler()
	cases := []struct {
		origin, site string
		method       string
		want         int
	}{
		{"https://evil.example.test", "cross-site", "POST", 403},
		{"", "cross-site", "POST", 403},
		{"https://app.example.test", "cross-site", "OPTIONS", 204},
		{"", "", "OPTIONS", 403},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, "http://localhost/v1/connect", strings.NewReader("{}"))
		req.RemoteAddr = "127.0.0.1:12345"
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("Sec-Fetch-Site", tc.site)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != tc.want {
			t.Fatalf("origin %q site %q method %s: %d != %d", tc.origin, tc.site, tc.method, rr.Code, tc.want)
		}
	}
	gw.mu.Lock()
	gw.sessions["a"] = &browserSession{owner: "person-1"}
	if gw.capacityLocked("person-1") || !gw.capacityLocked("person-2") {
		t.Fatal("per-principal capacity isolation failed")
	}
	delete(gw.sessions, "a")
	gw.mu.Unlock()
}

func TestRejectWildcardOrigins(t *testing.T) {
	_, err := New(Config{Authenticate: func(*http.Request) (string, error) { return "x", nil }, DialBackend: func(context.Context, string) (*mira.Session, error) { return nil, nil }, AllowedOrigins: []string{"https://*.example.test"}})
	if err == nil {
		t.Fatal("wildcard origin must be rejected")
	}
}
