package gateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh/delegation"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

func TestBrowserGatewayRequiresDelegation(t *testing.T) {
	_, e := New(Config{Authenticate: func(*http.Request) (string, error) { return "alice", nil }, DialBackend: func(context.Context, string) (*mira.Session, error) { return nil, errors.New("no backend") }, RequirePrincipalBinding: true})
	if e == nil {
		t.Fatal("JWT multi-tenant gateway permitted without backend binding")
	}
}
func TestBrowserPrincipalActuallyBoundToEncryptedUDPSession(t *testing.T) {
	psk := bytes.Repeat([]byte{0x72}, 32)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := mira.Listen("127.0.0.1:0", psk, mira.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	hold := make(chan struct{})
	defer close(hold)
	go func() {
		ss, e := backend.Accept(ctx)
		if e != nil {
			finished <- e
			return
		}
		defer ss.Close(context.Background())
		ch, e := ss.AcceptChannel(ctx)
		if e != nil {
			finished <- e
			return
		}
		verifier := &delegation.Verifier{Audience: "mira-upstream", PublicKey: pub}
		subject, e := verifier.Accept(ctx, ch, ss.ID())
		if e != nil {
			finished <- e
			return
		}
		m, e := ch.Receive(ctx)
		if e != nil {
			finished <- e
			return
		}
		id, e := ch.Send(ctx, []byte(subject+":"+string(m.Payload)))
		if e == nil {
			_, e = ch.AwaitReceipt(ctx, id, protocol.AckTransport)
		}
		finished <- e
		select {
		case <-hold:
		case <-ctx.Done():
		}
	}()
	gw, err := New(Config{
		Authenticate: func(r *http.Request) (string, error) {
			if r.Header.Get("Authorization") != "Bearer valid" {
				return "", ErrJWT
			}
			return "alice", nil
		},
		DialBackend: func(ctx context.Context, _ string) (*mira.Session, error) {
			return mira.Dial(ctx, backend.Addr().String(), psk, mira.Options{})
		},
		BindPrincipal: func(ctx context.Context, ss *mira.Session, ch protocol.Channel, owner string) error {
			return delegation.Prove(ctx, ch, ss.ID(), priv, "mira-upstream", owner)
		},
		RequirePrincipalBinding: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer gw.Close()
	api := httptest.NewTLSServer(gw.Handler())
	defer api.Close()
	call := func(method, path, body string) (*http.Response, error) {
		req, e := http.NewRequestWithContext(ctx, method, api.URL+path, strings.NewReader(body))
		if e != nil {
			return nil, e
		}
		req.Header.Set("Authorization", "Bearer valid")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		return api.Client().Do(req)
	}
	res, err := call("POST", "/v1/connect", "{}")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("connect status=%d body=%s", res.StatusCode, b)
	}
	var connected struct {
		Session string `json:"session"`
	}
	if err := json.NewDecoder(res.Body).Decode(&connected); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	payload := base64.StdEncoding.EncodeToString([]byte("hello"))
	request, _ := json.Marshal(map[string]string{"session": connected.Session, "data": payload})
	res, err = call("POST", "/v1/send", string(request))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("send status=%d", res.StatusCode)
	}
	res, err = call("GET", "/v1/pull?session="+connected.Session+"&cursor=0", "")
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("pull %d %s", res.StatusCode, b)
	}
	var out struct {
		Events []Event `json:"events"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if len(out.Events) != 1 {
		t.Fatalf("events=%v", out.Events)
	}
	raw, err := base64.StdEncoding.DecodeString(out.Events[0].Data)
	if err != nil || string(raw) != "alice:hello" {
		t.Fatalf("bound principal not preserved %q %v", raw, err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}
