package securetransport

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestSecureStreamRequestAndResponse(t *testing.T) {
	sessions := kv.NewMemoryStore()
	keys := protocol.SessionKeys{}
	keys.ClientToServer[0], keys.ServerToClient[0] = 17, 29
	now := time.Now()
	session := Session{Keys: keys, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
	session.ID[0], session.DeviceID[0] = 3, 4
	if err := createSession(sessions, session); err != nil {
		t.Fatal(err)
	}
	app := fh.New(fh.WithStreamRequestBody(true), fh.WithMaxRequestBodySize(1<<20))
	_, err := Install(app, Config{AllowEphemeralServerKey: true, RequireSecure: true, SessionStore: sessions, SkipDeviceStatusCheck: true, Limits: protocol.Limits{MaxBody: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	app.Post("/api/stream", func(c fh.Ctx) error {
		var received bytes.Buffer
		if err := c.StreamBody(func(r io.Reader) error {
			_, err := io.Copy(&received, r)
			return err
		}); err != nil {
			return err
		}
		if received.String() != "request body" || c.Get("Authorization") != "Bearer secret" {
			return fh.NewHTTPError(fh.StatusBadRequest, "INVALID_PROTECTED_REQUEST", "protected request was not restored")
		}
		c.Set("X-Protected", "response header")
		return c.Stream(func(w *fh.StreamWriter) error {
			if _, err := w.Write([]byte("reply ")); err != nil {
				return err
			}
			_, err := w.Write([]byte("body"))
			return err
		})
	})
	addr := startApp(t, app)
	meta := protocol.StreamMetadata{SessionID: session.ID, Sequence: 1, IssuedAt: now.UnixMilli(), ExpiresAt: now.Add(30 * time.Second).UnixMilli()}
	meta.RequestID[0], meta.Nonce[0] = 5, 6
	var wire bytes.Buffer
	encoder, err := protocol.NewStreamEncoder(&wire, keys.ClientToServer, meta, protocol.StreamBinding{Method: "POST", Target: "/api/stream"}, protocol.Limits{MaxBody: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	head, err := protocol.EncodeRequestStreamHead(protocol.RequestPayload{ContentType: "text/plain", Headers: []protocol.Header{{Name: "authorization", Value: "Bearer secret"}}}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteHead(head); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write([]byte("request body")); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", "http://"+addr+"/api/stream", bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(protocol.HeaderSecure, "2")
	req.Header.Set(protocol.HeaderStream, base64.RawURLEncoding.EncodeToString(meta.Encode()))
	req.Header.Set("Content-Type", protocol.MediaTypeStream)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get(protocol.HeaderSecure) != "2" || resp.Header.Get("X-Protected") != "" {
		t.Fatalf("unexpected protected response: status=%d headers=%v", resp.StatusCode, resp.Header)
	}
	responseMetaRaw, err := base64.RawURLEncoding.DecodeString(resp.Header.Get(protocol.HeaderStream))
	if err != nil {
		t.Fatal(err)
	}
	responseMeta, err := protocol.DecodeStreamMetadata(responseMetaRaw)
	if err != nil {
		t.Fatal(err)
	}
	if responseMeta.SessionID != meta.SessionID || responseMeta.RequestID != meta.RequestID || responseMeta.Sequence != meta.Sequence {
		t.Fatal("response stream was not bound to the request")
	}
	decoder, err := protocol.NewStreamDecoder(resp.Body, keys.ServerToClient, responseMeta, protocol.StreamBinding{Method: "POST", Target: "/api/stream", Status: resp.StatusCode, Response: true}, protocol.Limits{MaxBody: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	responseHead, err := decoder.ReadHead()
	if err != nil {
		t.Fatal(err)
	}
	protected, err := protocol.DecodeResponseStreamHead(responseHead, protocol.Limits{})
	foundProtected := false
	for _, header := range protected.Headers {
		if header.Name == "x-protected" && header.Value == "response header" {
			foundProtected = true
		}
	}
	if err != nil || protected.Status != http.StatusOK || !foundProtected {
		t.Fatalf("unexpected protected head: %+v err=%v", protected, err)
	}
	body, err := io.ReadAll(decoder)
	if err != nil || string(body) != "reply body" {
		t.Fatalf("unexpected protected body: %q err=%v", body, err)
	}
}
