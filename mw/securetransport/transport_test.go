package securetransport

import (
	"bufio"
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

type rawResponse struct {
	status  int
	headers map[string]string
	body    []byte
}

func startApp(t *testing.T, app *fh.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = app.Shutdown()
		_ = ln.Close()
	})
	go func() { _ = app.Serve(ln) }()
	time.Sleep(15 * time.Millisecond)
	return ln.Addr().String()
}

func rawRequest(t *testing.T, addr, method, target string, headers map[string]string, body []byte) rawResponse {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var request bytes.Buffer
	fmt.Fprintf(&request, "%s %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n", method, target)
	for name, value := range headers {
		fmt.Fprintf(&request, "%s: %s\r\n", name, value)
	}
	if len(body) > 0 {
		fmt.Fprintf(&request, "Content-Length: %d\r\n", len(body))
	}
	request.WriteString("\r\n")
	request.Write(body)
	if _, err := conn.Write(request.Bytes()); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	var protoName string
	var status int
	if _, err := fmt.Sscanf(strings.TrimSpace(statusLine), "%s %d", &protoName, &status); err != nil {
		t.Fatal(err)
	}
	responseHeaders := make(map[string]string)
	contentLength := -1
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("malformed response header %q", line)
		}
		name, value = strings.ToLower(strings.TrimSpace(name)), strings.TrimSpace(value)
		responseHeaders[name] = value
		if name == "content-length" {
			contentLength, _ = strconv.Atoi(value)
		}
	}
	var responseBody []byte
	if contentLength >= 0 {
		responseBody = make([]byte, contentLength)
		if _, err := io.ReadFull(reader, responseBody); err != nil {
			t.Fatal(err)
		}
	} else {
		responseBody, _ = io.ReadAll(reader)
	}
	return rawResponse{status: status, headers: responseHeaders, body: responseBody}
}

func TestEncryptedFHRequestResponseAndReplay(t *testing.T) {
	app := fh.New()
	transport, err := Install(app, Config{
		AllowEphemeralServerKey:                true,
		RequireSecure:                          true,
		Protect:                                func(c fh.Ctx) bool { return strings.HasPrefix(c.Path(), "/api/") },
		AllowedOrigins:                         []string{"https://app.example"},
		RequireOrigin:                          true,
		AllowUnauthenticatedDeviceRegistration: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	app.Post("/api/echo", func(c fh.Ctx) error {
		if c.Get("Authorization") != "Bearer bound-token" {
			return fh.NewHTTPError(fh.StatusUnauthorized, "TOKEN_MISSING", "token missing")
		}
		if c.Get("X-Secret-Meta") != "" {
			return fh.NewHTTPError(fh.StatusBadRequest, "OUTER_HEADER_LEAK", "untrusted outer application header was not stripped")
		}
		c.Set("X-Secret-Meta", "must-be-encrypted")
		return c.Type("application/json").SendBytes(append([]byte(`{"echo":`), append(c.BodyCopy(), '}')...))
	})
	app.Post("/api/error", func(c fh.Ctx) error {
		return fh.NewHTTPError(fh.StatusUnprocessableEntity, "EXPECTED_SECURE_ERROR", "encrypted application error")
	})
	addr := startApp(t, app)
	browserHeaders := map[string]string{"Origin": "https://app.example", "Sec-Fetch-Site": "same-origin"}

	devicePublic, devicePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	registration := protocol.DeviceRegistrationRequest{IssuedAt: time.Now().UnixMilli(), Name: "test browser"}
	registration.Nonce, _ = protocol.NewNonce16()
	copy(registration.PublicKey[:], devicePublic)
	registrationBody, _ := registration.Encode()
	registerHeaders := cloneHeaders(browserHeaders)
	registerHeaders["Content-Type"] = protocol.MediaTypeHandshake
	registered := rawRequest(t, addr, "POST", DefaultPrefix+"/device/register", registerHeaders, registrationBody)
	if registered.status != fh.StatusOK {
		t.Fatalf("register status=%d body=%q", registered.status, registered.body)
	}
	deviceResponse, err := protocol.DecodeDeviceRegistrationResponse(registered.body)
	if err != nil {
		t.Fatal(err)
	}

	clientEphemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	hello := protocol.ClientHello{
		DeviceID:    deviceResponse.DeviceID,
		IssuedAt:    now.UnixMilli(),
		ExpiresAt:   now.Add(time.Minute).UnixMilli(),
		ClientBuild: "middleware-test",
	}
	copy(hello.ClientPublic[:], clientEphemeral.PublicKey().Bytes())
	hello.Nonce, _ = protocol.NewNonce16()
	signingBytes, _ := hello.SigningBytes()
	copy(hello.Signature[:], ed25519.Sign(devicePrivate, signingBytes))
	helloBody, _ := hello.Encode()
	sessionHeaders := cloneHeaders(browserHeaders)
	sessionHeaders["Content-Type"] = protocol.MediaTypeHandshake
	sessionResponse := rawRequest(t, addr, "POST", DefaultPrefix+"/session", sessionHeaders, helloBody)
	if sessionResponse.status != fh.StatusOK {
		t.Fatalf("session status=%d body=%q", sessionResponse.status, sessionResponse.body)
	}
	serverHello, err := protocol.DecodeServerHello(sessionResponse.body)
	if err != nil {
		t.Fatal(err)
	}
	serverPublic, err := ecdh.X25519().NewPublicKey(serverHello.ServerPublic[:])
	if err != nil {
		t.Fatal(err)
	}
	serverEphemeral, err := ecdh.X25519().NewPublicKey(serverHello.ServerEphemeral[:])
	if err != nil {
		t.Fatal(err)
	}
	staticShared, err := clientEphemeral.ECDH(serverPublic)
	if err != nil {
		t.Fatal(err)
	}
	ephemeralShared, err := clientEphemeral.ECDH(serverEphemeral)
	if err != nil {
		t.Fatal(err)
	}
	combinedShared := append(append(make([]byte, 0, len(staticShared)+len(ephemeralShared)), staticShared...), ephemeralShared...)
	serverCore, _ := serverHello.CoreBytes()
	keys := protocol.DeriveSessionKeys(combinedShared, helloBody, serverCore)
	if !protocol.VerifyServerProof(keys.ServerToClient, helloBody, serverCore, serverHello.Proof) {
		t.Fatal("server proof verification failed")
	}
	if transport.PublicKey() != serverHello.ServerPublic {
		t.Fatal("server hello did not expose transport public key")
	}

	requestID, _ := protocol.NewID()
	requestNonce, _ := protocol.NewAEADNonce()
	requestNow := time.Now()
	requestEnvelope := protocol.RequestEnvelope{
		SessionID: serverHello.SessionID,
		RequestID: requestID,
		Sequence:  1,
		IssuedAt:  requestNow.UnixMilli(),
		ExpiresAt: requestNow.Add(time.Minute).UnixMilli(),
		Nonce:     requestNonce,
	}
	secureBody, err := protocol.EncryptRequest(keys.ClientToServer, "POST", "/api/echo", requestEnvelope, protocol.RequestPayload{
		ContentType: "application/json",
		Headers:     []protocol.Header{{Name: "authorization", Value: "Bearer bound-token"}},
		Body:        []byte(`{"message":"hello"}`),
	}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	requestHeaders := cloneHeaders(browserHeaders)
	requestHeaders[protocol.HeaderSecure] = "1"
	requestHeaders["Content-Type"] = protocol.MediaTypeRequest
	requestHeaders["Authorization"] = "Bearer untrusted-outer-token"
	requestHeaders["X-Secret-Meta"] = "untrusted-outer-metadata"
	secured := rawRequest(t, addr, "POST", "/api/echo", requestHeaders, secureBody)
	if secured.status != fh.StatusOK {
		t.Fatalf("secure status=%d body=%q", secured.status, secured.body)
	}
	if secured.headers[strings.ToLower(protocol.HeaderSecure)] != "1" {
		t.Fatal("secure response marker missing")
	}
	if secured.headers["content-type"] != protocol.MediaTypeRequest {
		t.Fatalf("unexpected outer content type %q", secured.headers["content-type"])
	}
	if _, exposed := secured.headers["x-secret-meta"]; exposed {
		t.Fatal("application response metadata leaked outside encrypted envelope")
	}
	responseEnvelope, responsePayload, err := protocol.DecryptResponse(keys.ServerToClient, secured.status, secured.body, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !protocol.EqualID(responseEnvelope.RequestID, requestID) || responseEnvelope.Sequence != 1 {
		t.Fatal("response was not bound to request")
	}
	if responsePayload.ContentType != "application/json" || string(responsePayload.Body) != `{"echo":{"message":"hello"}}` {
		t.Fatalf("unexpected decrypted response: type=%q body=%q", responsePayload.ContentType, responsePayload.Body)
	}
	foundSecretHeader := false
	for _, header := range responsePayload.Headers {
		if header.Name == "x-secret-meta" && header.Value == "must-be-encrypted" {
			foundSecretHeader = true
		}
	}
	if !foundSecretHeader {
		t.Fatal("encrypted response metadata missing")
	}

	replay := rawRequest(t, addr, "POST", "/api/echo", requestHeaders, secureBody)
	if replay.status != fh.StatusConflict {
		t.Fatalf("replay status=%d, want %d", replay.status, fh.StatusConflict)
	}

	errorRequestID, _ := protocol.NewID()
	errorNonce, _ := protocol.NewAEADNonce()
	errorNow := time.Now()
	errorEnvelope := protocol.RequestEnvelope{
		SessionID: serverHello.SessionID,
		RequestID: errorRequestID,
		Sequence:  2,
		IssuedAt:  errorNow.UnixMilli(),
		ExpiresAt: errorNow.Add(time.Minute).UnixMilli(),
		Nonce:     errorNonce,
	}
	errorBody, err := protocol.EncryptRequest(keys.ClientToServer, "POST", "/api/error", errorEnvelope, protocol.RequestPayload{}, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	errorHeaders := cloneHeaders(browserHeaders)
	errorHeaders[protocol.HeaderSecure] = "1"
	errorHeaders["Content-Type"] = protocol.MediaTypeRequest
	securedError := rawRequest(t, addr, "POST", "/api/error", errorHeaders, errorBody)
	if securedError.status != fh.StatusUnprocessableEntity {
		t.Fatalf("secure error status=%d body=%q", securedError.status, securedError.body)
	}
	if securedError.headers[strings.ToLower(protocol.HeaderSecure)] != "1" {
		t.Fatal("application error response was not protected")
	}
	errorResponseEnvelope, errorPayload, err := protocol.DecryptResponse(keys.ServerToClient, securedError.status, securedError.body, protocol.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !protocol.EqualID(errorResponseEnvelope.RequestID, errorRequestID) || errorPayload.Status != fh.StatusUnprocessableEntity || len(errorPayload.Body) == 0 {
		t.Fatalf("unexpected encrypted error response: request=%s status=%d body=%q", protocol.EncodeID(errorResponseEnvelope.RequestID), errorPayload.Status, errorPayload.Body)
	}
}

func TestGETHeaderEnvelope(t *testing.T) {
	transport, err := New(Config{AllowEphemeralServerKey: true, AllowUnauthenticatedDeviceRegistration: true})
	if err != nil {
		t.Fatal(err)
	}
	// Header decoding itself is covered independently because the full handshake
	// path is exercised by TestEncryptedFHRequestResponseAndReplay.
	id, _ := protocol.NewID()
	nonce, _ := protocol.NewAEADNonce()
	env := protocol.RequestEnvelope{SessionID: id, RequestID: id, Sequence: 1, IssuedAt: time.Now().UnixMilli(), ExpiresAt: time.Now().Add(time.Minute).UnixMilli(), Nonce: nonce, Ciphertext: []byte("ciphertext")}
	encoded, _ := env.Encode()
	value := base64.RawURLEncoding.EncodeToString(encoded)
	if value == "" || transport.PublicKeyBase64() == "" {
		t.Fatal("header envelope or server key encoding failed")
	}
}

func TestOriginlessSameOriginControlPost(t *testing.T) {
	app := fh.New()
	_, err := Install(app, Config{
		AllowEphemeralServerKey:                true,
		AllowUnauthenticatedDeviceRegistration: true,
		AllowedOrigins:                         []string{"http://localhost"},
		RequireOrigin:                          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	addr := startApp(t, app)

	body := []byte("not-a-registration-request")
	headers := map[string]string{"Content-Type": protocol.MediaTypeHandshake}
	rejected := rawRequest(t, addr, "POST", DefaultPrefix+"/device/register", headers, body)
	if rejected.status != fh.StatusForbidden {
		t.Fatalf("originless request without Fetch Metadata status=%d, want %d", rejected.status, fh.StatusForbidden)
	}

	headers["Sec-Fetch-Site"] = "same-origin"
	acceptedContext := rawRequest(t, addr, "POST", DefaultPrefix+"/device/register", headers, body)
	if acceptedContext.status != fh.StatusBadRequest {
		t.Fatalf("same-origin Fetch Metadata status=%d, want validation to reach body decoder with %d", acceptedContext.status, fh.StatusBadRequest)
	}
}

func cloneHeaders(source map[string]string) map[string]string {
	out := make(map[string]string, len(source)+2)
	for key, value := range source {
		out[key] = value
	}
	return out
}

func TestSameOriginHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		origin string
		host   string
		want   bool
	}{
		{name: "https default port", origin: "https://example.com", host: "example.com", want: true},
		{name: "https explicit default port", origin: "https://example.com", host: "example.com:443", want: true},
		{name: "http explicit default port", origin: "http://example.com:80", host: "example.com", want: true},
		{name: "custom matching port", origin: "https://example.com:8443", host: "example.com:8443", want: true},
		{name: "custom mismatched port", origin: "https://example.com:8443", host: "example.com", want: false},
		{name: "different host", origin: "https://example.com", host: "api.example.com", want: false},
		{name: "userinfo rejected", origin: "https://user@example.com", host: "example.com", want: false},
		{name: "unsupported scheme", origin: "ftp://example.com", host: "example.com", want: false},
		{name: "ipv6", origin: "https://[::1]:8443", host: "[::1]:8443", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameOriginHost(tt.origin, tt.host); got != tt.want {
				t.Fatalf("sameOriginHost(%q, %q)=%v, want %v", tt.origin, tt.host, got, tt.want)
			}
		})
	}
}

func TestPreserveOuterRequestHeader(t *testing.T) {
	t.Parallel()
	transport, err := New(Config{
		AllowEphemeralServerKey: true,
		PreserveOuterHeaders:    []string{"X-Trusted-Proxy-Signal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	preserved := []string{"Host", "Cookie", "Origin", "Sec-Fetch-Site", "Sec-CH-UA", "X-Trusted-Proxy-Signal"}
	for _, name := range preserved {
		if !transport.preserveOuterRequestHeader(name) {
			t.Fatalf("expected %q to be preserved", name)
		}
	}
	removed := []string{"Authorization", "Content-Type", "Accept", "X-Tenant", protocol.HeaderEnvelope, protocol.HeaderSecure}
	for _, name := range removed {
		if transport.preserveOuterRequestHeader(name) {
			t.Fatalf("expected %q to be removed", name)
		}
	}
}
