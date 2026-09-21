package invocation_test

import (
	"bytes"
	"testing"

	"github.com/oarkflow/fh/ref/invocation"
)

func TestInputImmutability(t *testing.T) {
	orig := []byte("hello world")
	input := invocation.NewInput(orig, "text/plain")

	// Mutate original slice
	orig[0] = 'X'
	if bytes.Equal(input.Bytes(), orig) {
		t.Fatalf("expected input to be immutable from original buffer mutation")
	}

	// Mutate returned slice
	ret := input.Bytes()
	ret[0] = 'Z'
	if bytes.Equal(input.Bytes(), ret) {
		t.Fatalf("expected input to be immutable from returned buffer mutation")
	}

	if input.ContentType() != "text/plain" {
		t.Errorf("expected content type text/plain, got %s", input.ContentType())
	}
	if input.Len() != 11 {
		t.Errorf("expected len 11, got %d", input.Len())
	}
}

func TestTransportMetadata(t *testing.T) {
	headers := map[string][]string{"X-Test": {"a", "b"}}
	query := map[string][]string{"q": {"search"}}
	params := map[string]string{"id": "123"}
	httpMeta := invocation.NewHTTPMeta("GET", "/users/123", "/users/:id", "localhost", headers, query, params)

	headers["X-Test"][0] = "mutated"
	if httpMeta.Headers["X-Test"][0] == "mutated" {
		t.Errorf("HTTPMeta headers should be cloned and immutable")
	}

	if httpMeta.TransportName() != "http" {
		t.Errorf("expected transport name http, got %s", httpMeta.TransportName())
	}

	grpcMeta := invocation.NewGRPCMeta("UserService", "GetUser", map[string][]string{"token": {"xyz"}})
	if grpcMeta.TransportName() != "grpc" {
		t.Errorf("expected transport name grpc, got %s", grpcMeta.TransportName())
	}

	queueMeta := invocation.NewQueueMeta("events", 1, "msg-001", map[string]string{"key": "val"})
	if queueMeta.TransportName() != "queue" {
		t.Errorf("expected transport name queue, got %s", queueMeta.TransportName())
	}

	wsMeta := invocation.WSMeta{ConnectionID: "conn-1", MessageType: "text"}
	if wsMeta.TransportName() != "ws" {
		t.Errorf("expected transport name ws, got %s", wsMeta.TransportName())
	}

	cliMeta := invocation.NewCLIMeta([]string{"run", "--dry"}, map[string]string{"ENV": "test"}, "/tmp")
	if cliMeta.TransportName() != "cli" {
		t.Errorf("expected transport name cli, got %s", cliMeta.TransportName())
	}
}
