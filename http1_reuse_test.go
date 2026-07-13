package fh

import (
	"net"
	"testing"
)

func TestHTTP1ConnectionReusesContextAndWriteBuffer(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	app := NewFast()
	state := &connState{writeBuf: make([]byte, 0, 256)}

	first := acquireHTTP1Ctx(server, app, state)
	first.status = StatusCreated
	first.writeBuf = &state.writeBuf
	*first.writeBuf = append((*first.writeBuf)[:0], "response"...)
	releaseCtx(first)

	second := acquireHTTP1Ctx(server, app, state)
	defer releaseCtx(second)

	if second != first {
		t.Fatal("HTTP/1 context was not reused for the keep-alive connection")
	}
	if second.status != StatusOK {
		t.Fatalf("context was not reset: status=%d", second.status)
	}
	if second.writeBuf != &state.writeBuf {
		t.Fatal("HTTP/1 response buffer is not connection-owned")
	}
	if cap(*second.writeBuf) < len("response") {
		t.Fatal("response buffer capacity was not retained")
	}
}
