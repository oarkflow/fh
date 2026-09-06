package fh

import (
	"bytes"
	"testing"
)

func TestDirectJSONAppenderMayReturnExternalStorage(t *testing.T) {
	conn := &benchConn{buf: make([]byte, 4096)}
	app := NewFast(WithDisableHTTP2(true), WithDisablePanicRecovery(true))
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)
	ctx.Header.KeepAlive = true
	external := []byte(`{"external":true}`)
	if err := ctx.JSONAppend(func([]byte) ([]byte, error) { return external, nil }); err != nil {
		t.Fatal(err)
	}
	written := conn.buf[:conn.pos]
	if !bytes.HasSuffix(written, external) {
		t.Fatalf("response=%q", written)
	}
	if ctx.writeBuf == nil || len(*ctx.writeBuf) != 0 {
		t.Fatal("caller-owned JSON was retained as the response buffer")
	}
}

func TestDirectJSONMapWithExactHeaderReserveDoesNotPanic(t *testing.T) {
	conn := &benchConn{buf: make([]byte, 4096)}
	app := NewFast(WithDisableHTTP2(true), WithDisablePanicRecovery(true))
	ctx := acquireCtx(conn, app)
	defer releaseCtx(ctx)
	ctx.Header.KeepAlive = true
	buf := make([]byte, directJSONHeaderReserve)
	ctx.writeBuf = &buf

	if err := ctx.JSON(Map{"message": "Hello, World!"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(conn.buf[:conn.pos], []byte(`{"message":"Hello, World!"}`)) {
		t.Fatalf("response=%q", conn.buf[:conn.pos])
	}
}
