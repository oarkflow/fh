package mira

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestDistinctDeviceCredentials(t *testing.T) {
	keys := map[string][]byte{"device-a": bytes.Repeat([]byte{0xa1}, 32), "device-b": bytes.Repeat([]byte{0xb2}, 32)}
	srv, err := ListenWithResolver("127.0.0.1:0", func(id string) ([]byte, bool) { k, ok := keys[id]; return k, ok }, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	a, err := DialWithIdentity(ctx, srv.Addr().String(), "device-a", keys["device-a"], Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close(context.Background())
	b, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b.Identity() != "device-a" {
		t.Fatalf("identity mismatch: %q", b.Identity())
	}
	defer b.Close(context.Background())
	badCtx, badCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer badCancel()
	_, err = DialWithIdentity(badCtx, srv.Addr().String(), "device-b", keys["device-a"], Options{HandshakeTimeout: 400 * time.Millisecond})
	if err == nil {
		t.Fatal("mismatched device credential authenticated")
	}
}
