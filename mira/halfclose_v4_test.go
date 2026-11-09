package mira

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"io"
	"testing"
	"time"
)

func TestReliableHalfCloseAndNoSendAfter(t *testing.T) {
	key := bytes.Repeat([]byte{69}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cli, err := Dial(ctx, srv.Addr().String(), key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	peer, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close(context.Background())
	a, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	b, err := peer.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := a.Send(ctx, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.(*Channel).CloseWrite(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Send(ctx, []byte("too late")); err != ErrClosed {
		t.Fatal("sent after close", err)
	}
	for i := 0; i < 5; i++ {
		m, e := b.Receive(ctx)
		if e != nil || len(m.Payload) != 1 || m.Payload[0] != byte(i) {
			t.Fatalf("got %v %v", m, e)
		}
	}
	_, err = b.Receive(ctx)
	if err != io.EOF {
		t.Fatalf("expected half-close EOF, got %v", err)
	}
	// A half-closed channel still receives messages from its other half.
	if _, err := b.Send(ctx, []byte("reply")); err != nil {
		t.Fatal(err)
	}
	got, err := a.Receive(ctx)
	if err != nil || string(got.Payload) != "reply" {
		t.Fatalf("reply %q %v", got.Payload, err)
	}
}
