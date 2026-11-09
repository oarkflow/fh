package mira

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/oarkflow/fh/protocol"
)

func TestFlowControlSlowReceiver(t *testing.T) {
	secret := bytes.Repeat([]byte{0xf1}, 32)
	opts := Options{MaxMessage: 32 << 10, MaxReceiveBytes: 64 << 10}
	srv, err := Listen("127.0.0.1:0", secret, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cli, err := Dial(ctx, srv.Addr().String(), secret, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	serv, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer serv.Close(context.Background())
	send, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	recv, err := serv.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := bytes.Repeat([]byte("z"), 30<<10)
	for i := 0; i < 3; i++ {
		if _, err = send.Send(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	// Before the application consumes anything, only two messages (60 KiB)
	// fit under the advertised 64 KiB connection-level credit window.
	time.Sleep(150 * time.Millisecond)
	if used := serv.recvBytes.Load(); used > 64<<10 {
		t.Fatalf("receive bytes exceeded budget: %d", used)
	}
	for i := 0; i < 3; i++ {
		got, err := recv.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Payload, m) {
			t.Fatalf("corrupted message %d", i)
		}
	}
}

func TestChannelCloseUnblocksReceive(t *testing.T) {
	key := bytes.Repeat([]byte{0x31}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := Dial(ctx, srv.Addr().String(), key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	serv, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer serv.Close(context.Background())
	a, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	b, err := serv.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = b.Receive(ctx)
	if err != ErrClosed {
		t.Fatalf("expected close, got %v", err)
	}
}
