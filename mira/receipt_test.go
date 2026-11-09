package mira

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"testing"
	"time"
)

func TestApplicationReceipts(t *testing.T) {
	key := bytes.Repeat([]byte{91}, 32)
	srv, e := Listen("127.0.0.1:0", key, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cli, e := Dial(ctx, srv.Addr().String(), key, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer cli.Close(context.Background())
	other, e := srv.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close(context.Background())
	a, e := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if e != nil {
		t.Fatal(e)
	}
	b, e := other.AcceptChannel(ctx)
	if e != nil {
		t.Fatal(e)
	}
	id, e := a.Send(ctx, []byte("commit transaction"))
	if e != nil {
		t.Fatal(e)
	}
	msg, e := b.Receive(ctx)
	if e != nil || msg.MessageID != id {
		t.Fatal(e, msg)
	}
	if _, e = a.AwaitReceipt(ctx, id, protocol.AckTransport); e != nil {
		t.Fatal(e)
	}
	if e = b.Acknowledge(ctx, msg.MessageID, protocol.AckStored); e != nil {
		t.Fatal(e)
	}
	if _, e = a.AwaitReceipt(ctx, id, protocol.AckStored); e != nil {
		t.Fatal(e)
	}
	short, cancelShort := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancelShort()
	if _, e = a.AwaitReceipt(short, id, protocol.AckProcessed); e == nil {
		t.Fatal("should not claim processed early")
	}
	if e = b.Acknowledge(ctx, msg.MessageID, protocol.AckProcessed); e != nil {
		t.Fatal(e)
	}
	if _, e = a.AwaitReceipt(ctx, id, protocol.AckProcessed); e != nil {
		t.Fatal(e)
	}
}
