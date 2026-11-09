package mira

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"testing"
	"time"
)

func TestClientSocketRebind(t *testing.T) {
	secret := bytes.Repeat([]byte{31}, 32)
	srv, err := Listen("127.0.0.1:0", secret, Options{IdleTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := Dial(ctx, srv.Addr().String(), secret, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	peer, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close(context.Background())
	ch, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	serverCh, err := peer.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old := cli.currentConn().LocalAddr().String()
	if err := cli.RebindClient(ctx); err != nil {
		t.Fatal(err)
	}
	now := cli.currentConn().LocalAddr().String()
	if old == now {
		t.Fatal("socket did not change")
	}
	// The actor sends an encrypted probe and the server validates this new path.
	id, err := ch.Send(ctx, []byte("after local socket handover"))
	if err != nil {
		t.Fatal(err)
	}
	msg, err := serverCh.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Payload) != "after local socket handover" {
		t.Fatal("corrupt")
	}
	if _, err := ch.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
		t.Fatal(err)
	}
	st, err := peer.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.ValidatedMigrations < 1 {
		t.Fatalf("no validated migration %+v", st)
	}
}
