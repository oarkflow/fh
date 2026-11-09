package mira

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"testing"
	"time"
)

func TestProbeMTUAndLargerFragments(t *testing.T) {
	secret := bytes.Repeat([]byte{27}, 32)
	srv, err := Listen("127.0.0.1:0", secret, Options{})
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
	serv, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer serv.Close(context.Background())
	mt, err := cli.ProbeMTU(ctx, 1400)
	if err != nil {
		t.Fatal(err)
	}
	if mt != 1400 {
		t.Fatalf("PMTU %d", mt)
	}
	c, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	s, err := serv.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("010203040506070809"), 6000)
	id, err := c.Send(ctx, payload)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Payload, payload) {
		t.Fatal("large-fragment corruption")
	}
	if _, err := c.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
		t.Fatal(err)
	}
	st, err := cli.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.PathMTU != 1400 {
		t.Fatalf("path MTU snapshot %+v", st)
	}
}
func TestProbeMTURejectedAboveSafetyLimit(t *testing.T) {
	s := &Session{}
	_, err := s.ProbeMTU(context.Background(), 1600)
	if err == nil {
		t.Fatal("must reject unsupported probe")
	}
}
