package mira

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/protocol"
)

func TestFullDuplexAndFragmentation(t *testing.T) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
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
	if cli.ID() != serv.ID() {
		t.Fatalf("CID mismatch: %d != %d", cli.ID(), serv.ID())
	}
	cc, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal("open client:", err)
	}
	sc, err := serv.AcceptChannel(ctx)
	if err != nil {
		t.Fatal("accept client:", err)
	}
	if cc.ID() != sc.ID() {
		t.Fatal("channel mismatch")
	}
	large := make([]byte, 60*1024)
	if _, err = rand.Read(large); err != nil {
		t.Fatal(err)
	}
	id, err := cc.Send(ctx, large)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sc.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, large) {
		t.Fatalf("large payload corrupted: got=%d", len(got.Payload))
	}
	if _, err = cc.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
		t.Fatal("receipt:", err)
	}
	rev, err := serv.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableUnordered})
	if err != nil {
		t.Fatal(err)
	}
	rcv, err := cli.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		msg := []byte(fmt.Sprintf("server push #%d", i))
		_, err = rev.Send(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
		got, err = rcv.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Payload, msg) {
			t.Fatalf("bad server push %s", got.Payload)
		}
	}
}

func TestWrongSecretRejected(t *testing.T) {
	key := bytes.Repeat([]byte{0x7a}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{HandshakeTimeout: 450 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = Dial(ctx, srv.Addr().String(), bytes.Repeat([]byte{0x4b}, 32), Options{HandshakeTimeout: 400 * time.Millisecond})
	if err == nil {
		t.Fatal("incorrect PSK must not authenticate")
	}
}

func TestBestEffortAndLatest(t *testing.T) {
	key := bytes.Repeat([]byte{0x38}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	for _, d := range []protocol.Delivery{protocol.BestEffort, protocol.LatestState} {
		a, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: d})
		if err != nil {
			t.Fatal(err)
		}
		b, err := serv.AcceptChannel(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = a.Send(ctx, []byte("ping"))
		if err != nil {
			t.Fatal(err)
		}
		m, err := b.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(m.Payload) != "ping" {
			t.Fatal("bad datagram")
		}
	}
}

func TestConcurrentBidirectionalChannels(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	ca, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	sa, err := serv.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := serv.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	sb, err := cli.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	writers := func(ch protocol.Channel, tag string) {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_, err := ch.Send(ctx, []byte(fmt.Sprintf("%s/%d", tag, i)))
			if err != nil {
				errs <- err
				return
			}
		}
	}
	readers := func(ch protocol.Channel, tag string) {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			m, err := ch.Receive(ctx)
			if err != nil {
				errs <- err
				return
			}
			want := fmt.Sprintf("%s/%d", tag, i)
			if string(m.Payload) != want {
				errs <- fmt.Errorf("got %s want %s", m.Payload, want)
				return
			}
		}
	}
	wg.Add(4)
	go writers(ca, "client")
	go writers(cb, "server")
	go readers(sa, "client")
	go readers(sb, "server")
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
