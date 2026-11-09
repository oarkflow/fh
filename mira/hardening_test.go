package mira

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh/protocol"
)

func TestEpochKeysAndAssociatedData(t *testing.T) {
	var sched trafficSchedule
	sched.id = 42
	sched.client = true
	copy(sched.master[:], bytes.Repeat([]byte{0xd1}, 32))
	sender, err := sched.protector(1, true)
	if err != nil {
		t.Fatal(err)
	}
	sched.client = false
	receiver, err := sched.protector(1, false)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := sealedEpoch(sender, 42, keyEpochPackets+1, 3, 1, 1, []byte("encrypted message"))
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := openedEpoch(receiver, packet, 3)
	if err != nil || string(got) != "encrypted message" {
		t.Fatalf("epoch roundtrip %s: %v", got, err)
	}
	packet[6] ^= 0x1 // Change authenticated connection id (AAD).
	if _, _, err := openedEpoch(receiver, packet, 3); err == nil {
		t.Fatal("header alteration accepted")
	}
	packet[6] ^= 0x1
	prev, err := sched.protector(0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openedEpoch(prev, packet, 3); err == nil {
		t.Fatal("wrong epoch key accepted")
	}
}

func TestChannelAuthorizationAndLiveRevocation(t *testing.T) {
	secret := bytes.Repeat([]byte{0xad}, 32)
	srv, err := ListenWithResolver("127.0.0.1:0", func(id string) ([]byte, bool) {
		if id != "device-a" {
			return nil, false
		}
		return secret, true
	}, Options{HandshakeTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetChannelAuthorizer(func(identity string, cfg protocol.ChannelConfig) bool {
		return identity == "device-a" && cfg.Delivery == protocol.ReliableOrdered
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, err := DialWithIdentity(ctx, srv.Addr().String(), "device-a", secret, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	server, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	if _, err := client.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.BestEffort}); !errors.Is(err, ErrClosed) {
		t.Fatalf("denied channel returned %v, expected ErrClosed", err)
	}
	ch, err := client.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := srv.RevokeIdentity("device-a"); n != 1 {
		t.Fatalf("revoked %d sessions", n)
	}
	select {
	case <-client.done:
	case <-ctx.Done():
		t.Fatal("GOAWAY not received after revocation")
	}
	if _, err := ch.Send(ctx, []byte("should fail")); !errors.Is(err, ErrClosed) {
		t.Fatalf("revoked session send: %v", err)
	}
	rejectCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err = DialWithIdentity(rejectCtx, srv.Addr().String(), "device-a", secret, Options{HandshakeTimeout: 300 * time.Millisecond}); err == nil {
		t.Fatal("revoked identity reconnected")
	}
}

func TestChannelLifecycleDoesNotExhaustLimit(t *testing.T) {
	key := bytes.Repeat([]byte{0xba}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{MaxChannels: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cli, err := Dial(ctx, srv.Addr().String(), key, Options{MaxChannels: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	peer, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close(context.Background())
	for i := 0; i < 20; i++ {
		ch, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
		if err != nil {
			t.Fatalf("cycle %d open: %v", i, err)
		}
		other, err := peer.AcceptChannel(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ch.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := other.Receive(ctx); !errors.Is(err, ErrClosed) {
			t.Fatalf("cycle %d expected remote close, got %v", i, err)
		}
	}
}

func TestEpochTransitionOverLiveUDP(t *testing.T) {
	key := bytes.Repeat([]byte{0xca}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cli, err := Dial(ctx, srv.Addr().String(), key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	server, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	sender, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := server.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const messages = 2200 // Each DATA + ACK passes 4096 packets on the sender.
	var wg sync.WaitGroup
	wg.Add(1)
	readErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		for i := 0; i < messages; i++ {
			m, e := receiver.Receive(ctx)
			if e != nil {
				readErr <- e
				return
			}
			if string(m.Payload) != fmt.Sprintf("m:%d", i) {
				readErr <- fmt.Errorf("wrong message %d: %s", i, m.Payload)
				return
			}
		}
	}()
	for i := 0; i < messages; i++ {
		id, err := sender.Send(ctx, []byte(fmt.Sprintf("m:%d", i)))
		if err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if _, err = sender.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
			t.Fatalf("receipt %d: %v", i, err)
		}
	}
	wg.Wait()
	select {
	case err := <-readErr:
		t.Fatal(err)
	default:
	}
}
