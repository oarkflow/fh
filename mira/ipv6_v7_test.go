package mira

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/oarkflow/fh/protocol"
)

// This is an IPv6 loopback smoke test, NOT IPv6-only WAN/NAT64 certification.
func TestIPv6LoopbackEncryptedFullDuplex(t *testing.T) {
	if _, err := net.ListenPacket("udp6", "[::1]:0"); err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	key := bytes.Repeat([]byte{0x29}, 32)
	srv, err := Listen("[::1]:0", key, Options{})
	if err != nil {
		t.Skipf("IPv6 listener unavailable: %v", err)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		ss, e := srv.Accept(ctx)
		if e != nil {
			done <- e
			return
		}
		defer ss.Close(context.Background())
		ch, e := ss.AcceptChannel(ctx)
		if e != nil {
			done <- e
			return
		}
		msg, e := ch.Receive(ctx)
		if e != nil {
			done <- e
			return
		}
		_, e = ch.Send(ctx, append([]byte("ipv6:"), msg.Payload...))
		done <- e
	}()
	client, err := Dial(ctx, srv.Addr().String(), key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	ch, err := client.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ch.Send(ctx, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	received, err := ch.Receive(ctx)
	if err != nil || string(received.Payload) != "ipv6:hello" {
		t.Fatalf("IPv6 response %q: %v", received.Payload, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
