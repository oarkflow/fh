package mira

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/protocol"
)

type mtuDroppingProxy struct {
	sock    *net.UDPConn
	drop    atomic.Bool
	stopped chan struct{}
}

func newMTUDroppingProxy(t *testing.T, backend *net.UDPAddr) *mtuDroppingProxy {
	t.Helper()
	sock, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	p := &mtuDroppingProxy{sock: sock, stopped: make(chan struct{})}
	go func() {
		defer close(p.stopped)
		b := make([]byte, 1600)
		var frontend *net.UDPAddr
		for {
			n, from, e := sock.ReadFromUDP(b)
			if e != nil {
				return
			}
			if from.Port == backend.Port && from.IP.Equal(backend.IP) {
				if frontend != nil {
					_, _ = sock.WriteToUDP(b[:n], frontend)
				}
			} else {
				frontend = from
				if p.drop.Load() && n > 1200 {
					continue
				}
				_, _ = sock.WriteToUDP(b[:n], backend)
			}
		}
	}()
	return p
}
func (p *mtuDroppingProxy) Close() { _ = p.sock.Close(); <-p.stopped }

func TestMTUBlackholeRecoveryRefragmentsReliableMessage(t *testing.T) {
	secret := bytes.Repeat([]byte{93}, 32)
	opts := Options{DisableAutoMTU: true, Retries: 20}
	server, e := Listen("127.0.0.1:0", secret, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	proxy := newMTUDroppingProxy(t, server.Addr().(*net.UDPAddr))
	defer proxy.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, e := Dial(ctx, proxy.sock.LocalAddr().String(), secret, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close(context.Background())
	peer, e := server.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer peer.Close(context.Background())
	mtu, e := client.ProbeMTU(ctx, 1400)
	if e != nil || mtu != 1400 {
		t.Fatalf("probe %d %v", mtu, e)
	}
	channel, e := client.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if e != nil {
		t.Fatal(e)
	}
	recv, e := peer.AcceptChannel(ctx)
	if e != nil {
		t.Fatal(e)
	}
	proxy.drop.Store(true)
	body := bytes.Repeat([]byte("MIRA-fallback-protected"), 2200)
	msgID, e := channel.Send(ctx, body)
	if e != nil {
		t.Fatal(e)
	}
	m, e := recv.Receive(ctx)
	if e != nil {
		t.Fatalf("reassembly failed: %v", e)
	}
	if !bytes.Equal(m.Payload, body) {
		t.Fatal("payload corrupted after refragmentation")
	}
	if _, e = channel.AwaitReceipt(ctx, msgID, protocol.AckTransport); e != nil {
		t.Fatal(e)
	}
	stats, e := client.Stats(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if stats.PathMTU != 1200 || stats.MTUBlackholeRecoveries == 0 {
		t.Fatalf("no fallback: %+v", stats)
	}
}
