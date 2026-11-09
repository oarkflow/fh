package mira

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"net"
	"sync"
	"testing"
	"time"
)

type rebindingProxy struct {
	front  *net.UDPConn
	backs  [2]*net.UDPConn
	mu     sync.RWMutex
	which  int
	client *net.UDPAddr
}

func newRebindingProxy(t *testing.T, dest *net.UDPAddr) *rebindingProxy {
	t.Helper()
	mk := func() *net.UDPConn {
		c, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	p := &rebindingProxy{front: mk(), backs: [2]*net.UDPConn{mk(), mk()}}
	go func() {
		b := make([]byte, 1500)
		for {
			n, a, e := p.front.ReadFromUDP(b)
			if e != nil {
				return
			}
			p.mu.Lock()
			p.client = a
			i := p.which
			p.mu.Unlock()
			p.backs[i].WriteToUDP(b[:n], dest)
		}
	}()
	for i, b := range p.backs {
		go func(which int, conn *net.UDPConn) {
			buf := make([]byte, 1500)
			for {
				n, _, e := conn.ReadFromUDP(buf)
				if e != nil {
					return
				}
				p.mu.RLock()
				active := which == p.which
				client := p.client
				p.mu.RUnlock()
				if active && client != nil {
					p.front.WriteToUDP(buf[:n], client)
				}
			}
		}(i, b)
	}
	return p
}
func (p *rebindingProxy) switchPath() { p.mu.Lock(); p.which = 1; p.mu.Unlock() }
func (p *rebindingProxy) close() {
	p.front.Close()
	for _, c := range p.backs {
		c.Close()
	}
}
func TestServerPathMigration(t *testing.T) {
	key := bytes.Repeat([]byte{70}, 32)
	srv, e := Listen("127.0.0.1:0", key, Options{Retries: 25})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	proxy := newRebindingProxy(t, srv.Addr().(*net.UDPAddr))
	defer proxy.close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	client, e := Dial(ctx, proxy.front.LocalAddr().String(), key, Options{Retries: 25})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close(context.Background())
	peer, e := srv.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer peer.Close(context.Background())
	a, e := client.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if e != nil {
		t.Fatal(e)
	}
	b, e := peer.AcceptChannel(ctx)
	if e != nil {
		t.Fatal(e)
	}
	sendReceive := func(want string) {
		t.Helper()
		id, e := a.Send(ctx, []byte(want))
		if e != nil {
			t.Fatal(e)
		}
		m, e := b.Receive(ctx)
		if e != nil || string(m.Payload) != want {
			t.Fatalf("received %q: %v", m.Payload, e)
		}
		if _, e = a.AwaitReceipt(ctx, id, protocol.AckTransport); e != nil {
			t.Fatal(e)
		}
	}
	sendReceive("before handover")
	proxy.switchPath()
	sendReceive("after NAT rebinding")
	stats, e := peer.Stats(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if stats.ValidatedMigrations != 1 {
		t.Fatalf("migration not validated: %+v", stats)
	}
	if peer.RemoteAddr().(*net.UDPAddr).Port != proxy.backs[1].LocalAddr().(*net.UDPAddr).Port {
		t.Fatal("wrong active path")
	}
}
