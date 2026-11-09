package mira

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/internal/wire"
	"github.com/oarkflow/fh/protocol"
)

// This in-process UDP proxy deterministically drops each 6th encrypted DATA
// packet in either direction, including transport acknowledgments.
type lossyProxy struct {
	conn       *net.UDPConn
	upstream   *net.UDPConn
	dest       *net.UDPAddr
	downstream *net.UDPAddr
	mu         sync.Mutex
	client     *net.UDPAddr
	stopped    chan struct{}
	every      atomic.Int64
	seen       atomic.Int64
}

func newLossyProxy(t *testing.T, dest *net.UDPAddr) *lossyProxy {
	t.Helper()
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	p := &lossyProxy{conn: front, upstream: back, dest: dest, stopped: make(chan struct{})}
	p.every.Store(6)
	p.downstream = front.LocalAddr().(*net.UDPAddr)
	go func() {
		defer close(p.stopped)
		b := make([]byte, 1500)
		for {
			n, from, e := front.ReadFromUDP(b)
			if e != nil {
				return
			}
			p.mu.Lock()
			p.client = from
			p.mu.Unlock()
			raw := append([]byte(nil), b[:n]...)
			if p.drop(raw) {
				continue
			}
			_, _ = back.WriteToUDP(raw, dest)
		}
	}()
	go func() {
		b := make([]byte, 1500)
		for {
			n, _, e := back.ReadFromUDP(b)
			if e != nil {
				return
			}
			raw := append([]byte(nil), b[:n]...)
			if p.drop(raw) {
				continue
			}
			p.mu.Lock()
			client := p.client
			p.mu.Unlock()
			if client != nil {
				_, _ = front.WriteToUDP(raw, client)
			}
		}
	}()
	return p
}
func (p *lossyProxy) drop(raw []byte) bool {
	h, err := wire.ParseHeader(raw)
	return err == nil && h.Kind == wire.KindData && p.seen.Add(1)%p.every.Load() == 0
}
func (p *lossyProxy) Close() { _ = p.conn.Close(); _ = p.upstream.Close(); <-p.stopped }

func TestReliabilityWithPacketLoss(t *testing.T) {
	key := bytes.Repeat([]byte{0x43}, 32)
	srv, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	proxy := newLossyProxy(t, srv.Addr().(*net.UDPAddr))
	defer proxy.Close()
	// We only change proxy endpoint; the handshake and both application
	// directions are transported through loss-injecting UDP forwarding.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cli, err := Dial(ctx, proxy.downstream.String(), key, Options{Retries: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close(context.Background())
	serv, err := srv.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer serv.Close(context.Background())
	c, err := cli.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	s, err := serv.AcceptChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msg := bytes.Repeat([]byte("xyz123"), 15*1024) // 90KiB, 103 packets
	id, err := c.Send(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, msg) {
		t.Fatalf("wrong bytes under packet loss: %d", len(got.Payload))
	}
	receiptCtx, cancelReceipt := context.WithTimeout(ctx, 6*time.Second)
	defer cancelReceipt()
	_, err = c.AwaitReceipt(receiptCtx, id, protocol.AckTransport)
	if err != nil {
		cs, ce := cli.Stats(ctx)
		ss, se := serv.Stats(ctx)
		ch := c.(*Channel)
		ch.mu.Lock()
		r := ch.receipts[id]
		left := -1
		if r != nil {
			left = r.left
		}
		ch.mu.Unlock()
		t.Fatalf("missing ACK %v left=%d client=%+v clienterr=%v server=%+v servererr=%v", err, left, cs, ce, ss, se)
	}
	if proxy.seen.Load() < 100 {
		t.Fatalf("expected packet fragmentation, saw %d packets", proxy.seen.Load())
	}
}
