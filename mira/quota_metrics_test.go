package mira

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"strings"
	"testing"
	"time"
)

func TestPerIdentityAdmissionAndMetrics(t *testing.T) {
	key := bytes.Repeat([]byte{28}, 32)
	srv, e := ListenWithResolver("127.0.0.1:0", func(id string) ([]byte, bool) { return key, id == "alice" }, Options{MaxSessionsPerIdentity: 1, HandshakeTimeout: 600 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a, e := DialWithIdentity(ctx, srv.Addr().String(), "alice", key, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close(context.Background())
	serverSide, e := srv.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer serverSide.Close(context.Background())
	secondCtx, end := context.WithTimeout(ctx, 800*time.Millisecond)
	defer end()
	if other, e := DialWithIdentity(secondCtx, srv.Addr().String(), "alice", key, Options{HandshakeTimeout: 600 * time.Millisecond}); e == nil {
		other.Close(context.Background())
		t.Fatal("per-identity quota bypass")
	}
	st := srv.Stats()
	if st.ActiveSessions != 1 || st.HandshakesAccepted != 1 || st.HandshakeAttempts < 2 {
		t.Fatalf("unexpected stats %+v", st)
	}
	var out bytes.Buffer
	if e = srv.WritePrometheus(&out); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "mira_active_sessions 1") || !strings.Contains(out.String(), "# TYPE mira_active_sessions gauge") || !strings.Contains(out.String(), "# TYPE mira_datagrams_read_total counter") || strings.Contains(out.String(), "alice") {
		t.Fatalf("metrics disclose private state: %s", out.String())
	}
}
func TestInboundRateLimitAndPriorityFairness(t *testing.T) {
	key := bytes.Repeat([]byte{29}, 32)
	srv, e := Listen("127.0.0.1:0", key, Options{MaxInboundBytesPerSecond: 2 << 10})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, e := Dial(ctx, srv.Addr().String(), key, Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close(context.Background())
	s, e := srv.Accept(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close(context.Background())
	a, e := c.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered, Priority: 100})
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.AcceptChannel(ctx)
	if e != nil {
		t.Fatal(e)
	}
	x := bytes.Repeat([]byte{5}, 5<<10)
	_, e = a.Send(ctx, x)
	if e != nil {
		t.Fatal(e)
	}
	msg, e := b.Receive(ctx)
	if e != nil || !bytes.Equal(x, msg.Payload) {
		t.Fatalf("rate-limited message loss: %d %v", len(msg.Payload), e)
	}
	st, e := s.Stats(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if st.RateLimitedBytes == 0 {
		t.Fatal("rate limit did not exercise backpressure")
	}
}
