package mira_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/oarkflow/fh/internal/netsim"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/protocol"
)

type wanResult struct {
	p50, max  time.Duration
	stats     mira.SessionStats
	forwarded uint64
	dropped   uint64
}

// wanEcho runs rounds of echo-with-receipt of size bytes across an emulated
// path and returns latency percentiles plus the client session's final stats.
func wanEcho(t testing.TB, path netsim.Config, opts mira.Options, size, rounds int) wanResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ca, err := devpki.New()
	if err != nil {
		t.Fatal(err)
	}
	sc, _ := ca.ServerTLS()
	srv, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", sc, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	srv.SetChannelAuthorizer(func(string, protocol.ChannelConfig) bool { return true })
	go func() {
		s, err := srv.Accept(ctx)
		if err != nil {
			return
		}
		ch, err := s.AcceptChannel(ctx)
		if err != nil {
			return
		}
		for {
			m, err := ch.Receive(ctx)
			if err != nil {
				return
			}
			if _, err := ch.Send(ctx, m.Payload); err != nil {
				return
			}
		}
	}()
	proxy, err := netsim.New(srv.Addr().(*net.UDPAddr), path)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	cc, _ := ca.ClientTLS("wan-client")
	s, err := mira.DialTLS(ctx, srv.TLSAddr().String(), proxy.Addr().String(), cc, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	ch, err := s.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered, MaxMessage: int64(size)})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, size)
	var d []time.Duration
	for i := 0; i < rounds; i++ {
		start := time.Now()
		id, err := ch.Send(ctx, payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ch.Receive(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := ch.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
			t.Fatal(err)
		}
		d = append(d, time.Since(start))
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	st, _ := s.Stats(ctx)
	return wanResult{p50: d[len(d)/2], max: d[len(d)-1], stats: st, forwarded: proxy.Forwarded.Load(), dropped: proxy.Dropped.Load()}
}

// TestWANSlowStartReport prints how many network round trips a message of each
// size costs on a long path. Run it with -v. It does not assert; see
// TestLargeMessageRoundTripBudget for the regression guard.
func TestWANSlowStartReport(t *testing.T) {
	skipUnlessReport(t)
	path := netsim.Config{OneWayDelay: 67 * time.Millisecond, Seed: 7}
	rtt := 2 * path.OneWayDelay
	for _, size := range []int{256, 4 << 10, 16 << 10, 64 << 10, 256 << 10} {
		r := wanEcho(t, path, mira.Options{}, size, 4)
		fmt.Printf("WAN rtt=%v size=%-7d p50=%-9v = %4.1f RTT  cwnd=%-7d retrans=%-3d fastLoss=%-3d fwd=%d\n",
			rtt, size, r.p50.Round(time.Millisecond), float64(r.p50)/float64(rtt), r.stats.CongestionWindowBytes, r.stats.Retransmissions, r.stats.FastLossSignals, r.forwarded)
	}
}

// TestWANImpairedReport prints behavior under loss and jitter (run with -v).
func TestWANImpairedReport(t *testing.T) {
	skipUnlessReport(t)
	cases := []struct {
		name string
		path netsim.Config
	}{
		{"1% loss", netsim.Config{OneWayDelay: 67 * time.Millisecond, Loss: 0.01, Seed: 11}},
		{"3% loss", netsim.Config{OneWayDelay: 67 * time.Millisecond, Loss: 0.03, Seed: 12}},
		{"jitter 30ms", netsim.Config{OneWayDelay: 67 * time.Millisecond, Jitter: 30 * time.Millisecond, Seed: 13}},
		{"10MB/s bottleneck", netsim.Config{OneWayDelay: 67 * time.Millisecond, RateBytesPerSec: 10 << 20, QueueBytes: 256 << 10, Seed: 14}},
	}
	rtt := 134 * time.Millisecond
	for _, c := range cases {
		for _, size := range []int{16 << 10, 256 << 10} {
			r := wanEcho(t, c.path, mira.Options{}, size, 4)
			fmt.Printf("IMP %-18s size=%-7d p50=%-9v = %5.1f RTT  max=%-9v retrans=%-4d fastLoss=%-4d dropped=%d\n",
				c.name, size, r.p50.Round(time.Millisecond), float64(r.p50)/float64(rtt), r.max.Round(time.Millisecond), r.stats.Retransmissions, r.stats.FastLossSignals, r.dropped)
		}
	}
}

func skipUnlessReport(t *testing.T) {
	t.Helper()
	if os.Getenv("MIRA_WAN_REPORT") == "" {
		t.Skip("set MIRA_WAN_REPORT=1 (and -v) to print the long-path behavior report")
	}
}

// TestLargeMessageRoundTripBudget guards slow start on a 134 ms path. Before
// the fixes a 64 KiB echo cost ~7.3 RTT and 256 KiB ~18.6 RTT, because startup
// control-frame retransmissions were counted as congestion (halving cwnd and
// ending slow start for the whole session). The budgets leave headroom for
// loaded CI machines; healthy runs measure ~4.2 and ~8.0.
func TestLargeMessageRoundTripBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated 134 ms path; skipped in -short")
	}
	path := netsim.Config{OneWayDelay: 67 * time.Millisecond, Seed: 7}
	rtt := 2 * path.OneWayDelay
	for _, c := range []struct {
		size   int
		budget float64
	}{{4 << 10, 2.0}, {64 << 10, 6.0}, {256 << 10, 11.0}} {
		r := wanEcho(t, path, mira.Options{}, c.size, 3)
		got := float64(r.p50) / float64(rtt)
		t.Logf("size=%d p50=%v = %.1f RTT (budget %.1f) cwnd=%d", c.size, r.p50.Round(time.Millisecond), got, c.budget, r.stats.CongestionWindowBytes)
		if got > c.budget {
			t.Errorf("%d-byte echo took %.1f RTT, budget %.1f: slow start has regressed", c.size, got, c.budget)
		}
	}
}

// TestImpairedPathsStayCorrect checks data integrity and completion under loss
// and reordering. It asserts correctness, not speed.
func TestImpairedPathsStayCorrect(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated impaired paths; skipped in -short")
	}
	for name, path := range map[string]netsim.Config{
		"loss":   {OneWayDelay: 20 * time.Millisecond, Loss: 0.03, Seed: 21},
		"jitter": {OneWayDelay: 20 * time.Millisecond, Jitter: 15 * time.Millisecond, Seed: 22},
	} {
		r := wanEcho(t, path, mira.Options{}, 128<<10, 2)
		t.Logf("%s: p50=%v retrans=%d dropped=%d", name, r.p50.Round(time.Millisecond), r.stats.Retransmissions, r.dropped)
	}
}
