// mira-load is a repeatable localhost or remote echo workload generator.
// Benchmarks are illustrative and cannot establish fairness against QUIC/
// WebSocket unless the same workload and security settings are reproduced.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"log"
	"sort"
	"sync"
	"time"
)

type result struct {
	samples []time.Duration
	sent    int64
	failure error
}

func main() {
	clients := flag.Int("clients", 10, "simultaneous sessions (1..200)")
	iterations := flag.Int("messages", 100, "round-trips per session (1..100000)")
	size := flag.Int("bytes", 256, "payload bytes (1..16384)")
	timeout := flag.Duration("timeout", 2*time.Minute, "total benchmark deadline")
	flag.Parse()
	if *clients < 1 || *clients > 200 || *iterations < 1 || *iterations > 100000 || *size < 1 || *size > 16384 {
		log.Fatal("invalid limits")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	key := bytes.Repeat([]byte{0x62}, 32) // localhost-only test key; NEVER use on real networks.
	srv, e := mira.Listen("127.0.0.1:0", key, mira.Options{MaxSessions: *clients + 20, MaxSessionLifetime: *timeout + time.Minute})
	if e != nil {
		log.Fatal(e)
	}
	defer srv.Close()
	var handlers sync.WaitGroup
	handlers.Add(1)
	go func() {
		defer handlers.Done()
		for {
			session, e := srv.Accept(ctx)
			if e != nil {
				return
			}
			handlers.Add(1)
			go func(s *mira.Session) {
				defer handlers.Done()
				defer s.Close(context.Background())
				ch, e := s.AcceptChannel(ctx)
				if e != nil {
					return
				}
				for {
					m, e := ch.Receive(ctx)
					if e != nil {
						return
					}
					if _, e = ch.Send(ctx, m.Payload); e != nil {
						return
					}
				}
			}(session)
		}
	}()
	start := time.Now()
	results := make(chan result, *clients)
	for j := 0; j < *clients; j++ {
		go func() {
			result := result{samples: make([]time.Duration, 0, min(*iterations, 1000))}
			defer func() { results <- result }()
			session, e := mira.Dial(ctx, srv.Addr().String(), key, mira.Options{MaxSessionLifetime: *timeout + time.Minute})
			if e != nil {
				result.failure = e
				return
			}
			defer session.Close(context.Background())
			ch, e := session.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
			if e != nil {
				result.failure = e
				return
			}
			payload := bytes.Repeat([]byte{0xab}, *size)
			step := max(1, *iterations/1000)
			for i := 0; i < *iterations; i++ {
				t0 := time.Now()
				_, e = ch.Send(ctx, payload)
				if e != nil {
					result.failure = e
					return
				}
				response, e := ch.Receive(ctx)
				if e != nil {
					result.failure = e
					return
				}
				if !bytes.Equal(response.Payload, payload) {
					result.failure = fmt.Errorf("payload mismatch")
					return
				}
				result.sent++
				if i%step == 0 {
					result.samples = append(result.samples, time.Since(t0))
				}
			}
		}()
	}
	all := make([]time.Duration, 0, min(*clients**iterations, 100000))
	var sent int64
	failed := 0
	for i := 0; i < *clients; i++ {
		r := <-results
		sent += r.sent
		all = append(all, r.samples...)
		if r.failure != nil {
			failed++
			log.Printf("worker failed: %v", r.failure)
		}
	}
	cancel()
	_ = srv.Close()
	handlers.Wait()
	elapsed := time.Since(start)
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if len(all) == 0 {
		log.Fatal("no successful samples")
	}
	at := func(p float64) time.Duration { return all[min(int(p*float64(len(all)-1)), len(all)-1)] }
	fmt.Printf("clients=%d completed=%d failed=%d payload=%dB elapsed=%s throughput=%.1f msg/s p50=%s p95=%s p99=%s samples=%d\n", *clients, sent, failed, *size, elapsed, float64(sent)/elapsed.Seconds(), at(.50), at(.95), at(.99), len(all))
	if failed > 0 {
		log.Fatal("benchmark contained failures")
	}
}
