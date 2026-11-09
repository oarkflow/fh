// mira-bench measures encrypted MIRA round trips against a separate echo server.
// It reports p50/p95/p99 and failures; it does not certify QUIC/WS superiority.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/fh/internal/certconfig"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

type report struct {
	Protocol            string  `json:"protocol"`
	Clients             int     `json:"clients"`
	Messages            int     `json:"messages"`
	PayloadBytes        int     `json:"payload_bytes"`
	Completed           int     `json:"completed"`
	Failures            int     `json:"failures"`
	DurationMS          float64 `json:"duration_ms"`
	RoundTripsPerSecond float64 `json:"round_trips_per_second"`
	P50MS               float64 `json:"p50_ms"`
	P95MS               float64 `json:"p95_ms"`
	P99MS               float64 `json:"p99_ms"`
}
type result struct {
	samples []time.Duration
	count   int
	err     error
}

func percentile(samples []time.Duration, p float64) float64 {
	if len(samples) == 0 {
		return math.NaN()
	}
	return float64(samples[min(int(math.Ceil(float64(len(samples))*p))-1, len(samples)-1)]) / float64(time.Millisecond)
}
func main() {
	tcp := flag.String("tcp", "127.0.0.1:7448", "mTLS bootstrap endpoint")
	udp := flag.String("udp", "127.0.0.1:7447", "MIRA UDP endpoint")
	cert := flag.String("cert", "", "client certificate PEM")
	key := flag.String("key", "", "client private key PEM")
	ca := flag.String("ca", "", "trusted CA PEM")
	name := flag.String("name", "server.local", "TLS server DNS name")
	clients := flag.Int("clients", 2, "concurrent sessions (1..100)")
	messages := flag.Int("messages", 100, "round trips per session (1..10000)")
	size := flag.Int("bytes", 256, "payload bytes (1..65536)")
	timeout := flag.Duration("timeout", 2*time.Minute, "global deadline")
	output := flag.String("output", "", "JSON output file, default stdout")
	flag.Parse()
	if *clients < 1 || *clients > 100 || *messages < 1 || *messages > 10000 || *size < 1 || *size > 65536 || *timeout < time.Second || *timeout > time.Hour {
		log.Fatal("invalid test limits")
	}
	cfg, err := certconfig.Client(*cert, *key, *ca, *name)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	start := time.Now()
	results := make(chan result, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := result{samples: make([]time.Duration, 0, *messages)}
			defer func() { results <- out }()
			ss, e := mira.DialTLS(ctx, *tcp, *udp, cfg, mira.Options{MaxSessionLifetime: *timeout + time.Minute})
			if e != nil {
				out.err = e
				return
			}
			defer ss.Close(context.Background())
			ch, e := ss.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered, MaxMessage: int64(*size)})
			if e != nil {
				out.err = e
				return
			}
			defer ch.Close(context.Background())
			payload := bytes.Repeat([]byte{0x5a}, *size)
			for n := 0; n < *messages; n++ {
				begin := time.Now()
				id, e := ch.Send(ctx, payload)
				if e != nil {
					out.err = e
					return
				}
				m, e := ch.Receive(ctx)
				if e != nil {
					out.err = e
					return
				}
				if !bytes.Equal(m.Payload, payload) {
					out.err = errors.New("payload mismatch")
					return
				}
				if _, e = ch.AwaitReceipt(ctx, id, protocol.AckTransport); e != nil {
					out.err = e
					return
				}
				out.count++
				out.samples = append(out.samples, time.Since(begin))
			}
		}()
	}
	wg.Wait()
	close(results)
	var all []time.Duration
	var done, failed int
	for r := range results {
		done += r.count
		all = append(all, r.samples...)
		if r.err != nil {
			failed++
			log.Printf("benchmark session failed: %v", r.err)
		}
	}
	if len(all) == 0 {
		log.Fatal("zero completed requests")
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	elapsed := time.Since(start)
	value := report{Protocol: "MIRA-T/1-mTLS", Clients: *clients, Messages: *messages, PayloadBytes: *size, Completed: done, Failures: failed, DurationMS: float64(elapsed) / float64(time.Millisecond), RoundTripsPerSecond: float64(done) / elapsed.Seconds(), P50MS: percentile(all, .5), P95MS: percentile(all, .95), P99MS: percentile(all, .99)}
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		log.Fatal(e)
	}
	b = append(b, '\n')
	if *output == "" {
		_, e = os.Stdout.Write(b)
	} else {
		e = os.WriteFile(*output, b, 0600)
	}
	if e != nil {
		log.Fatal(e)
	}
	if failed != 0 || done != *clients**messages {
		os.Exit(1)
	}
}
