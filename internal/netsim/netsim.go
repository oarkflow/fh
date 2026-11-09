// Package netsim is a small UDP network emulator for tests: a proxy that sits
// between one client and a server and imposes one-way delay, jitter, random
// loss and an optional rate-limited bottleneck with a bounded queue. It exists
// so transport behavior on long or lossy paths (where slow start and loss
// recovery matter) can be reproduced on a laptop in seconds.
package netsim

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config describes the emulated path. Zero values mean "no impairment".
type Config struct {
	OneWayDelay time.Duration // applied in each direction (RTT = 2x)
	Jitter      time.Duration // uniform extra delay in [0, Jitter); causes reordering
	Loss        float64       // independent drop probability per datagram, each direction
	Seed        int64         // makes loss/jitter reproducible (0 = 1)
	// RateBytesPerSec, when > 0, models a bottleneck link shared by both
	// directions' traffic toward the server and back: datagrams are serialized
	// at this rate and tail-dropped once QueueBytes are waiting.
	RateBytesPerSec int64
	QueueBytes      int64 // bottleneck queue limit (default 64 KiB when rate-limited)
}

// Proxy forwards datagrams between one client and the destination.
type Proxy struct {
	cfg      Config
	front    *net.UDPConn // client talks to this
	back     *net.UDPConn // server sees this as the client
	dest     *net.UDPAddr
	mu       sync.Mutex
	client   *net.UDPAddr
	rng      [2]*rand.Rand // one per direction so concurrent pumps stay deterministic
	nextFree [2]time.Time  // bottleneck departure clock per direction
	queued   [2]int64
	wg       sync.WaitGroup
	closed   atomic.Bool
	qmu      sync.Mutex
	queue    [2][]delivery
	wake     [2]chan struct{}
	done     chan struct{}

	Forwarded atomic.Uint64
	Dropped   atomic.Uint64
}

// New starts a proxy on a loopback port in front of dest.
func New(dest *net.UDPAddr, cfg Config) (*Proxy, error) {
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	back, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		front.Close()
		return nil, err
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = 1
	}
	if cfg.RateBytesPerSec > 0 && cfg.QueueBytes == 0 {
		cfg.QueueBytes = 64 << 10
	}
	p := &Proxy{cfg: cfg, front: front, back: back, dest: dest, rng: [2]*rand.Rand{rand.New(rand.NewSource(seed)), rand.New(rand.NewSource(seed + 1))},
		wake: [2]chan struct{}{make(chan struct{}, 1), make(chan struct{}, 1)}, done: make(chan struct{})}
	p.wg.Add(4)
	go p.pump(front, 0)
	go p.pump(back, 1)
	go p.deliver(0)
	go p.deliver(1)
	return p, nil
}

// Addr is the address the client should use as its server UDP address.
func (p *Proxy) Addr() *net.UDPAddr { return p.front.LocalAddr().(*net.UDPAddr) }

// Close stops the proxy. In-flight delayed datagrams are discarded.
func (p *Proxy) Close() {
	p.closed.Store(true)
	close(p.done)
	p.front.Close()
	p.back.Close()
	p.wg.Wait()
}

// pump reads from one socket; dir 0 is client->server, dir 1 is server->client.
func (p *Proxy) pump(c *net.UDPConn, dir int) {
	defer p.wg.Done()
	buf := make([]byte, 2048)
	for {
		n, from, err := c.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if dir == 0 {
			p.mu.Lock()
			p.client = from
			p.mu.Unlock()
		}
		pkt := append([]byte(nil), buf[:n]...)
		wait, ok := p.admit(dir, n)
		if !ok {
			p.Dropped.Add(1)
			continue
		}
		p.enqueue(dir, delivery{due: time.Now().Add(wait), pkt: pkt})
	}
}

type delivery struct {
	due time.Time
	pkt []byte
}

// enqueue inserts d into the direction's queue ordered by due time (stable for
// equal times), so datagrams are only reordered when jitter makes them so.
func (p *Proxy) enqueue(dir int, d delivery) {
	p.qmu.Lock()
	q := p.queue[dir]
	i := len(q)
	for i > 0 && q[i-1].due.After(d.due) {
		i--
	}
	q = append(q, delivery{})
	copy(q[i+1:], q[i:])
	q[i] = d
	p.queue[dir] = q
	p.qmu.Unlock()
	select {
	case p.wake[dir] <- struct{}{}:
	default:
	}
}

// deliver is the single sender goroutine for one direction.
func (p *Proxy) deliver(dir int) {
	defer p.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		p.qmu.Lock()
		var next *delivery
		if len(p.queue[dir]) > 0 {
			next = &p.queue[dir][0]
		}
		var wait time.Duration
		if next != nil {
			wait = time.Until(next.due)
		}
		if next != nil && wait <= 0 {
			d := p.queue[dir][0]
			p.queue[dir] = p.queue[dir][1:]
			p.qmu.Unlock()
			p.release(dir, len(d.pkt))
			if p.closed.Load() {
				return
			}
			if dir == 0 {
				_, _ = p.back.WriteToUDP(d.pkt, p.dest)
			} else {
				p.mu.Lock()
				client := p.client
				p.mu.Unlock()
				if client != nil {
					_, _ = p.front.WriteToUDP(d.pkt, client)
				}
			}
			p.Forwarded.Add(1)
			continue
		}
		p.qmu.Unlock()
		if next == nil {
			wait = time.Hour
		}
		timer.Reset(wait)
		select {
		case <-p.wake[dir]:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		case <-p.done:
			return
		}
	}
}

// admit decides whether a datagram survives and how long it is delayed.
func (p *Proxy) admit(dir, size int) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cfg.Loss > 0 && p.rng[dir].Float64() < p.cfg.Loss {
		return 0, false
	}
	wait := p.cfg.OneWayDelay
	if p.cfg.Jitter > 0 {
		wait += time.Duration(p.rng[dir].Int63n(int64(p.cfg.Jitter)))
	}
	if p.cfg.RateBytesPerSec > 0 {
		if p.queued[dir]+int64(size) > p.cfg.QueueBytes {
			return 0, false // tail drop
		}
		now := time.Now()
		start := p.nextFree[dir]
		if start.Before(now) {
			start = now
		}
		serialize := time.Duration(int64(size) * int64(time.Second) / p.cfg.RateBytesPerSec)
		p.nextFree[dir] = start.Add(serialize)
		p.queued[dir] += int64(size)
		wait += p.nextFree[dir].Sub(now)
	}
	return wait, true
}

func (p *Proxy) release(dir, size int) {
	if p.cfg.RateBytesPerSec > 0 {
		p.mu.Lock()
		p.queued[dir] -= int64(size)
		p.mu.Unlock()
	}
}
