package netsim

import (
	"net"
	"testing"
	"time"
)

func echoServer(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := c.ReadFromUDP(buf)
			if err != nil {
				return
			}
			c.WriteToUDP(buf[:n], from)
		}
	}()
	t.Cleanup(func() { c.Close() })
	return c
}

func client(t *testing.T, p *Proxy) *net.UDPConn {
	t.Helper()
	c, err := net.DialUDP("udp", nil, p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestDelayAppliesBothWaysAndPreservesOrder(t *testing.T) {
	srv := echoServer(t)
	p, err := New(srv.LocalAddr().(*net.UDPAddr), Config{OneWayDelay: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := client(t, p)
	start := time.Now()
	const n = 200
	for i := 0; i < n; i++ {
		c.Write([]byte{byte(i)})
	}
	buf := make([]byte, 16)
	for i := 0; i < n; i++ {
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		k, err := c.Read(buf)
		if err != nil || k != 1 {
			t.Fatalf("read %d: %v", i, err)
		}
		if buf[0] != byte(i) {
			t.Fatalf("reordered without jitter: got %d want %d", buf[0], byte(i))
		}
	}
	if rtt := time.Since(start); rtt < 80*time.Millisecond {
		t.Fatalf("round trip %v is below the configured 80 ms", rtt)
	}
}

func TestLossIsReproducible(t *testing.T) {
	count := func() uint64 {
		srv := echoServer(t)
		p, _ := New(srv.LocalAddr().(*net.UDPAddr), Config{Loss: 0.3, Seed: 5})
		defer p.Close()
		c := client(t, p)
		for i := 0; i < 300; i++ {
			c.Write([]byte{1})
		}
		time.Sleep(200 * time.Millisecond)
		return p.Dropped.Load()
	}
	a, b := count(), count()
	if a == 0 || a != b {
		t.Fatalf("drops %d and %d: want identical and non-zero for the same seed", a, b)
	}
}
