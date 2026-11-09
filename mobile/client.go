// Package mobile provides a Go-only, gomobile-friendly façade over native
// MIRA sessions. Platform background execution, notifications, and packaging
// require Android/iOS tooling and remain explicit integration requirements.
package mobile

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"net"
	"sync"
	"time"
)

type Client struct {
	mu       sync.RWMutex
	session  *mira.Session
	channels map[int64]protocol.Channel
}

func NewClient() *Client { return &Client{channels: make(map[int64]protocol.Channel)} }
func (c *Client) Connect(tcpAddr, udpAddr, serverName string, rootCA, clientCert, clientKey []byte) error {
	if len(rootCA) == 0 || len(clientCert) == 0 || len(clientKey) == 0 || serverName == "" {
		return errors.New("mobile: TLS credentials required")
	}
	cert, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootCA) {
		return errors.New("mobile: invalid root CA")
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, ClientSessionCache: tls.NewLRUClientSessionCache(16)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ss, err := mira.DialTLS(ctx, tcpAddr, udpAddr, cfg, mira.Options{})
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.session != nil {
		c.mu.Unlock()
		_ = ss.Close(context.Background())
		return errors.New("mobile: already connected")
	}
	c.session = ss
	c.mu.Unlock()
	return nil
}

// ConnectInsecureDevelopment is deliberately limited to loopback networks.
// Do not deploy shared-PSK authentication for public mobile clients.
func (c *Client) ConnectInsecureDevelopment(udpAddr string, psk []byte) error {
	target, err := net.ResolveUDPAddr("udp", udpAddr)
	if err != nil {
		return err
	}
	if target.IP == nil || !target.IP.IsLoopback() {
		return errors.New("mobile: legacy PSK is limited to loopback")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ss, err := mira.Dial(ctx, udpAddr, psk, mira.Options{})
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.session != nil {
		c.mu.Unlock()
		_ = ss.Close(context.Background())
		return errors.New("mobile: already connected")
	}
	c.session = ss
	c.mu.Unlock()
	return nil
}
func (c *Client) current() (*mira.Session, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.session == nil {
		return nil, errors.New("mobile: not connected")
	}
	return c.session, nil
}
func (c *Client) OpenChannel(delivery int) (int64, error) {
	ss, err := c.current()
	if err != nil {
		return 0, err
	}
	if delivery < int(protocol.ReliableOrdered) || delivery > int(protocol.Bulk) {
		return 0, errors.New("mobile: invalid delivery mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, err := ss.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.Delivery(delivery)})
	if err != nil {
		return 0, err
	}
	id := int64(ch.ID())
	c.mu.Lock()
	c.channels[id] = ch
	c.mu.Unlock()
	return id, nil
}
func (c *Client) AcceptChannel(timeoutMillis int64) (int64, error) {
	ss, err := c.current()
	if err != nil {
		return 0, err
	}
	ctx, cancel := timeout(timeoutMillis)
	defer cancel()
	ch, err := ss.AcceptChannel(ctx)
	if err != nil {
		return 0, err
	}
	id := int64(ch.ID())
	c.mu.Lock()
	c.channels[id] = ch
	c.mu.Unlock()
	return id, nil
}
func (c *Client) channel(id int64) (protocol.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ch := c.channels[id]
	if ch == nil {
		return nil, errors.New("mobile: unknown channel")
	}
	return ch, nil
}
func timeout(ms int64) (context.Context, context.CancelFunc) {
	if ms <= 0 {
		ms = 10000
	}
	if ms > 120000 {
		ms = 120000
	}
	return context.WithTimeout(context.Background(), time.Duration(ms)*time.Millisecond)
}
func (c *Client) Send(id int64, payload []byte) (int64, error) {
	ch, err := c.channel(id)
	if err != nil {
		return 0, err
	}
	ctx, cancel := timeout(10000)
	defer cancel()
	n, err := ch.Send(ctx, payload)
	return int64(n), err
}
func (c *Client) Receive(id, timeoutMillis int64) ([]byte, error) {
	ch, err := c.channel(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := timeout(timeoutMillis)
	defer cancel()
	m, err := ch.Receive(ctx)
	if err != nil {
		return nil, err
	}
	return m.Payload, nil
}
func (c *Client) WaitTransportReceipt(id, message int64, timeoutMillis int64) error {
	ch, err := c.channel(id)
	if err != nil {
		return err
	}
	ctx, cancel := timeout(timeoutMillis)
	defer cancel()
	_, err = ch.AwaitReceipt(ctx, uint64(message), protocol.AckTransport)
	return err
}
func (c *Client) Rebind() error {
	ss, err := c.current()
	if err != nil {
		return err
	}
	ctx, cancel := timeout(5000)
	defer cancel()
	return ss.RebindClient(ctx)
}

// RebindAndWait verifies that an authenticated packet was acknowledged
// after a mobile network interface change. Call this when the OS reports
// Wi-Fi/cellular handover; apps must also implement offline push handling.
func (c *Client) RebindAndWait(timeoutMillis int64) error {
	ss, err := c.current()
	if err != nil {
		return err
	}
	ctx, cancel := timeout(timeoutMillis)
	defer cancel()
	return ss.RebindAndWait(ctx)
}

// TransportStatsJSON is a bounded, secret-free diagnostic snapshot that can
// be consumed through gomobile bindings without Go struct marshaling.
func (c *Client) TransportStatsJSON() (string, error) {
	ss, err := c.current()
	if err != nil {
		return "", err
	}
	ctx, cancel := timeout(2000)
	defer cancel()
	stats, err := ss.Stats(ctx)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(stats)
	return string(b), err
}

func (c *Client) CloseChannel(id int64) error {
	ch, err := c.channel(id)
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.channels, id)
	c.mu.Unlock()
	ctx, cancel := timeout(3000)
	defer cancel()
	return ch.Close(ctx)
}
func (c *Client) Close() error {
	c.mu.Lock()
	ss := c.session
	c.session = nil
	c.channels = make(map[int64]protocol.Channel)
	c.mu.Unlock()
	if ss == nil {
		return nil
	}
	ctx, cancel := timeout(3000)
	defer cancel()
	return ss.Close(ctx)
}
