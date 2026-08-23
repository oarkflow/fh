package fh

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	// proxyV2Signature is the 12-byte signature for PROXY protocol v2.
	proxyV2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

	ErrInvalidProxyHeader = errors.New("proxyprotocol: invalid or unsupported proxy header")
	ErrProxyReadTimeout   = errors.New("proxyprotocol: read timeout on header")
)

// ProxyProtocolConfig configures PROXY protocol negotiation.
type ProxyProtocolConfig struct {
	// Timeout is the maximum time to wait for the PROXY header.
	// Defaults to 5 seconds.
	Timeout time.Duration

	// FallbackPassthrough allows connections without a PROXY header to be served as normal.
	// Defaults to false (strict mode).
	FallbackPassthrough bool
}

// ProxyConn wraps a net.Conn whose RemoteAddr has been replaced by the PROXY header.
type proxyConn struct {
	net.Conn
	remoteAddr net.Addr
	reader     io.Reader
}

func (c *proxyConn) RemoteAddr() net.Addr {
	if c.remoteAddr != nil {
		return c.remoteAddr
	}
	return c.Conn.RemoteAddr()
}

func (c *proxyConn) Read(b []byte) (int, error) {
	if c.reader != nil {
		return c.reader.Read(b)
	}
	return c.Conn.Read(b)
}

// ProxyProtocolListener wraps an underlying net.Listener and decodes PROXY headers on accept.
type ProxyProtocolListener struct {
	net.Listener
	cfg ProxyProtocolConfig
}

// NewProxyProtocolListener wraps ln with PROXY protocol v1 and v2 support.
func NewProxyProtocolListener(ln net.Listener, cfg ...ProxyProtocolConfig) *ProxyProtocolListener {
	c := ProxyProtocolConfig{
		Timeout: 5 * time.Second,
	}
	if len(cfg) > 0 {
		if cfg[0].Timeout > 0 {
			c.Timeout = cfg[0].Timeout
		}
		c.FallbackPassthrough = cfg[0].FallbackPassthrough
	}
	return &ProxyProtocolListener{
		Listener: ln,
		cfg:      c,
	}
}

// Accept waits for and returns the next connection to the listener, parsing its PROXY header.
func (l *ProxyProtocolListener) Accept() (net.Conn, error) {
	rawConn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	if l.cfg.Timeout > 0 {
		_ = rawConn.SetReadDeadline(time.Now().Add(l.cfg.Timeout))
	}

	bufr := bufio.NewReader(rawConn)
	remoteAddr, err := parseProxyHeader(bufr, rawConn.RemoteAddr(), l.cfg.FallbackPassthrough)

	if l.cfg.Timeout > 0 {
		_ = rawConn.SetReadDeadline(time.Time{})
	}

	if err != nil {
		_ = rawConn.Close()
		return nil, err
	}

	return &proxyConn{
		Conn:       rawConn,
		remoteAddr: remoteAddr,
		reader:     bufr,
	}, nil
}

func parseProxyHeader(bufr *bufio.Reader, fallback net.Addr, fallbackPassthrough bool) (net.Addr, error) {
	// Peek at the first few bytes to determine v1 or v2
	magic, err := bufr.Peek(16)
	if err != nil && len(magic) < 8 {
		if fallbackPassthrough {
			return fallback, nil
		}
		return nil, err
	}

	// 1. Check for PROXY protocol v2 (binary, 12-byte magic prefix)
	if len(magic) >= 12 && bytes.Equal(magic[:12], proxyV2Signature) {
		return parseProxyV2(bufr)
	}

	// 2. Check for PROXY protocol v1 (text: "PROXY ...\r\n")
	if len(magic) >= 6 && bytes.Equal(magic[:6], []byte("PROXY ")) {
		return parseProxyV1(bufr)
	}

	if fallbackPassthrough {
		return fallback, nil
	}
	return nil, ErrInvalidProxyHeader
}

// parseProxyV1 parses PROXY v1 text header format:
// PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n
func parseProxyV1(bufr *bufio.Reader) (net.Addr, error) {
	line, err := bufr.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return nil, ErrInvalidProxyHeader
	}

	if parts[1] == "UNKNOWN" {
		return nil, nil
	}

	if len(parts) != 6 {
		return nil, ErrInvalidProxyHeader
	}

	proto := parts[1]
	srcIPStr := parts[2]
	srcPortStr := parts[4]

	srcIP := net.ParseIP(srcIPStr)
	if srcIP == nil {
		return nil, ErrInvalidProxyHeader
	}

	srcPort, err := strconv.Atoi(srcPortStr)
	if err != nil || srcPort < 0 || srcPort > 65535 {
		return nil, ErrInvalidProxyHeader
	}

	switch proto {
	case "TCP4", "TCP6":
		return &net.TCPAddr{
			IP:   srcIP,
			Port: srcPort,
		}, nil
	default:
		return nil, ErrInvalidProxyHeader
	}
}

// parseProxyV2 parses PROXY v2 binary header format.
func parseProxyV2(bufr *bufio.Reader) (net.Addr, error) {
	// Skip 12-byte magic signature
	_, err := bufr.Discard(12)
	if err != nil {
		return nil, err
	}

	// Read version/command (1 byte) and family/transport (1 byte)
	var header [4]byte
	if _, err := io.ReadFull(bufr, header[:]); err != nil {
		return nil, err
	}

	verCmd := header[0]
	famTrans := header[1]
	length := binary.BigEndian.Uint16(header[2:4])

	ver := (verCmd & 0xF0) >> 4
	cmd := verCmd & 0x0F

	if ver != 2 {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrInvalidProxyHeader, ver)
	}

	// Command: 0 = LOCAL (ignore addresses), 1 = PROXY
	if cmd == 0 {
		_, _ = bufr.Discard(int(length))
		return nil, nil
	}
	if cmd != 1 {
		return nil, fmt.Errorf("%w: unknown command %d", ErrInvalidProxyHeader, cmd)
	}

	addrBytes := make([]byte, length)
	if _, err := io.ReadFull(bufr, addrBytes); err != nil {
		return nil, err
	}

	family := (famTrans & 0xF0) >> 4
	switch family {
	case 1: // AF_INET (IPv4) - 4 src, 4 dst, 2 src port, 2 dst port = 12 bytes
		if len(addrBytes) < 12 {
			return nil, ErrInvalidProxyHeader
		}
		srcIP := net.IP(addrBytes[0:4])
		srcPort := int(binary.BigEndian.Uint16(addrBytes[8:10]))
		return &net.TCPAddr{
			IP:   srcIP,
			Port: srcPort,
		}, nil

	case 2: // AF_INET6 (IPv6) - 16 src, 16 dst, 2 src port, 2 dst port = 36 bytes
		if len(addrBytes) < 36 {
			return nil, ErrInvalidProxyHeader
		}
		srcIP := net.IP(addrBytes[0:16])
		srcPort := int(binary.BigEndian.Uint16(addrBytes[32:34]))
		return &net.TCPAddr{
			IP:   srcIP,
			Port: srcPort,
		}, nil

	case 0: // AF_UNSPEC
		return nil, nil

	default:
		return nil, nil
	}
}

// ListenProxyProtocol starts listening on addr with PROXY protocol v1/v2 support.
func (a *App) ListenProxyProtocol(addr string, cfg ...ProxyProtocolConfig) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return a.Serve(NewProxyProtocolListener(ln, cfg...))
}

// ListenTLSProxyProtocol starts listening on addr with TLS and PROXY protocol v1/v2 support.
func (a *App) ListenTLSProxyProtocol(addr, certFile, keyFile string, cfg ...ProxyProtocolConfig) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tlsConfig, err := a.prepareTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	proxyLn := NewProxyProtocolListener(ln, cfg...)
	return a.Serve(tls.NewListener(proxyLn, tlsConfig))
}
