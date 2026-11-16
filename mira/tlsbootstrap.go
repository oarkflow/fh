package mira

// TLS bootstrap is a reviewed-handshake deployment profile for the otherwise
// custom UDP transport. TLS 1.3 mutual certificates establish identity and a
// fresh traffic master via RFC 8446 exporters; UDP remains MIRA-T/1 throughout.
// TLS resumption is controlled by crypto/tls (no early data or 0-RTT).

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/oarkflow/fh/internal/wire"
)

const tlsALPN = "mira-t/1-udp-bootstrap"
const tlsExporter = "EXPORTER-MIRA-T1-UDP-v1"

// IdentityAuthorizer admits a verified TLS certificate identity before MIRA
// allocates a session. Implementations must be safe for concurrent calls and
// should be nonblocking.
type IdentityAuthorizer func(identity string) bool

// SecureServer adds an authenticated TLS 1.3 control port to a native MIRA UDP
// server. It rejects the legacy PSK handshake entirely. Only mutual-TLS peers
// with a proven UDP return path are delivered to Accept.
type SecureServer struct {
	*Server
	listener  net.Listener
	authorize IdentityAuthorizer
	stopped   chan struct{}
	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
	limit     chan struct{}
	connMu    sync.Mutex
	conns     map[net.Conn]string
	ipCounts  map[string]int
}

// ListenTLS requires a CA-verified client certificate and TLS 1.3 server
// certificate. tcpAddr and udpAddr may be different ports. Certificates must
// identify each client by a non-empty URI SAN or Subject CommonName.
func ListenTLS(udpAddr, tcpAddr string, cfg *tls.Config, opts Options) (*SecureServer, error) {
	return listenTLS(udpAddr, tcpAddr, cfg, opts, nil)
}

// ListenTLSWithIdentityAuthorizer rejects verified certificates that are not
// admitted by authorize before allocating a MIRA session. The callback may be
// backed by an atomically replaced allowlist.
func ListenTLSWithIdentityAuthorizer(udpAddr, tcpAddr string, cfg *tls.Config, authorize IdentityAuthorizer, opts Options) (*SecureServer, error) {
	if authorize == nil {
		return nil, errors.New("mira: nil TLS identity authorizer")
	}
	return listenTLS(udpAddr, tcpAddr, cfg, opts, authorize)
}

func listenTLS(udpAddr, tcpAddr string, cfg *tls.Config, opts Options, authorize IdentityAuthorizer) (*SecureServer, error) {
	if cfg == nil || len(cfg.Certificates) == 0 || cfg.ClientCAs == nil {
		return nil, errors.New("mira: TLS server certificates and client CA are mandatory")
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, errors.New("mira: TLS client certificates must be required and CA-verified")
	}
	c := cfg.Clone()
	c.MinVersion = tls.VersionTLS13
	c.MaxVersion = tls.VersionTLS13
	c.NextProtos = []string{tlsALPN}
	if getConfig := c.GetConfigForClient; getConfig != nil {
		clientCAs := c.ClientCAs
		verifyPeerCertificate := c.VerifyPeerCertificate
		verifyConnection := c.VerifyConnection
		c.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			selected, err := getConfig(hello)
			if err != nil || selected == nil {
				return selected, err
			}
			selected = selected.Clone()
			// A dynamic SNI config must not weaken this listener's mandatory
			// client-certificate verification or change its TLS/ALPN profile.
			selected.MinVersion = tls.VersionTLS13
			selected.MaxVersion = tls.VersionTLS13
			selected.ClientAuth = tls.RequireAndVerifyClientCert
			if selected.ClientCAs == nil {
				selected.ClientCAs = clientCAs
			}
			if selected.ClientCAs == nil {
				return nil, errors.New("mira: dynamic TLS config has no client CA")
			}
			selected.NextProtos = []string{tlsALPN}
			selected.GetConfigForClient = nil
			if selected.VerifyPeerCertificate == nil {
				selected.VerifyPeerCertificate = verifyPeerCertificate
			}
			if selected.VerifyConnection == nil {
				selected.VerifyConnection = verifyConnection
			}
			return selected, nil
		}
	}
	listener, err := tls.Listen("tcp", tcpAddr, c)
	if err != nil {
		return nil, err
	}
	// Internal resolver deliberately rejects every legacy UDP handshake.
	// The TLS bootstrap installs authenticated sessions directly.
	secureOpts := opts
	secureOpts.AllowLegacyPSKOnPublicIPs = true
	secureOpts.RequireChannelAuthorizer = true
	srv, err := ListenWithResolver(udpAddr, func(string) ([]byte, bool) { return nil, false }, secureOpts)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	// No legacy stateless UDP handshake may create a session in this mode.
	srv.secureOnly.Store(true)
	sec := &SecureServer{Server: srv, listener: listener, authorize: authorize, stopped: make(chan struct{}), limit: make(chan struct{}, 128), conns: make(map[net.Conn]string), ipCounts: make(map[string]int)}
	sec.wg.Add(1)
	go sec.acceptTLS()
	return sec, nil
}

func (s *SecureServer) TLSAddr() net.Addr { return s.listener.Addr() }

func (s *SecureServer) Close() error {
	s.closeOnce.Do(func() {
		close(s.stopped)
		_ = s.listener.Close()
		s.connMu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.connMu.Unlock()
		s.closeErr = s.Server.Close()
		s.wg.Wait()
	})
	return s.closeErr
}

func (s *SecureServer) acceptTLS() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopped:
				return
			default:
			}
			if ne, ok := err.(net.Error); (ok && ne.Timeout()) || errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EINTR) {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			return
		}
		select {
		case <-s.stopped:
			_ = conn.Close()
			return
		case <-s.done:
			_ = conn.Close()
			return
		default:
		}
		remote, ok := conn.RemoteAddr().(*net.TCPAddr)
		if !ok || remote.IP == nil {
			_ = conn.Close()
			continue
		}
		s.tlsHandshakeAttempts.Add(1)
		if !s.Server.admitIP(remote.IP) {
			s.tlsHandshakeLimited.Add(1)
			_ = conn.Close()
			continue
		}
		ip := remote.IP.String()
		select {
		case s.limit <- struct{}{}:
			s.connMu.Lock()
			select {
			case <-s.stopped:
				s.connMu.Unlock()
				<-s.limit
				_ = conn.Close()
				return
			default:
				if s.ipCounts[ip] >= s.opts.MaxTLSHandshakesPerIP {
					s.connMu.Unlock()
					<-s.limit
					s.tlsHandshakeLimited.Add(1)
					_ = conn.Close()
					continue
				}
				s.conns[conn] = ip
				s.ipCounts[ip]++
			}
			s.connMu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() { <-s.limit }()
				defer s.removeTLSConn(conn)
				s.acceptOneTLS(conn)
			}()
		default:
			s.tlsHandshakeLimited.Add(1)
			_ = conn.Close()
		}
	}
}

func (s *SecureServer) removeTLSConn(conn net.Conn) {
	_ = conn.Close()
	s.connMu.Lock()
	if ip := s.conns[conn]; ip != "" {
		delete(s.conns, conn)
		if s.ipCounts[ip] <= 1 {
			delete(s.ipCounts, ip)
		} else {
			s.ipCounts[ip]--
		}
	}
	s.connMu.Unlock()
}

func certIdentity(state tls.ConnectionState) string {
	if len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return ""
	}
	cert := state.PeerCertificates[0]
	if len(cert.URIs) > 0 {
		return cert.URIs[0].String()
	}
	if cert.Subject.CommonName != "" {
		return cert.Subject.CommonName
	}
	// Stable identity for valid client certs without CN or URI SAN.
	hash := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return "spki:" + hex.EncodeToString(hash[:])
}

func (s *SecureServer) acceptOneTLS(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(s.opts.HandshakeTimeout))
	tc, ok := c.(*tls.Conn)
	if !ok || tc.Handshake() != nil {
		return
	}
	state := tc.ConnectionState()
	if state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != tlsALPN {
		return
	}
	identity := certIdentity(state)
	if identity == "" || len(identity) > 256 {
		return
	}
	if s.authorize != nil && !s.authorize(identity) {
		return
	}
	var remoteIP net.IP
	if a, ok := tc.RemoteAddr().(*net.TCPAddr); ok {
		remoteIP = a.IP
	}
	if remoteIP == nil {
		return
	}
	s.mu.RLock()
	_, revoked := s.revoked[identity]
	count := len(s.sessions) + s.pendingHandshakes
	perIdentity := s.identitySlots[identity]
	s.mu.RUnlock()
	if revoked || count >= s.opts.MaxSessions || (s.opts.MaxSessionsPerIdentity > 0 && perIdentity >= s.opts.MaxSessionsPerIdentity) {
		return
	}
	var cidBytes [8]byte
	if _, err := rand.Read(cidBytes[:]); err != nil {
		return
	}
	cid := binary.BigEndian.Uint64(cidBytes[:])
	if cid == 0 {
		return
	}
	exported, err := state.ExportKeyingMaterial(tlsExporter, cidBytes[:], 32)
	if err != nil {
		return
	}
	var schedule trafficSchedule
	defer clear(schedule.master[:])
	copy(schedule.master[:], exported)
	clear(exported)
	schedule.id = cid
	schedule.client = false
	tx, err := schedule.protector(0, true)
	if err != nil {
		return
	}
	rx, err := schedule.protector(0, false)
	if err != nil {
		return
	}
	// The client UDP port is unknown until an encrypted packet proves reachability.
	remote := &net.UDPAddr{IP: remoteIP, Port: 0}
	ss := newSession(s.conn, remote, cid, tx, rx, &schedule, false, s.opts, s.Server)
	ss.identity = identity
	ss.bootstrapTLS = true
	s.mu.Lock()
	_, revoked = s.revoked[identity]
	select {
	case <-s.done:
		s.mu.Unlock()
		return
	default:
	}
	if revoked || s.sessions[cid] != nil || len(s.sessions)+s.pendingHandshakes >= s.opts.MaxSessions {
		s.mu.Unlock()
		return
	}
	if s.opts.MaxSessionsPerIdentity > 0 && s.identitySlots[identity] >= s.opts.MaxSessionsPerIdentity {
		s.mu.Unlock()
		return
	}
	s.sessions[cid] = ss
	s.identitySlots[identity]++
	// Start before publishing the map lock so Close cannot observe an
	// unstarted session without its completion channel or owner goroutine.
	ss.start()
	s.mu.Unlock()
	// TLS only transfers the random connection ID; traffic keys stay inside
	// TLS exporters and never appear in a plaintext bootstrap packet.
	if _, err := tc.Write(cidBytes[:]); err != nil {
		ss.stop()
		<-ss.finished
		return
	}
	s.handshakesAccepted.Add(1)
}

// DialTLS establishes authenticated transport keys with mutual TLS 1.3,
// then opens the MIRA UDP session. serverUDPAddr must be an address whose UDP
// packets come from the same endpoint specified here (typically same host).
// A TLS ClientSessionCache permits TLS 1.3 ticket-based resumption while a new
// exporter produces fresh keys for every new UDP session. No early data.
func DialTLS(ctx context.Context, serverTCPAddr, serverUDPAddr string, cfg *tls.Config, opts Options) (*Session, error) {
	if cfg == nil || len(cfg.Certificates) == 0 || cfg.RootCAs == nil || cfg.ServerName == "" || cfg.InsecureSkipVerify {
		return nil, errors.New("mira: trusted roots, client certificate and ServerName required")
	}
	c := cfg.Clone()
	c.MinVersion = tls.VersionTLS13
	c.MaxVersion = tls.VersionTLS13
	c.NextProtos = []string{tlsALPN}
	o := opts.normalized()
	dialer := &tls.Dialer{Config: c, NetDialer: &net.Dialer{Timeout: o.HandshakeTimeout}}
	bootstrap, err := dialer.DialContext(ctx, "tcp", serverTCPAddr)
	if err != nil {
		return nil, err
	}
	defer bootstrap.Close()
	stopCancel := context.AfterFunc(ctx, func() { _ = bootstrap.Close() })
	defer stopCancel()
	tc, ok := bootstrap.(*tls.Conn)
	if !ok {
		return nil, errors.New("mira: TLS connection required")
	}
	state := tc.ConnectionState()
	if state.Version != tls.VersionTLS13 || state.NegotiatedProtocol != tlsALPN {
		return nil, errors.New("mira: invalid TLS bootstrap negotiation")
	}
	_ = tc.SetReadDeadline(time.Now().Add(o.HandshakeTimeout))
	var cidBytes [8]byte
	if _, err = io.ReadFull(tc, cidBytes[:]); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cid := binary.BigEndian.Uint64(cidBytes[:])
	if cid == 0 {
		return nil, errors.New("mira: zero connection ID")
	}
	exported, err := state.ExportKeyingMaterial(tlsExporter, cidBytes[:], 32)
	if err != nil {
		return nil, err
	}
	var schedule trafficSchedule
	defer clear(schedule.master[:])
	copy(schedule.master[:], exported)
	clear(exported)
	schedule.id = cid
	schedule.client = true
	tx, err := schedule.protector(0, true)
	if err != nil {
		return nil, err
	}
	rx, err := schedule.protector(0, false)
	if err != nil {
		return nil, err
	}
	remote, err := net.ResolveUDPAddr("udp", serverUDPAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	if err := configureUDPBuffers(conn, o); err != nil {
		_ = conn.Close()
		return nil, err
	}
	ss := newSession(conn, remote, cid, tx, rx, &schedule, true, o, nil)
	ss.identity = certIdentity(state) // Client peer identity is the server certificate; primarily diagnostic.
	ss.tlsReady = make(chan struct{})
	ss.start()
	// The session's initial credit frame is an authenticated probe. The
	// server challenges its UDP return path before releasing the session.
	// Wait until a server packet has been received and validated.
	deadline := time.NewTimer(o.HandshakeTimeout)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			ss.stop()
			return nil, ctx.Err()
		case <-deadline.C:
			ss.stop()
			return nil, ErrTimeout
		case <-ss.done:
			return nil, ErrClosed
		case <-ss.tlsReady:
			return ss, nil
		}
	}
}

// Assert standard library dependency only and selected wire framing revision.
var _ = wire.Version
