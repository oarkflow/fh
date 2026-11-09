package mira

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/internal/cryptokit"
	"github.com/oarkflow/fh/internal/wire"
	"github.com/oarkflow/fh/protocol"
)

type ipRate struct {
	tokens float64
	last   time.Time
}

type handshake struct {
	received        time.Time
	hello, helloAck []byte
	s               *Session
	ready           []byte
	established     bool
}

// Server accepts mutually PSK-authenticated MIRA-T/1 sessions on a UDP socket.
// PSKResolver resolves independently provisioned, high-entropy device secrets.
// Calls must be nonblocking: the UDP packet reader invokes it synchronously.
// Use an in-memory snapshot or cache, not a remote database per handshake.
type PSKResolver func(identity string) ([]byte, bool)

// ChannelAuthorizer evaluates inbound remote channel requests. It must be
// thread-safe and nonblocking; it executes on the session worker. Denying a
// request sends an authenticated reset. No callback means permit for legacy
// development compatibility; production deployments MUST configure one.
type ChannelAuthorizer func(identity string, cfg protocol.ChannelConfig) bool

type Server struct {
	secureOnly         atomic.Bool
	conn               *net.UDPConn
	opts               Options
	psk                []byte
	resolver           PSKResolver
	done               chan struct{}
	once               sync.Once
	wg                 sync.WaitGroup
	mu                 sync.RWMutex
	handshakes         map[uint64]*handshake
	sessions           map[uint64]*Session
	accepts            chan *Session
	ipRates            map[string]*ipRate
	revoked            map[string]struct{}
	authorize          ChannelAuthorizer
	handshakeAttempts  atomic.Uint64
	handshakesAccepted atomic.Uint64
	datagramsRead      atomic.Uint64
	datagramsDropped   atomic.Uint64
	payloadBytesRead   atomic.Uint64
}

func Listen(addr string, secret []byte, opts Options) (*Server, error) {
	if len(secret) < 32 {
		return nil, ErrInvalidSecret
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	if (udpAddr.IP == nil || !udpAddr.IP.IsLoopback()) && !opts.AllowLegacyPSKOnPublicIPs {
		return nil, errors.New("mira: legacy PSK listener is loopback-only; use ListenTLS for production")
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	s := &Server{conn: conn, opts: opts.normalized(), psk: append([]byte(nil), secret...),
		done: make(chan struct{}), handshakes: make(map[uint64]*handshake), sessions: make(map[uint64]*Session), accepts: make(chan *Session, 64), ipRates: make(map[string]*ipRate), revoked: make(map[string]struct{})}
	s.wg.Add(1)
	go s.readLoop()
	return s, nil
}

// ListenWithResolver accepts clients provisioned with distinct device keys.
// Revocation takes effect for new connections; existing sessions require an
// application-level policy decision or an administrative disconnect.
func ListenWithResolver(addr string, resolve PSKResolver, opts Options) (*Server, error) {
	if resolve == nil {
		return nil, errors.New("mira: nil credential resolver")
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	if (udpAddr.IP == nil || !udpAddr.IP.IsLoopback()) && !opts.AllowLegacyPSKOnPublicIPs {
		return nil, errors.New("mira: legacy PSK listener is loopback-only; use ListenTLS for production")
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	s := &Server{conn: conn, opts: opts.normalized(), resolver: resolve, done: make(chan struct{}),
		handshakes: make(map[uint64]*handshake), sessions: make(map[uint64]*Session), accepts: make(chan *Session, 64), ipRates: make(map[string]*ipRate), revoked: make(map[string]struct{})}
	s.wg.Add(1)
	go s.readLoop()
	return s, nil
}

// SetChannelAuthorizer changes the policy for all subsequent remote opens.
func (s *Server) SetChannelAuthorizer(check ChannelAuthorizer) {
	s.mu.Lock()
	s.authorize = check
	s.mu.Unlock()
}

func (s *Server) allowChannel(identity string, cfg protocol.ChannelConfig) bool {
	s.mu.RLock()
	check := s.authorize
	_, denied := s.revoked[identity]
	s.mu.RUnlock()
	if denied {
		return false
	}
	if check == nil {
		return !s.opts.RequireChannelAuthorizer
	}
	return check(identity, cfg)
}

// RevokeIdentity denies new sessions and channel opens and disconnects existing
// sessions belonging to one identity. Persist the revocation in the credential
// provider too if it must survive server restarts.
func (s *Server) RevokeIdentity(identity string) int {
	if identity == "" {
		return 0
	}
	s.mu.Lock()
	s.revoked[identity] = struct{}{}
	sessions := make([]*Session, 0)
	for id, hs := range s.handshakes {
		if hs.s.identity == identity {
			delete(s.handshakes, id)
		}
	}
	for _, sess := range s.sessions {
		if sess.identity == identity {
			sessions = append(sessions, sess)
		}
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.disconnect()
	}
	return len(sessions)
}

// AllowIdentity clears an in-memory block. The credential resolver still
// determines whether that identity is allowed to authenticate.
func (s *Server) AllowIdentity(identity string) {
	s.mu.Lock()
	delete(s.revoked, identity)
	s.mu.Unlock()
}

// ServerStats contains only bounded metadata and does not expose secrets.
type ServerStats struct {
	ActiveSessions     int
	PendingHandshakes  int
	BlockedIdentities  int
	TotalSessionSlots  int
	HandshakeAttempts  uint64
	HandshakesAccepted uint64
	DatagramsRead      uint64
	DatagramsDropped   uint64
	PayloadBytesRead   uint64
}

func (s *Server) Stats() ServerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pending := 0
	for _, hs := range s.handshakes {
		if !hs.established {
			pending++
		}
	}
	return ServerStats{
		ActiveSessions: len(s.sessions), PendingHandshakes: pending,
		BlockedIdentities: len(s.revoked), TotalSessionSlots: s.opts.MaxSessions,
		HandshakeAttempts: s.handshakeAttempts.Load(), HandshakesAccepted: s.handshakesAccepted.Load(), DatagramsRead: s.datagramsRead.Load(), DatagramsDropped: s.datagramsDropped.Load(), PayloadBytesRead: s.payloadBytesRead.Load(),
	}
}

// WritePrometheus exports only aggregate counters, never device identities,
// session keys, connection IDs, payloads or packet capture data. Bind the
// monitoring listener to a trusted loopback or management interface.
func (s *Server) WritePrometheus(w io.Writer) error {
	m := s.Stats()
	for _, line := range []struct {
		name string
		typ  string
		v    uint64
	}{
		{"mira_active_sessions", "gauge", uint64(m.ActiveSessions)},
		{"mira_pending_handshakes", "gauge", uint64(m.PendingHandshakes)},
		{"mira_handshake_attempts_total", "counter", m.HandshakeAttempts},
		{"mira_handshakes_accepted_total", "counter", m.HandshakesAccepted},
		{"mira_datagrams_read_total", "counter", m.DatagramsRead},
		{"mira_datagrams_dropped_total", "counter", m.DatagramsDropped},
		{"mira_payload_bytes_read_total", "counter", m.PayloadBytesRead},
	} {
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", line.name, line.typ, line.name, line.v); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) Addr() net.Addr { return s.conn.LocalAddr() }
func (s *Server) Accept(ctx context.Context) (*Session, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrClosed
	case sess := <-s.accepts:
		return sess, nil
	}
}
func (s *Server) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		snapshot := make([]*Session, 0, len(s.sessions))
		for _, sess := range s.sessions {
			snapshot = append(snapshot, sess)
		}
		s.mu.Unlock()
		for _, sess := range snapshot {
			sess.disconnect()
		}
		deadline := time.Now().Add(2 * time.Second)
		for _, sess := range snapshot {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				sess.stop()
				continue
			}
			select {
			case <-sess.finished:
			case <-time.After(remaining):
				sess.stop()
			}
		}
		_ = s.conn.Close()
	})
	s.wg.Wait()
	return nil
}
func (s *Server) removeSession(id uint64) {
	s.mu.Lock()
	delete(s.sessions, id)
	delete(s.handshakes, id)
	s.mu.Unlock()
}

func addrEqual(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.Zone == b.Zone && a.IP.Equal(b.IP)
}

func (s *Server) readLoop() {
	defer s.wg.Done()
	buf := make([]byte, wire.MaxDatagram+1)
	sweep := time.Now()
	for {
		select {
		case <-s.done:
			return
		default:
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-s.done:
				return
			default:
			}
			var ne net.Error
			if !errors.As(err, &ne) || !ne.Timeout() {
				continue
			}
		} else if n >= wire.HeaderSize && n <= wire.MaxDatagram {
			s.datagramsRead.Add(1)
			s.payloadBytesRead.Add(uint64(n))
			p := append([]byte(nil), buf[:n]...)
			h, e := wire.ParseHeader(p)
			if e == nil {
				switch h.Kind {
				case wire.KindHello:
					s.onHello(h, p, addr)
				case wire.KindFinish:
					s.onFinish(h, p, addr)
				case wire.KindData:
					s.mu.RLock()
					ss := s.sessions[h.Connection]
					s.mu.RUnlock()
					if ss != nil {
						select {
						case ss.incoming <- inboundPacket{raw: p, from: addr}:
						default:
							s.datagramsDropped.Add(1)
						}
					} else {
						s.datagramsDropped.Add(1)
					}
				}
			}
		}
		if time.Since(sweep) > time.Second*2 {
			s.expireHandshakes()
			sweep = time.Now()
		}
	}
}
func (s *Server) expireHandshakes() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ip, rate := range s.ipRates {
		if time.Since(rate.last) > time.Minute {
			delete(s.ipRates, ip)
		}
	}
	for cid, hs := range s.handshakes {
		if !hs.established && time.Since(hs.received) > s.opts.HandshakeTimeout {
			delete(s.handshakes, cid)
		}
		if hs.established && time.Since(hs.received) > 10*time.Second {
			delete(s.handshakes, cid)
		}
	}
}

// admitIP enforces a bounded, per-source token bucket before cryptographic work.
// Called only from the server UDP read loop.
func (s *Server) admitIP(addr *net.UDPAddr) bool {
	key := addr.IP.String()
	rate := s.ipRates[key]
	now := time.Now()
	if rate == nil {
		if len(s.ipRates) >= 8192 {
			return false
		}
		s.ipRates[key] = &ipRate{tokens: 99, last: now}
		return true
	}
	rate.tokens += now.Sub(rate.last).Seconds() * 50
	if rate.tokens > 100 {
		rate.tokens = 100
	}
	rate.last = now
	if rate.tokens < 1 {
		return false
	}
	rate.tokens--
	return true
}
func (s *Server) onHello(h wire.Header, p []byte, addr *net.UDPAddr) {
	if s.secureOnly.Load() {
		return
	}
	s.handshakeAttempts.Add(1)
	if !s.admitIP(addr) {
		return
	}
	if h.Number != 0 || h.Flags != 0 || h.Epoch != 0 {
		return
	}
	identity, clientPart, clientPub, ok := parseHelloBody(p[wire.HeaderSize:])
	if !ok {
		return
	}
	secret := s.psk
	if s.resolver != nil {
		if identity == "" {
			return
		}
		var exists bool
		secret, exists = s.resolver(identity)
		if !exists || len(secret) < 32 {
			return
		}
		// Resolver-owned key buffers may be reused/rotated by the caller.
		// Take an independent copy so this handshake does not race that store.
		secret = append([]byte(nil), secret...)
		defer clear(secret)
	} else if identity != "" {
		return
	}
	s.mu.RLock()
	_, denied := s.revoked[identity]
	s.mu.RUnlock()
	if denied {
		return
	}
	if !checkHello(secret, p[wire.HeaderSize:], h.Connection) {
		return
	}
	s.mu.RLock()
	hs := s.handshakes[h.Connection]
	active := s.sessions[h.Connection]
	n := len(s.sessions)
	for _, h := range s.handshakes {
		if !h.established {
			n++
		}
	}
	s.mu.RUnlock()
	if hs != nil {
		if sameHello(hs.hello, p) && addrEqual(hs.s.getRemote(), addr) {
			_, _ = s.conn.WriteToUDP(hs.helloAck, addr)
		}
		return
	}
	if active != nil || n >= s.opts.MaxSessions {
		return
	}
	if s.opts.MaxSessionsPerIdentity > 0 && identity != "" {
		count := 0
		s.mu.RLock()
		for _, v := range s.sessions {
			if v.identity == identity {
				count++
			}
		}
		for _, v := range s.handshakes {
			if !v.established && v.s.identity == identity {
				count++
			}
		}
		s.mu.RUnlock()
		if count >= s.opts.MaxSessionsPerIdentity {
			return
		}
	}
	key, err := newEphemeral()
	if err != nil {
		return
	}
	var nonce [16]byte
	if randomBytes(nonce[:]) != nil {
		return
	}
	ack, err := makeHelloAck(secret, p, h.Connection, key, nonce)
	if err != nil {
		return
	}
	tx, rx, schedule, err := derive(secret, key, clientPub, clientPart, ack[wire.HeaderSize:wire.HeaderSize+48], h.Connection, false)
	if err != nil {
		return
	}
	ss := newSession(s.conn, addr, h.Connection, tx, rx, schedule, false, s.opts, s)
	ss.identity = identity
	hs = &handshake{received: time.Now(), hello: p, helloAck: ack, s: ss}
	s.mu.Lock()
	activeCount := len(s.sessions)
	for _, h := range s.handshakes {
		if !h.established {
			activeCount++
		}
	}
	_, denied = s.revoked[identity]
	if denied || s.handshakes[h.Connection] != nil || s.sessions[h.Connection] != nil || activeCount >= s.opts.MaxSessions {
		s.mu.Unlock()
		return
	}
	if s.opts.MaxSessionsPerIdentity > 0 && identity != "" {
		count := 0
		for _, v := range s.sessions {
			if v.identity == identity {
				count++
			}
		}
		for _, v := range s.handshakes {
			if !v.established && v.s.identity == identity {
				count++
			}
		}
		if count >= s.opts.MaxSessionsPerIdentity {
			s.mu.Unlock()
			return
		}
	}
	s.handshakes[h.Connection] = hs
	s.mu.Unlock()
	_, _ = s.conn.WriteToUDP(ack, addr)
}
func (s *Server) onFinish(h wire.Header, p []byte, addr *net.UDPAddr) {
	if h.Number != 1 || h.Epoch != 0 || h.Flags != 0 {
		return
	}
	s.mu.RLock()
	hs := s.handshakes[h.Connection]
	s.mu.RUnlock()
	if hs == nil || !addrEqual(hs.s.getRemote(), addr) {
		return
	}
	s.mu.RLock()
	_, denied := s.revoked[hs.s.identity]
	s.mu.RUnlock()
	if denied {
		return
	}
	_, plain, err := opened(hs.s.rx, p, wire.KindFinish)
	if err != nil || string(plain) != "MIRA-T/1 CLIENT FINISH" {
		return
	}
	s.mu.RLock()
	already := hs.established
	previousReady := hs.ready
	s.mu.RUnlock()
	if already {
		_, _ = s.conn.WriteToUDP(previousReady, addr)
		return
	}
	frame, err := wire.AppendFrame(nil, wire.Frame{Type: wire.FramePing, Body: []byte("READY")})
	if err != nil {
		return
	}
	ready, err := sealed(hs.s.tx, h.Connection, 1, wire.KindData, 0, frame)
	if err != nil {
		return
	}
	s.mu.Lock()
	if hs.established {
		s.mu.Unlock()
		return
	}
	hs.ready = ready
	hs.established = true
	s.sessions[h.Connection] = hs.s
	s.mu.Unlock()
	s.handshakesAccepted.Add(1)
	hs.s.start()
	_, _ = s.conn.WriteToUDP(ready, addr)
	select {
	case s.accepts <- hs.s:
	default:
		hs.s.stop()
	}
}

// Dial establishes an encrypted session. The same secret must be provisioned
// securely to both endpoints; it must not be a human password.
func Dial(ctx context.Context, addr string, secret []byte, opts Options) (*Session, error) {
	return DialWithIdentity(ctx, addr, "", secret, opts)
}

// DialWithIdentity proves possession of a unique secret assigned to identity.
func DialWithIdentity(ctx context.Context, addr, identity string, secret []byte, opts Options) (*Session, error) {
	if len(secret) < 32 {
		return nil, ErrInvalidSecret
	}
	o := opts.normalized()
	target, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	if (target.IP == nil || !target.IP.IsLoopback()) && !opts.AllowLegacyPSKOnPublicIPs {
		return nil, errors.New("mira: legacy PSK dial is loopback-only; use DialTLS")
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	// Deadline and context cancellation must interrupt blocked UDP reads even
	// if a platform fails to wake reliably for repeated SetReadDeadline calls.
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, o.HandshakeTimeout)
	defer cancelHandshake()
	originalConn := conn
	var stateMu sync.Mutex
	transferred := false
	stopInterrupt := context.AfterFunc(handshakeCtx, func() {
		stateMu.Lock()
		if !transferred {
			_ = originalConn.Close()
		}
		stateMu.Unlock()
	})
	defer stopInterrupt()
	handshakeError := func() error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrTimeout
	}
	var cidB [8]byte
	if _, err = rand.Read(cidB[:]); err != nil {
		return nil, err
	}
	cid := binary.BigEndian.Uint64(cidB[:])
	if cid == 0 {
		cid = 1
	}
	key, err := newEphemeral()
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if randomBytes(nonce[:]) != nil {
		return nil, errors.New("mira: secure randomness unavailable")
	}
	hello, err := makeHello(secret, cid, key, nonce, identity)
	if err != nil {
		return nil, err
	}
	deadline, _ := handshakeCtx.Deadline()
	buf := make([]byte, wire.MaxDatagram+1)
	var ack []byte
	for time.Now().Before(deadline) {
		select {
		case <-handshakeCtx.Done():
			return nil, handshakeError()
		default:
		}
		if _, err = conn.WriteToUDP(hello, target); err != nil {
			return nil, err
		}
		until := time.Now().Add(250 * time.Millisecond)
		if until.After(deadline) {
			until = deadline
		}
		_ = conn.SetReadDeadline(until)
		n, remote, e := conn.ReadFromUDP(buf)
		if e != nil {
			var ne net.Error
			if errors.As(e, &ne) && ne.Timeout() {
				continue
			}
			if handshakeCtx.Err() != nil {
				return nil, handshakeError()
			}
			return nil, e
		}
		if !addrEqual(remote, target) {
			continue
		}
		if n > wire.MaxDatagram {
			continue
		}
		p := append([]byte(nil), buf[:n]...)
		h, e := wire.ParseHeader(p)
		if e != nil || h.Connection != cid || h.Kind != wire.KindHelloAck || h.Number != 0 {
			continue
		}
		if !checkHelloAck(secret, hello, p[wire.HeaderSize:]) {
			continue
		}
		ack = p
		break
	}
	if ack == nil {
		return nil, ErrTimeout
	}
	_, clientPart, _, ok := parseHelloBody(hello[wire.HeaderSize:])
	if !ok {
		return nil, errors.New("mira: invalid client hello")
	}
	tx, rx, schedule, err := derive(secret, key, ack[wire.HeaderSize:wire.HeaderSize+32], clientPart, ack[wire.HeaderSize:wire.HeaderSize+48], cid, true)
	if err != nil {
		return nil, err
	}
	finish, err := sealed(tx, cid, 1, wire.KindFinish, 0, []byte("MIRA-T/1 CLIENT FINISH"))
	if err != nil {
		return nil, err
	}
	var ready bool
	for time.Now().Before(deadline) {
		select {
		case <-handshakeCtx.Done():
			return nil, handshakeError()
		default:
		}
		if _, err = conn.WriteToUDP(finish, target); err != nil {
			return nil, err
		}
		until := time.Now().Add(250 * time.Millisecond)
		if until.After(deadline) {
			until = deadline
		}
		_ = conn.SetReadDeadline(until)
		n, remote, e := conn.ReadFromUDP(buf)
		if e != nil {
			var ne net.Error
			if errors.As(e, &ne) && ne.Timeout() {
				continue
			}
			if handshakeCtx.Err() != nil {
				return nil, handshakeError()
			}
			return nil, e
		}
		if !addrEqual(remote, target) || n > wire.MaxDatagram {
			continue
		}
		p := append([]byte(nil), buf[:n]...)
		h, plain, e := opened(rx, p, wire.KindData)
		if e != nil || h.Connection != cid || h.Number != 1 {
			continue
		}
		frames, e := wire.ParseFrames(plain, 4)
		if e != nil || len(frames) != 1 || frames[0].Type != wire.FramePing || string(frames[0].Body) != "READY" {
			continue
		}
		ready = true
		break
	}
	if !ready {
		return nil, ErrTimeout
	}
	stateMu.Lock()
	// Stop cancellation from closing the now handoff-owned socket.
	if handshakeCtx.Err() != nil {
		stateMu.Unlock()
		return nil, handshakeError()
	}
	transferred = true
	stateMu.Unlock()
	ss := newSession(conn, target, cid, tx, rx, schedule, true, o, nil)
	ss.identity = identity
	ss.start()
	conn = nil // session owns socket
	return ss, nil
}

// Client and server share this same encrypted session engine.
func newSession(conn *net.UDPConn, remote *net.UDPAddr, cid uint64, tx, rx *cryptokit.Protector, schedule trafficSchedule, client bool, opts Options, srv *Server) *Session {
	ss := &Session{conn: conn, remote: remote, id: cid, tx: tx, rx: rx, schedule: schedule, client: client, opts: opts, server: srv,
		incoming: make(chan inboundPacket, 256), requests: make(chan any, 128), done: make(chan struct{}),
		accepts: make(chan *Channel, opts.MaxChannels), channels: make(map[uint64]*Channel), closedIDs: make(map[uint64]struct{}), pending: make(map[uint64]*sentPacket),
		nextPN: 2, pathMTU: 1200, started: time.Now(), lastRecv: time.Now(), cwnd: 10 * wire.MaxDatagram, ssthresh: 1 << 30}
	if client {
		ss.nextChannel = 1
	} else {
		ss.nextChannel = 2
	}
	ss.txChain = schedule.initialChain(true)
	ss.rxChain = schedule.initialChain(false)
	clear(ss.schedule.master[:]) // Do not retain initial exporter/handshake master.
	return ss
}

func (s *Server) String() string { return fmt.Sprintf("MIRA-T/1 server on %s", s.Addr()) }
