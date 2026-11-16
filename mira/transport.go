package mira

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/internal/cryptokit"
	"github.com/oarkflow/fh/internal/wire"
	"github.com/oarkflow/fh/protocol"
)

const maxIPRateEntries = 8192

type ipRate struct {
	tokens float64
	last   time.Time
	slot   int
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
	secureOnly            atomic.Bool
	conn                  *net.UDPConn
	opts                  Options
	psk                   []byte
	resolver              PSKResolver
	globalHandshakeTokens float64
	globalHandshakeLast   time.Time
	done                  chan struct{}
	once                  sync.Once
	closeErr              error
	wg                    sync.WaitGroup
	mu                    sync.RWMutex
	rateMu                sync.Mutex
	handshakes            map[uint64]*handshake
	sessions              map[uint64]*Session
	identitySlots         map[string]int
	receiveBytes          atomic.Int64
	pendingHandshakes     int // Protected by mu; keeps global admission checks O(1).
	accepts               chan *Session
	ipRates               map[netip.Addr]*ipRate
	ipRateOrder           []netip.Addr // FIFO replacement ring, protected by rateMu.
	ipRateOrderCount      int
	ipRateOrderNext       int
	revoked               map[string]struct{}
	authorize             ChannelAuthorizer
	handshakeAttempts     atomic.Uint64
	handshakeRateLimited  atomic.Uint64
	tlsHandshakeAttempts  atomic.Uint64
	tlsHandshakeLimited   atomic.Uint64
	handshakesAccepted    atomic.Uint64
	datagramsRead         atomic.Uint64
	datagramsDropped      atomic.Uint64
	payloadBytesRead      atomic.Uint64
	datagramsSent         atomic.Uint64
	datagramSendErrors    atomic.Uint64
	payloadBytesSent      atomic.Uint64
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
	opts = opts.normalized()
	if err := configureUDPBuffers(conn, opts); err != nil {
		_ = conn.Close()
		return nil, err
	}
	s := &Server{conn: conn, opts: opts, psk: append([]byte(nil), secret...),
		done: make(chan struct{}), handshakes: make(map[uint64]*handshake), sessions: make(map[uint64]*Session), identitySlots: make(map[string]int), accepts: make(chan *Session, 64), ipRates: make(map[netip.Addr]*ipRate), ipRateOrder: make([]netip.Addr, maxIPRateEntries), revoked: make(map[string]struct{})}
	s.wg.Add(2)
	go s.readLoop()
	go s.sweepLoop()
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
	opts = opts.normalized()
	if err := configureUDPBuffers(conn, opts); err != nil {
		_ = conn.Close()
		return nil, err
	}
	s := &Server{conn: conn, opts: opts, resolver: resolve, done: make(chan struct{}),
		handshakes: make(map[uint64]*handshake), sessions: make(map[uint64]*Session), identitySlots: make(map[string]int), accepts: make(chan *Session, 64), ipRates: make(map[netip.Addr]*ipRate), ipRateOrder: make([]netip.Addr, maxIPRateEntries), revoked: make(map[string]struct{})}
	s.wg.Add(2)
	go s.readLoop()
	go s.sweepLoop()
	return s, nil
}

func configureUDPBuffers(conn *net.UDPConn, opts Options) error {
	if opts.UDPReadBufferBytes > 0 {
		if err := conn.SetReadBuffer(opts.UDPReadBufferBytes); err != nil {
			return fmt.Errorf("mira: set UDP read buffer: %w", err)
		}
	}
	if opts.UDPWriteBufferBytes > 0 {
		if err := conn.SetWriteBuffer(opts.UDPWriteBufferBytes); err != nil {
			return fmt.Errorf("mira: set UDP write buffer: %w", err)
		}
	}
	return nil
}

// waitUDPReadRetry prevents a persistent socket receive error from spinning a
// reader core. The caller owns delay; successful reads reset it to zero.
func waitUDPReadRetry(done <-chan struct{}, delay *time.Duration) bool {
	if *delay < time.Millisecond {
		*delay = time.Millisecond
	} else if *delay < 100*time.Millisecond {
		*delay *= 2
		if *delay > 100*time.Millisecond {
			*delay = 100 * time.Millisecond
		}
	}
	timer := time.NewTimer(*delay)
	defer timer.Stop()
	select {
	case <-done:
		return false
	case <-timer.C:
		return true
	}
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
			s.removeHandshakeLocked(id)
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

// ReplaceRevokedIdentities atomically replaces the in-memory revocation
// snapshot and disconnects sessions outside the current allowlist. Explicitly
// removed identities remain blocked for this reload; lingering sessions from
// older reloads stay blocked until they finish. The supplied maps are copied
// and bounded by the configured identity and session ceilings.
func (s *Server) ReplaceRevokedIdentities(identities, allowed map[string]struct{}) error {
	if len(identities) > maxConfiguredSessions || len(allowed) > maxConfiguredSessions {
		return errors.New("mira: revocation snapshot exceeds identity limit")
	}
	next := make(map[string]struct{}, len(identities))
	for identity := range identities {
		if identity == "" || len(identity) > 256 {
			return errors.New("mira: invalid identity in revocation snapshot")
		}
		next[identity] = struct{}{}
	}
	permittedIDs := make(map[string]struct{}, len(allowed))
	for identity := range allowed {
		if identity == "" || len(identity) > 256 {
			return errors.New("mira: invalid identity in allowlist snapshot")
		}
		permittedIDs[identity] = struct{}{}
	}
	var sessions []*Session
	s.mu.Lock()
	for id, hs := range s.handshakes {
		_, explicitlyRevoked := next[hs.s.identity]
		_, permitted := permittedIDs[hs.s.identity]
		if explicitlyRevoked || !permitted {
			if hs.s.identity != "" {
				next[hs.s.identity] = struct{}{}
			}
			s.removeHandshakeLocked(id)
		}
	}
	for _, sess := range s.sessions {
		_, explicitlyRevoked := next[sess.identity]
		_, permitted := permittedIDs[sess.identity]
		if explicitlyRevoked || !permitted {
			if sess.identity != "" {
				next[sess.identity] = struct{}{}
			}
			sessions = append(sessions, sess)
		}
	}
	s.revoked = next
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.disconnect()
	}
	return nil
}

// ServerStats contains only bounded metadata and does not expose secrets.
type ServerStats struct {
	ActiveSessions       int
	PendingHandshakes    int
	BlockedIdentities    int
	TotalSessionSlots    int
	ReceiveBufferedBytes int64
	ReceiveBufferLimit   int64
	HandshakeAttempts    uint64
	HandshakeRateLimited uint64
	TLSHandshakeAttempts uint64
	TLSHandshakeLimited  uint64
	HandshakesAccepted   uint64
	DatagramsRead        uint64
	DatagramsDropped     uint64
	PayloadBytesRead     uint64
	DatagramsSent        uint64
	DatagramSendErrors   uint64
	PayloadBytesSent     uint64
}

func (s *Server) Stats() ServerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ServerStats{
		ActiveSessions: len(s.sessions), PendingHandshakes: s.pendingHandshakes,
		BlockedIdentities: len(s.revoked), TotalSessionSlots: s.opts.MaxSessions,
		ReceiveBufferedBytes: s.receiveBytes.Load(), ReceiveBufferLimit: s.opts.MaxServerReceiveBytes,
		HandshakeAttempts: s.handshakeAttempts.Load(), HandshakeRateLimited: s.handshakeRateLimited.Load(), TLSHandshakeAttempts: s.tlsHandshakeAttempts.Load(), TLSHandshakeLimited: s.tlsHandshakeLimited.Load(), HandshakesAccepted: s.handshakesAccepted.Load(), DatagramsRead: s.datagramsRead.Load(), DatagramsDropped: s.datagramsDropped.Load(), PayloadBytesRead: s.payloadBytesRead.Load(),
		DatagramsSent: s.datagramsSent.Load(), DatagramSendErrors: s.datagramSendErrors.Load(), PayloadBytesSent: s.payloadBytesSent.Load(),
	}
}

func (s *Server) reserveReceive(n int64) bool {
	if n <= 0 {
		return false
	}
	for {
		x := s.receiveBytes.Load()
		if x < 0 || x > s.opts.MaxServerReceiveBytes || n > s.opts.MaxServerReceiveBytes-x {
			return false
		}
		if s.receiveBytes.CompareAndSwap(x, x+n) {
			return true
		}
	}
}

func (s *Server) writeUDP(raw []byte, to *net.UDPAddr) (int, error) {
	n, err := s.conn.WriteToUDP(raw, to)
	if err != nil {
		s.datagramSendErrors.Add(1)
	} else {
		s.datagramsSent.Add(1)
		s.payloadBytesSent.Add(uint64(n))
	}
	return n, err
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
		{"mira_receive_buffered_bytes", "gauge", uint64(m.ReceiveBufferedBytes)},
		{"mira_receive_buffer_limit_bytes", "gauge", uint64(m.ReceiveBufferLimit)},
		{"mira_pending_handshakes", "gauge", uint64(m.PendingHandshakes)},
		{"mira_handshake_attempts_total", "counter", m.HandshakeAttempts},
		{"mira_handshake_rate_limited_total", "counter", m.HandshakeRateLimited},
		{"mira_tls_handshake_attempts_total", "counter", m.TLSHandshakeAttempts},
		{"mira_tls_handshake_rate_limited_total", "counter", m.TLSHandshakeLimited},
		{"mira_handshakes_accepted_total", "counter", m.HandshakesAccepted},
		{"mira_datagrams_read_total", "counter", m.DatagramsRead},
		{"mira_datagrams_dropped_total", "counter", m.DatagramsDropped},
		{"mira_payload_bytes_read_total", "counter", m.PayloadBytesRead},
		{"mira_datagrams_sent_total", "counter", m.DatagramsSent},
		{"mira_datagram_send_errors_total", "counter", m.DatagramSendErrors},
		{"mira_payload_bytes_sent_total", "counter", m.PayloadBytesSent},
	} {
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", line.name, line.typ, line.name, line.v); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) Addr() net.Addr { return s.conn.LocalAddr() }
func (s *Server) Accept(ctx context.Context) (*Session, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.done:
			return nil, ErrClosed
		case sess := <-s.accepts:
			if sess == nil {
				continue
			}
			select {
			case <-s.done:
				return nil, ErrClosed
			default:
			}
			select {
			case <-sess.done:
				continue
			default:
				return sess, nil
			}
		}
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
		graceTimer := time.NewTimer(2 * time.Second)
		for i, sess := range snapshot {
			select {
			case <-sess.finished:
				continue
			default:
			}
			select {
			case <-sess.finished:
			case <-graceTimer.C:
				for _, pending := range snapshot[i:] {
					pending.stop()
				}
				goto gracefulDrainDone
			}
		}
	gracefulDrainDone:
		graceTimer.Stop()
		// The first deadline is a graceful drain. All remaining sessions have
		// been stopped above; give their owner goroutines one bounded cleanup
		// interval before clearing shared server state.
		cleanupTimer := time.NewTimer(2 * time.Second)
		defer cleanupTimer.Stop()
		for _, sess := range snapshot {
			select {
			case <-sess.finished:
			case <-cleanupTimer.C:
				for _, pending := range snapshot {
					select {
					case <-pending.finished:
					default:
						s.closeErr = ErrTimeout
						goto sessionsStopped
					}
				}
				goto sessionsStopped
			}
		}
	sessionsStopped:
		_ = s.conn.Close()
	})
	s.wg.Wait()
	s.mu.Lock()
	for id, hs := range s.handshakes {
		clear(hs.hello)
		clear(hs.helloAck)
		clear(hs.ready)
		s.removeHandshakeLocked(id)
	}
	clear(s.handshakes)
	clear(s.psk)
	s.psk = nil
	s.mu.Unlock()
	s.rateMu.Lock()
	clear(s.ipRates)
	s.rateMu.Unlock()
	return s.closeErr
}
func (s *Server) removeSession(id uint64) {
	s.mu.Lock()
	sess := s.sessions[id]
	hs := s.handshakes[id]
	delete(s.sessions, id)
	delete(s.handshakes, id)
	if sess != nil {
		s.removeIdentitySlotLocked(sess.identity)
	} else if hs != nil && !hs.established {
		s.pendingHandshakes--
		s.removeIdentitySlotLocked(hs.s.identity)
	}
	s.mu.Unlock()
}

func (s *Server) removeIdentitySlotLocked(identity string) {
	if count := s.identitySlots[identity]; count <= 1 {
		delete(s.identitySlots, identity)
	} else {
		s.identitySlots[identity] = count - 1
	}
}

func (s *Server) removeHandshakeLocked(id uint64) {
	hs := s.handshakes[id]
	if hs == nil {
		return
	}
	delete(s.handshakes, id)
	if !hs.established {
		s.pendingHandshakes--
		s.removeIdentitySlotLocked(hs.s.identity)
	}
}

// deliverSession linearizes application delivery against identity revocation.
// The session must already be registered in the server's active-session map.
func (s *Server) deliverSession(sess *Session) bool {
	s.mu.RLock()
	_, denied := s.revoked[sess.identity]
	deliver := !denied && s.sessions[sess.id] == sess
	select {
	case <-sess.done:
		deliver = false
	default:
	}
	if deliver {
		select {
		case s.accepts <- sess:
		default:
			deliver = false
		}
	}
	s.mu.RUnlock()
	return deliver
}

// deliverChannel rechecks session ownership and revocation after the channel
// policy callback has run, then linearizes application delivery with revokes.
func (s *Server) deliverChannel(sess *Session, ch *Channel) bool {
	s.mu.RLock()
	_, denied := s.revoked[sess.identity]
	deliver := !denied && s.sessions[sess.id] == sess
	select {
	case <-sess.done:
		deliver = false
	default:
	}
	if deliver {
		select {
		case sess.accepts <- ch:
		default:
			deliver = false
		}
	}
	s.mu.RUnlock()
	return deliver
}

func addrEqual(a, b *net.UDPAddr) bool {
	return a != nil && b != nil && a.Port == b.Port && a.Zone == b.Zone && a.IP.Equal(b.IP)
}

func (s *Server) readLoop() {
	defer s.wg.Done()
	var readErrorDelay time.Duration
	for {
		select {
		case <-s.done:
			return
		default:
		}
		packet := acquireInboundBuffer()
		n, addr, err := s.conn.ReadFromUDP(packet)
		if err != nil {
			releaseInboundDatagram(packet)
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-s.done:
				return
			default:
			}
			if !waitUDPReadRetry(s.done, &readErrorDelay) {
				return
			}
			continue
		}
		readErrorDelay = 0
		select {
		case <-s.done:
			releaseInboundDatagram(packet)
			return
		default:
		}
		if n < wire.HeaderSize || n > wire.MaxDatagram {
			s.datagramsDropped.Add(1)
			releaseInboundDatagram(packet)
			continue
		}
		s.datagramsRead.Add(1)
		s.payloadBytesRead.Add(uint64(n))
		packet = packet[:n]
		h, parseErr := wire.ParseHeader(packet)
		if parseErr != nil {
			s.datagramsDropped.Add(1)
		}
		transferred := false
		if parseErr == nil {
			switch h.Kind {
			case wire.KindHello:
				s.onHello(h, packet, addr)
			case wire.KindFinish:
				s.onFinish(h, packet, addr)
			case wire.KindData:
				s.mu.RLock()
				ss := s.sessions[h.Connection]
				if ss != nil {
					select {
					case <-ss.done:
					case ss.incoming <- inboundPacket{raw: packet, from: addr, pooled: true}:
						transferred = true
					default:
					}
				}
				// Keep removal from overtaking a packet enqueue; the session
				// actor drains the queue after it leaves the session map.
				s.mu.RUnlock()
				if !transferred {
					s.datagramsDropped.Add(1)
				}
			}
		}
		if !transferred {
			releaseInboundDatagram(packet)
		}
	}
}

// sweepLoop expires admission state independently of packet traffic. Keeping
// deadlines out of readLoop avoids a deadline syscall for every datagram.
func (s *Server) sweepLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.expireHandshakes()
		}
	}
}

func (s *Server) expireHandshakes() {
	s.rateMu.Lock()
	for ip, rate := range s.ipRates {
		if time.Since(rate.last) > time.Minute {
			delete(s.ipRates, ip)
		}
	}
	s.rateMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for cid, hs := range s.handshakes {
		if !hs.established && time.Since(hs.received) > s.opts.HandshakeTimeout {
			s.removeHandshakeLocked(cid)
		}
		if hs.established && time.Since(hs.received) > 10*time.Second {
			delete(s.handshakes, cid)
		}
	}
}

// admitIP enforces endpoint-wide and per-source token buckets before handshake
// cryptographic work. rateMu protects both buckets across UDP and TLS listeners.
func (s *Server) admitIP(ip net.IP) bool {
	key, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	key = key.Unmap()
	now := time.Now()
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	rateLimit := float64(s.opts.MaxHandshakeAttemptsPerSecond)
	burst := 2 * rateLimit
	if s.globalHandshakeLast.IsZero() {
		s.globalHandshakeTokens = burst
	} else {
		s.globalHandshakeTokens += now.Sub(s.globalHandshakeLast).Seconds() * rateLimit
		if s.globalHandshakeTokens > burst {
			s.globalHandshakeTokens = burst
		}
	}
	s.globalHandshakeLast = now
	if s.globalHandshakeTokens < 1 {
		return false
	}
	rate := s.ipRates[key]
	if rate == nil {
		slot := s.ipRateOrderCount
		if s.ipRateOrderCount < maxIPRateEntries {
			s.ipRateOrderCount++
		} else {
			slot = s.ipRateOrderNext
			oldKey := s.ipRateOrder[slot]
			if old := s.ipRates[oldKey]; old != nil && old.slot == slot {
				delete(s.ipRates, oldKey)
			}
			s.ipRateOrderNext = (slot + 1) % maxIPRateEntries
		}
		s.ipRateOrder[slot] = key
		s.ipRates[key] = &ipRate{tokens: 99, last: now, slot: slot}
		s.globalHandshakeTokens--
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
	s.globalHandshakeTokens--
	return true
}
func (s *Server) onHello(h wire.Header, p []byte, addr *net.UDPAddr) {
	if s.secureOnly.Load() {
		return
	}
	s.handshakeAttempts.Add(1)
	if !s.admitIP(addr.IP) {
		s.handshakeRateLimited.Add(1)
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
	n := len(s.sessions) + s.pendingHandshakes
	s.mu.RUnlock()
	if hs != nil {
		if sameHello(hs.hello, p) && addrEqual(hs.s.getRemote(), addr) {
			_, _ = s.writeUDP(hs.helloAck, addr)
		}
		return
	}
	if active != nil || n >= s.opts.MaxSessions {
		return
	}
	if s.opts.MaxSessionsPerIdentity > 0 && identity != "" {
		s.mu.RLock()
		count := s.identitySlots[identity]
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
	defer clear(schedule.master[:])
	ss := newSession(s.conn, addr, h.Connection, tx, rx, &schedule, false, s.opts, s)
	ss.identity = identity
	hs = &handshake{received: time.Now(), hello: append([]byte(nil), p...), helloAck: ack, s: ss}
	s.mu.Lock()
	activeCount := len(s.sessions) + s.pendingHandshakes
	_, denied = s.revoked[identity]
	select {
	case <-s.done:
		s.mu.Unlock()
		return
	default:
	}
	if denied || s.handshakes[h.Connection] != nil || s.sessions[h.Connection] != nil || activeCount >= s.opts.MaxSessions ||
		(s.opts.MaxSessionsPerIdentity > 0 && identity != "" && s.identitySlots[identity] >= s.opts.MaxSessionsPerIdentity) {
		s.mu.Unlock()
		return
	}
	s.handshakes[h.Connection] = hs
	s.pendingHandshakes++
	s.identitySlots[identity]++
	s.mu.Unlock()
	_, _ = s.writeUDP(ack, addr)
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
		s.mu.RLock()
		current := s.handshakes[h.Connection] == hs && s.sessions[h.Connection] == hs.s
		_, denied := s.revoked[hs.s.identity]
		s.mu.RUnlock()
		if current && !denied {
			_, _ = s.writeUDP(previousReady, addr)
		}
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
	_, denied = s.revoked[hs.s.identity]
	if denied || s.handshakes[h.Connection] != hs || s.sessions[h.Connection] != nil || hs.established {
		s.mu.Unlock()
		return
	}
	hs.ready = ready
	hs.established = true
	s.pendingHandshakes--
	s.sessions[h.Connection] = hs.s
	// Publish only after the session owner and finished channel are initialized;
	// concurrent Server.Close snapshots sessions under this same lock.
	hs.s.start()
	s.mu.Unlock()
	if !s.deliverSession(hs.s) {
		hs.s.stop()
		return
	}
	s.handshakesAccepted.Add(1)
	_, _ = s.writeUDP(ready, addr)
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
	if err := configureUDPBuffers(conn, o); err != nil {
		_ = conn.Close()
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
	defer clear(schedule.master[:])
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
	ss := newSession(conn, target, cid, tx, rx, &schedule, true, o, nil)
	ss.identity = identity
	ss.start()
	conn = nil // session owns socket
	return ss, nil
}

// Client and server share this same encrypted session engine.
func newSession(conn *net.UDPConn, remote *net.UDPAddr, cid uint64, tx, rx *cryptokit.Protector, schedule *trafficSchedule, client bool, opts Options, srv *Server) *Session {
	// The admission queue is deliberately smaller than the active-channel cap:
	// a slow application must not turn a large configured channel limit into a
	// correspondingly large pointer array for every session.
	acceptQueue := min(opts.MaxChannels, 64)
	ss := &Session{conn: conn, remote: remote, id: cid, tx: tx, rx: rx, schedule: *schedule, client: client, opts: opts, server: srv,
		incoming: make(chan inboundPacket, 256), requests: make(chan any, 128), done: make(chan struct{}),
		accepts: make(chan *Channel, acceptQueue), channels: make(map[uint64]*Channel), closedIDs: make(map[uint64]struct{}), pending: make(map[uint64]*sentPacket),
		nextPN: 2, pathMTU: 1200, started: time.Now(), lastRecv: time.Now(), cwnd: 10 * wire.MaxDatagram, ssthresh: 1 << 30}
	if client {
		ss.nextChannel = 1
	} else {
		ss.nextChannel = 2
	}
	ss.txChain = schedule.initialChain(true)
	ss.rxChain = schedule.initialChain(false)
	clear(schedule.master[:])
	clear(ss.schedule.master[:]) // Do not retain initial exporter/handshake master.
	return ss
}

func (s *Server) String() string { return fmt.Sprintf("MIRA-T/1 server on %s", s.Addr()) }
