package mira

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/internal/cryptokit"
	"github.com/oarkflow/fh/internal/replay"
	"github.com/oarkflow/fh/internal/wire"
	"github.com/oarkflow/fh/protocol"
)

type openRequest struct {
	ctx    context.Context
	cfg    protocol.ChannelConfig
	result chan openResult
}
type openResult struct {
	ch  *Channel
	err error
}
type sendRequest struct {
	c        *Channel
	id       uint64
	data     []byte
	reserved int64
}
type closeRequest struct{ id uint64 }
type receiptRequest struct{ frame wire.Frame }
type terminateRequest struct{}
type rebindProbeRequest struct{ reply chan error }
type halfCloseRequest struct {
	id    uint64
	final uint64
}
type mtuProbeRequest struct {
	target int
	result chan int
}
type statsRequest struct{ reply chan SessionStats }
type inboundPacket struct {
	raw  []byte
	from *net.UDPAddr
}
type candidatePath struct {
	addr           *net.UDPAddr
	token          [8]byte
	first          time.Time
	lastChallenge  time.Time
	received, sent int
}

// SessionStats is a consistent snapshot from the single-owner session worker.
// It contains aggregate transport state only, never secrets or payloads.
type SessionStats struct {
	ConnectionID           uint64
	RemoteIdentity         string
	ActiveChannels         int
	PendingPackets         int
	QueuedBytes            int64
	ReceiveBufferedBytes   int64
	CongestionWindowBytes  int
	InFlightBytes          int
	RTT                    time.Duration
	SentPacketNumber       uint64
	ReceivedPacketNumber   uint64
	PathMTU                int
	MTUBlackholeRecoveries uint64
	TXKeyEpoch             uint16
	RXKeyEpoch             uint16
	Retransmissions        uint64
	FastLossSignals        uint64
	PersistentCongestion   uint64
	AckRangesSent          uint64
	ValidatedMigrations    uint64
	RateLimitedBytes       uint64
}

type txFrame struct {
	probeMTU      int
	rebindReply   chan error
	reclaimCredit bool // retransmission after refragmentation of a credited message
	probeReply    chan int
	frame         wire.Frame
	reliable      bool
	c             *Channel
	msgID         uint64
}
type sentPacket struct {
	raw   []byte
	sent  time.Time
	first time.Time
	tries int
	frame txFrame
	size  int
}

// Session is a full-duplex session on a native UDP socket.
type Session struct {
	conn                        *net.UDPConn
	connMu                      sync.RWMutex
	remote                      *net.UDPAddr
	remoteMu                    sync.RWMutex
	path                        *candidatePath
	migrations                  uint64
	id                          uint64
	identity                    string
	client                      bool
	bootstrapTLS                bool          // Require UDP return-path validation before transmitting data.
	tlsReady                    chan struct{} // Client receives a valid authenticated UDP response.
	tlsReadyOnce                sync.Once
	validated                   bool // Only accessed from the session actor.
	opts                        Options
	tx, rx                      *cryptokit.Protector
	rxPrevious                  *cryptokit.Protector
	txChain, rxChain            [32]byte
	schedule                    trafficSchedule
	txEpoch, rxEpoch            uint16
	pathMTU                     int
	mtuProbeOutstanding         bool
	mtuProbeTarget              int
	mtuBlackholeRecoveries      uint64
	mtuProbeFailures            int
	lastSmallAck                time.Time
	nextMTUProbe                time.Time
	maxRecvPN                   uint64
	server                      *Server
	incoming                    chan inboundPacket
	requests                    chan any
	wake                        chan struct{}
	accepts                     chan *Channel
	done                        chan struct{}
	finished                    chan struct{}
	once                        sync.Once
	recvBytes                   atomic.Int64
	consumedBytes               atomic.Int64
	queuedBytes                 atomic.Int64
	channels                    map[uint64]*Channel
	closedIDs                   map[uint64]struct{}
	closedOrder                 []uint64
	nextChannel                 uint64
	nextPN                      uint64
	pending                     map[uint64]*sentPacket
	queue                       []txFrame
	control                     []txFrame
	replay                      replay.Window
	started, lastRecv, lastSent time.Time
	cwnd, ssthresh              int
	inFlightBytes               int
	peerLimit                   uint64
	sentCredit                  uint64
	lastCredit                  uint64
	srtt, rttvar                time.Duration
	retransmissions             uint64
	fastLossSignals             uint64
	persistentCongestion        uint64
	persistentRecovery          bool
	lastAckTime                 time.Time
	ackRangesSent               uint64
	acksPending                 map[uint64]struct{}
	acksHistory                 []uint64
	nextPaced                   time.Time
	lastLoss                    time.Time
	lastServedChannel           uint64
	schedulerPackets            uint64
	inboundTokens               float64
	inboundLast                 time.Time
	rateLimitedBytes            uint64
}

// Stats returns a thread-safe view of the current session state.
func (s *Session) Stats(ctx context.Context) (SessionStats, error) {
	reply := make(chan SessionStats, 1)
	select {
	case s.requests <- statsRequest{reply: reply}:
	case <-s.done:
		return SessionStats{}, ErrClosed
	case <-ctx.Done():
		return SessionStats{}, ctx.Err()
	}
	select {
	case st := <-reply:
		return st, nil
	case <-s.done:
		return SessionStats{}, ErrClosed
	case <-ctx.Done():
		return SessionStats{}, ctx.Err()
	}
}

func (s *Session) snapshot() SessionStats {
	return SessionStats{
		ConnectionID: s.id, RemoteIdentity: s.identity, ActiveChannels: len(s.channels),
		PendingPackets: len(s.pending), QueuedBytes: s.queuedBytes.Load(),
		ReceiveBufferedBytes: s.recvBytes.Load(), CongestionWindowBytes: s.cwnd,
		InFlightBytes: s.inFlightBytes, RTT: s.srtt, SentPacketNumber: s.nextPN - 1,
		ReceivedPacketNumber: s.maxRecvPN, PathMTU: s.pathMTU, MTUBlackholeRecoveries: s.mtuBlackholeRecoveries, TXKeyEpoch: s.txEpoch, RXKeyEpoch: s.rxEpoch,
		Retransmissions: s.retransmissions, FastLossSignals: s.fastLossSignals, PersistentCongestion: s.persistentCongestion, AckRangesSent: s.ackRangesSent, ValidatedMigrations: s.migrations, RateLimitedBytes: s.rateLimitedBytes,
	}
}

func (s *Session) ID() uint64           { return s.id }
func (s *Session) Identity() string     { return s.identity }
func (s *Session) RemoteAddr() net.Addr { return s.getRemote() }
func (s *Session) getRemote() *net.UDPAddr {
	s.remoteMu.RLock()
	defer s.remoteMu.RUnlock()
	return s.remote
}
func (s *Session) setRemote(addr *net.UDPAddr) {
	s.remoteMu.Lock()
	s.remote = addr
	s.remoteMu.Unlock()
}
func (s *Session) currentConn() *net.UDPConn {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return s.conn
}
func (s *Session) writeUDP(raw []byte, to *net.UDPAddr) (int, error) {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	if s.conn == nil {
		return 0, ErrClosed
	}
	return s.conn.WriteToUDP(raw, to)
}

// RebindClient replaces the client's UDP socket (for example after changing
// Wi-Fi/cellular networks). The server must validate the new source address
// before forwarding application traffic. Pending reliable ciphertext remains
// available for retransmission; application replay is still required after a
// full process crash.
// ProbeMTU tests whether an authenticated datagram of target bytes survives
// the current path. Timeouts and oversized datagram errors return 0, never
// raise the fragmentation limit. The maximum is 1472 bytes on v4.
func (s *Session) ProbeMTU(ctx context.Context, target int) (int, error) {
	if target < 1200 || target > wire.MaxDatagram {
		return 0, errors.New("mira: unsupported MTU target")
	}
	reply := make(chan int, 1)
	select {
	case s.requests <- mtuProbeRequest{target, reply}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		return 0, ErrClosed
	}
	select {
	case n := <-reply:
		return n, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		return 0, ErrClosed
	}
}
func (s *Session) RebindClient(ctx context.Context) error {
	if !s.client {
		return errors.New("mira: only a client can rebind its UDP socket")
	}
	select {
	case <-s.done:
		return ErrClosed
	default:
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return err
	}
	s.connMu.Lock()
	select {
	case <-s.done:
		s.connMu.Unlock()
		_ = conn.Close()
		return ErrClosed
	default:
	}
	previous := s.conn
	s.conn = conn
	s.connMu.Unlock()
	if previous != nil {
		_ = previous.Close()
	}
	go s.readClient()
	select {
	case s.requests <- rebindProbeRequest{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	}
}

// RebindAndWait changes the UDP client socket, then waits for an ACK
// of a fresh authenticated PATH probe. Unlike RebindClient, completion
// confirms the new return path is working rather than merely queued.
func (s *Session) RebindAndWait(ctx context.Context) error {
	if err := s.RebindClient(ctx); err != nil {
		return err
	}
	ack := make(chan error, 1)
	select {
	case s.requests <- rebindProbeRequest{reply: ack}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	}
}

func (s *Session) stop() {
	s.once.Do(func() {
		close(s.done)
		if s.client {
			if c := s.currentConn(); c != nil {
				_ = c.Close()
			}
		}
	})
}
func (s *Session) Close(ctx context.Context) error {
	s.disconnect()
	grace := time.NewTimer(500 * time.Millisecond)
	defer grace.Stop()
	select {
	case <-s.finished:
		return nil
	case <-ctx.Done():
		s.stop()
		return ctx.Err()
	case <-grace.C:
		s.stop() // Force termination if a graceful GOAWAY could not drain.
	}
	limit := time.NewTimer(2 * time.Second)
	defer limit.Stop()
	select {
	case <-s.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-limit.C:
		return ErrTimeout
	}
}

// disconnect asks the owner goroutine to transmit an authenticated terminal
// frame before closing. Revocation is enforced server-side immediately even if
// all terminal notifications are lost in flight.
func (s *Session) disconnect() {
	select {
	case <-s.done:
	case s.requests <- terminateRequest{}:
	default:
		s.stop()
	}
}

// sendTerminal runs only on the session goroutine. Retransmitted terminal
// frames are byte-identical to prevent nonce reuse. Shutdown notification is
// best effort; an unreachable endpoint eventually expires its idle timeout.
func (s *Session) sendTerminal() {
	if s.nextPN >= (uint64(1)<<16)*keyEpochPackets {
		return
	}
	frame, err := wire.AppendFrame(nil, wire.Frame{Type: wire.FrameGoAway})
	if err != nil {
		return
	}
	pn := s.nextPN
	s.nextPN++
	p, epoch, err := s.encryptor(pn)
	if err != nil {
		return
	}
	raw, err := sealedEpoch(p, s.id, pn, wire.KindData, 0, epoch, frame)
	if err != nil {
		return
	}
	for i := 0; i < 3; i++ {
		_, _ = s.writeUDP(raw, s.getRemote())
	}
}

func (s *Session) start() {
	s.wake = make(chan struct{}, 1)
	s.finished = make(chan struct{})
	// Initial connection-level receive credit; retransmitted until ACKed.
	s.control = append(s.control, txFrame{frame: wire.Frame{Type: wire.FrameCredit, Body: encodeCredit(uint64(s.opts.MaxReceiveBytes))}, reliable: true})
	go s.run()
	if s.client {
		go s.readClient()
	}
}
func (s *Session) reserveQueue(n int64) bool {
	for {
		x := s.queuedBytes.Load()
		if x+n > s.opts.MaxQueuedBytes {
			return false
		}
		if s.queuedBytes.CompareAndSwap(x, x+n) {
			return true
		}
	}
}
func (s *Session) releaseQueue(n int64) { s.queuedBytes.Add(-n) }
func (s *Session) reserveReceive(n int64) bool {
	for {
		x := s.recvBytes.Load()
		if x+n > s.opts.MaxReceiveBytes {
			return false
		}
		if s.recvBytes.CompareAndSwap(x, x+n) {
			return true
		}
	}
}
func (s *Session) OpenChannel(ctx context.Context, cfg protocol.ChannelConfig) (protocol.Channel, error) {
	if cfg.Delivery < protocol.ReliableOrdered || cfg.Delivery > protocol.Bulk {
		return nil, errors.New("mira: invalid delivery mode")
	}
	if cfg.MaxMessage <= 0 {
		cfg.MaxMessage = int64(s.opts.MaxMessage)
	}
	if cfg.MaxMessage > int64(s.opts.MaxMessage) {
		return nil, errors.New("mira: max message exceeds session limit")
	}
	if cfg.Lease < 0 || cfg.Lease > 24*time.Hour {
		return nil, errors.New("mira: invalid lease")
	}
	result := make(chan openResult, 1)
	select {
	case s.requests <- openRequest{ctx: ctx, cfg: cfg, result: result}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrClosed
	}
	select {
	case r := <-result:
		return r.ch, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrClosed
	}
}
func (s *Session) AcceptChannel(ctx context.Context) (protocol.Channel, error) {
	select {
	case c := <-s.accepts:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
		return nil, ErrClosed
	}
}
func (s *Session) readClient() {
	c := s.currentConn()
	buf := make([]byte, wire.MaxDatagram+1)
	for {
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		n, remote, err := c.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-s.done:
				return
			default:
			}
			continue
		}
		if !addrEqual(remote, s.getRemote()) || n > wire.MaxDatagram {
			continue
		}
		p := append([]byte(nil), buf[:n]...)
		select {
		case s.incoming <- inboundPacket{raw: p, from: remote}:
		case <-s.done:
			return
		default:
		}
	}
}
func (s *Session) run() {
	defer func() {
		s.stop()
		for _, c := range s.channels {
			c.signalClose()
		}
		if s.server != nil {
			s.server.removeSession(s.id)
		}
		close(s.finished)
	}()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	// The pacer spaces packets by sub-millisecond intervals. Without its own
	// wake-up, a send blocked only by pacing would wait for the next inbound
	// packet or the 5 ms ticker, capping throughput near one packet per tick.
	pace := time.NewTimer(time.Hour)
	pace.Stop()
	defer pace.Stop()
	// Delayed-ACK timer. ACKs used to leave only on the 5 ms ticker, which put
	// up to 5 ms into every transport receipt and every loss-recovery round
	// trip. A short timer still coalesces bursts into one ACK_RANGES frame.
	ackTimer := time.NewTimer(time.Hour)
	ackTimer.Stop()
	defer ackTimer.Stop()
	ackArmed := false
	for {
		select {
		case <-s.done:
			return
		case <-pace.C:
		case <-ackTimer.C:
			ackArmed = false
			s.flushAcks()
		case p := <-s.incoming:
			s.processPacket(p)
		case req := <-s.requests:
			s.handleRequest(req)
		case <-s.wake:
			s.drainCompleted()
			s.publishCredit()
		case <-ticker.C:
			now := time.Now()
			if now.Sub(s.lastRecv) > s.opts.IdleTimeout || now.Sub(s.started) > s.opts.MaxSessionLifetime {
				return
			}
			s.retry(now)
			s.autoProbe(now)
			s.flushAcks()
			if now.Sub(s.lastSent) > 15*time.Second {
				s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FramePing}})
			}
			s.expireAssemblies(now)
			s.publishCredit()
		}
		s.flush()
		if len(s.queue) > 0 || len(s.control) > 0 {
			if d := time.Until(s.nextPaced); d > 0 {
				pace.Reset(d)
			}
		}
		if len(s.acksPending) > 0 && !ackArmed {
			ackArmed = true
			ackTimer.Reset(ackDelay)
		}
	}
}
func (s *Session) handleRequest(req any) {
	switch r := req.(type) {
	case openRequest:
		if len(s.channels) >= s.opts.MaxChannels {
			r.result <- openResult{err: ErrBackpressure}
			return
		}
		id := s.nextChannel
		s.nextChannel += 2
		c := newChannel(s, id, r.cfg, true)
		s.channels[id] = c
		// OpenChannel returns only after OPEN_ACK, avoiding data before authorization.
		ready := c.ready
		go func() {
			select {
			case err := <-ready:
				r.result <- openResult{ch: c, err: err}
			case <-r.ctx.Done():
				select {
				case s.requests <- closeRequest{id: id}:
				case <-s.done:
				}
				r.result <- openResult{err: r.ctx.Err()}
			case <-s.done:
				r.result <- openResult{err: ErrClosed}
			}
		}()
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameOpen, Body: encodeOpen(id, c.cfg)}, reliable: true})
	case sendRequest:
		if r.c.closed.Load() || r.c.expired() || s.channels[r.c.id] != r.c {
			r.c.signalClose()
			if r.c.cfg.Delivery == protocol.BestEffort || r.c.cfg.Delivery == protocol.LatestState {
				s.releaseQueue(r.reserved)
			}
			return
		}
		reliable := r.c.cfg.Delivery != protocol.BestEffort && r.c.cfg.Delivery != protocol.LatestState
		chunkSize := fragmentSize
		if s.pathMTU > 1200 {
			chunkSize = min(1300, s.pathMTU-96)
		}
		count := (len(r.data) + chunkSize - 1) / chunkSize
		if count == 0 {
			count = 1
		}
		if reliable {
			r.c.mu.Lock()
			if state := r.c.receipts[r.id]; state != nil {
				state.payload = r.data
			}
			r.c.mu.Unlock()
			r.c.setLeft(r.id, count)
		}
		if r.c.cfg.Delivery == protocol.LatestState {
			// Unsent, superseded states are dropped; in-flight states remain best-effort.
			out := s.queue[:0]
			for _, f := range s.queue {
				if f.c != r.c {
					out = append(out, f)
				}
			}
			s.queue = out
		}
		for i := 0; i < count; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > len(r.data) {
				end = len(r.data)
			}
			f := wire.Frame{Type: wire.FrameData, Body: encodeDataSized(r.c.id, r.id, uint64(len(r.data)), uint64(start), uint64(chunkSize), r.data[start:end])}
			s.queue = append(s.queue, txFrame{frame: f, reliable: reliable, c: r.c, msgID: r.id})
		}
		if !reliable {
			s.releaseQueue(r.reserved)
		}
	case statsRequest:
		r.reply <- s.snapshot()
	case halfCloseRequest:
		body := binary.AppendUvarint(nil, r.id)
		body = binary.AppendUvarint(body, r.final)
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameHalfClose, Body: body}, reliable: true})
	case mtuProbeRequest:
		s.queueMTUProbe(r.target, r.result)
	case rebindProbeRequest:
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FramePing, Body: []byte("REBIND")}, reliable: true, rebindReply: r.reply})
	case terminateRequest:
		s.sendTerminal()
		s.stop()
	case receiptRequest:
		s.enqueueControl(txFrame{frame: r.frame, reliable: true})
	case closeRequest:
		if c := s.channels[r.id]; c != nil {
			s.closeChannel(c)
			s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameClose, Body: binary.AppendUvarint(nil, r.id)}, reliable: true})
		}
	}
}

// closeChannel runs only on the session owner goroutine. Keep a bounded
// tombstone window so delayed retransmitted OPEN packets cannot resurrect a
// recently closed channel, while allowing unlimited sequential channel use.
func (s *Session) closeChannel(c *Channel) {
	if s.channels[c.id] != c {
		return
	}
	c.signalClose()
	delete(s.channels, c.id)
	s.closedIDs[c.id] = struct{}{}
	s.closedOrder = append(s.closedOrder, c.id)
	if len(s.closedOrder) > 4096 {
		delete(s.closedIDs, s.closedOrder[0])
		s.closedOrder[0] = 0
		s.closedOrder = s.closedOrder[1:]
	}
	for _, a := range c.assemblies {
		s.recvBytes.Add(-int64(len(a.payload)))
		s.consumedBytes.Add(int64(len(a.payload)))
	}
	c.assemblies = nil
	for _, m := range c.completed {
		s.recvBytes.Add(-int64(len(m.Payload)))
		s.consumedBytes.Add(int64(len(m.Payload)))
	}
	c.completed = nil
	// Only previously received messages in c.recv remain available for drain.
	filtered := s.queue[:0]
	for _, f := range s.queue {
		if f.c != c {
			filtered = append(filtered, f)
		}
	}
	s.queue = filtered
	for pn, p := range s.pending {
		if p.frame.c == c {
			delete(s.pending, pn)
			s.inFlightBytes -= p.size
		}
	}
}

func (s *Session) enqueueControl(f txFrame) {
	if len(s.control) < 256 {
		s.control = append(s.control, f)
	} else {
		// Never silently drop security or reliability control messages.
		s.stop()
	}
}
func (s *Session) flush() {
	if s.bootstrapTLS && !s.validated {
		return
	}
	for len(s.queue) > 0 || len(s.control) > 0 {
		if time.Now().Before(s.nextPaced) {
			return
		}
		control := len(s.control) > 0
		var f txFrame
		queueIndex := 0
		if control {
			f = s.control[0]
		} else {
			queueIndex = s.chooseQueueFrame()
			if queueIndex < 0 {
				return
			}
			f = s.queue[queueIndex]
		}
		// All packet types are governed by the in-flight congestion budget.
		if len(s.pending) >= s.opts.MaxInflight || s.inFlightBytes+wire.MaxDatagram > s.cwnd {
			return
		}
		if f.frame.Type == wire.FrameData && f.reliable {
			_, _, total, offset, _, ok := decodeData(f.frame.Body)
			if !ok {
				return
			}
			if offset == 0 && !f.reclaimCredit && (s.sentCredit > s.peerLimit || total > s.peerLimit-s.sentCredit) {
				// Allow fragments belonging to previously-started messages
				// to progress even when a new message cannot claim credit.
				if !control && s.skipBlockedCredit() {
					continue
				}
				return
			}
			if offset == 0 && !f.reclaimCredit {
				s.sentCredit += total
			}
		}
		if control {
			s.control = s.control[1:]
		} else {
			s.lastServedChannel = f.c.id
			s.schedulerPackets++
			copy(s.queue[queueIndex:], s.queue[queueIndex+1:])
			s.queue[len(s.queue)-1] = txFrame{}
			s.queue = s.queue[:len(s.queue)-1]
		}
		var body []byte
		body, err := wire.AppendFrame(nil, f.frame)
		if err != nil {
			continue
		}
		if s.nextPN >= (uint64(1)<<16)*keyEpochPackets {
			s.stop()
			return
		}
		pn := s.nextPN
		s.nextPN++
		flag := uint8(0)
		if f.reliable {
			flag = ackRequired
		}
		protector, epoch, err := s.encryptor(pn)
		if err != nil {
			s.stop()
			return
		}
		raw, err := sealedEpoch(protector, s.id, pn, wire.KindData, flag, epoch, body)
		if err != nil {
			continue
		}
		if _, err = s.writeUDP(raw, s.getRemote()); err != nil {
			if f.probeMTU > 0 {
				s.onProbeFailure()
				if f.probeReply != nil {
					select {
					case f.probeReply <- 0:
					default:
					}
				}
				continue
			}
			s.stop()
			return
		}
		s.lastSent = time.Now()
		s.advancePacer(s.lastSent, len(raw))
		if f.reliable {
			s.pending[pn] = &sentPacket{raw: raw, sent: s.lastSent, first: s.lastSent, tries: 0, frame: f, size: len(raw)}
			s.inFlightBytes += len(raw)
		}
	}
}

// chooseQueueFrame services first queued frame of each channel, preserving
// per-channel enqueue order while alternating independent channels. Larger
// priority values receive preference, but every eighth packet intentionally
// favors lower priorities so a busy channel cannot starve all others.
func (s *Session) chooseQueueFrame() int {
	seen := make(map[uint64]struct{}, min(len(s.queue), s.opts.MaxChannels))
	selected := -1
	bestPriority := int(-1)
	pickLow := s.schedulerPackets%8 == 7
	for i, f := range s.queue {
		if f.c == nil {
			continue
		}
		if _, ok := seen[f.c.id]; ok {
			continue
		}
		seen[f.c.id] = struct{}{}
		if f.frame.Type == wire.FrameData && f.reliable {
			_, _, total, offset, _, ok := decodeData(f.frame.Body)
			if !ok {
				continue
			}
			if offset == 0 && !f.reclaimCredit && (s.sentCredit > s.peerLimit || total > s.peerLimit-s.sentCredit) {
				continue
			}
		}
		priority := int(f.c.cfg.Priority)
		if selected < 0 || (pickLow && priority < bestPriority) || (!pickLow && priority > bestPriority) || (priority == bestPriority && f.c.id > s.lastServedChannel && s.queue[selected].c.id <= s.lastServedChannel) {
			selected = i
			bestPriority = priority
		}
	}
	return selected
}

// skipBlockedCredit rotates a previously-started fragment to the front.
// This prevents a new message waiting on receiver credit from blocking data
// needed to finish an earlier already-credited message.
func (s *Session) skipBlockedCredit() bool {
	for i := 1; i < len(s.queue); i++ {
		f := s.queue[i]
		if f.frame.Type != wire.FrameData || !f.reliable {
			continue
		}
		_, _, _, offset, _, ok := decodeData(f.frame.Body)
		if !ok || offset == 0 {
			continue
		}
		copy(s.queue[1:i+1], s.queue[:i])
		s.queue[0] = f
		return true
	}
	return false
}

// queueAck collects authenticated, accepted packet numbers. A bounded range
// batch avoids an ACK storm for large fragmented messages and is repeated on
// duplicate retransmissions when a previous ACK datagram was lost.
func (s *Session) queueAck(pn uint64) {
	if s.acksPending == nil {
		s.acksPending = make(map[uint64]struct{})
	}
	if len(s.acksPending) >= 256 {
		s.flushAcks()
	}
	if _, seen := s.acksPending[pn]; !seen {
		s.acksHistory = append(s.acksHistory, pn)
		if len(s.acksHistory) > 128 {
			s.acksHistory = append([]uint64(nil), s.acksHistory[len(s.acksHistory)-128:]...)
		}
	}
	s.acksPending[pn] = struct{}{}
	if len(s.acksPending) >= 16 {
		s.flushAcks()
	}
}
func (s *Session) flushAcks() {
	if len(s.acksPending) == 0 {
		return
	}
	if s.nextPN >= (uint64(1)<<16)*keyEpochPackets {
		s.stop()
		return
	}
	// Service pending ACKs first, otherwise unsent older ACKs can become
	// permanently shadowed by a hot history window and cause an ACK storm.
	list := make([]uint64, 0, 32)
	for pn := range s.acksPending {
		list = append(list, pn)
	}
	sort.Slice(list, func(i, j int) bool { return list[i] > list[j] })
	if len(list) > 32 {
		list = list[:32]
	}
	if len(list) < 32 {
		seen := make(map[uint64]struct{}, len(list))
		for _, n := range list {
			seen[n] = struct{}{}
		}
		for i := len(s.acksHistory) - 1; i >= 0 && len(list) < 32; i-- {
			pn := s.acksHistory[i]
			if _, ok := seen[pn]; !ok {
				list = append(list, pn)
				seen[pn] = struct{}{}
			}
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i] > list[j] })
	body, err := wire.AppendFrame(nil, wire.Frame{Type: wire.FrameAckRanges, Body: encodeAckRanges(list)})
	if err != nil {
		return
	}
	pn := s.nextPN
	s.nextPN++
	protector, epoch, err := s.encryptor(pn)
	if err != nil {
		s.stop()
		return
	}
	raw, err := sealedEpoch(protector, s.id, pn, wire.KindData, 0, epoch, body)
	if err != nil {
		s.stop()
		return
	}
	if _, err = s.writeUDP(raw, s.getRemote()); err != nil {
		s.stop()
		return
	}
	s.lastSent = time.Now()
	s.ackRangesSent++
	for _, n := range list {
		delete(s.acksPending, n)
	}
}
func (s *Session) pacingDelay(bytes int) time.Duration {
	rtt := s.srtt
	if rtt < minPacingRTT {
		rtt = minPacingRTT
	}
	cwnd := max(2*wire.MaxDatagram, s.cwnd)
	if s.cwnd < s.ssthresh {
		// Slow start must be able to double the window every RTT, which needs a
		// send rate above cwnd/RTT. Pace at 2x until the first loss.
		cwnd *= 2
	}
	delay := time.Duration(int64(rtt) * int64(bytes) / int64(cwnd))
	if delay < pacerFloor {
		delay = pacerFloor
	}
	if delay > 25*time.Millisecond {
		delay = 25 * time.Millisecond
	}
	return delay
}

const (
	// ackDelay bounds how long an accepted packet waits for its ACK.
	ackDelay = time.Millisecond
	// minPacingRTT floors the RTT used to derive the pacing rate (cwnd/RTT).
	// It was 25 ms, which throttled every low-latency path to cwnd/25ms
	// regardless of its real RTT.
	minPacingRTT = time.Millisecond
	// initialRTO applies until the first RTT sample; minRTOMargin is the least
	// slack added to srtt for the retransmission timer.
	initialRTO   = 500 * time.Millisecond
	minRTOMargin = 10 * time.Millisecond
	pacerFloor   = 50 * time.Microsecond
	// paceBurst is how many packets' worth of idle time the pacer banks. Real
	// timers are too coarse to space every packet by tens of microseconds, so
	// short bursts are allowed while the long-run rate stays cwnd/RTT.
	paceBurst = 10
)

// advancePacer charges one sent packet against a virtual-time pacer: idle time
// earns at most paceBurst packets of credit, and each packet then costs one
// pacing interval. The long-run send rate is unchanged.
func (s *Session) advancePacer(now time.Time, bytes int) {
	delay := s.pacingDelay(bytes)
	if earliest := now.Add(-paceBurst * delay); s.nextPaced.Before(earliest) {
		s.nextPaced = earliest
	}
	s.nextPaced = s.nextPaced.Add(delay)
}
func (s *Session) processPacket(input inboundPacket) {
	p := input.raw
	h, err := wire.ParseHeader(p)
	if err != nil || h.Connection != s.id || h.Kind != wire.KindData || h.Flags & ^uint8(ackRequired) != 0 {
		return
	}
	if h.Number < 2 || h.Number >= (uint64(1)<<16)*keyEpochPackets || uint64(h.Epoch) != h.Number/keyEpochPackets || s.replay.TooOld(h.Number) {
		return
	}
	// Bounds the attacker-controlled epoch jump before deriving new keys.
	if h.Number > s.maxRecvPN+(1<<16) {
		return
	}
	protector, err := s.decryptor(h.Epoch)
	if err != nil {
		return
	}
	_, body, err := openedEpoch(protector, p, wire.KindData)
	if err != nil {
		return
	}
	frames, err := wire.ParseFrames(body, 1)
	if err != nil || len(frames) != 1 || frames[0].Flags != 0 {
		return
	}
	// An authenticated packet from a different source address alone does not
	// authorize switching the destination: prove bidirectional reachability.
	// Never dispatch user data or ACKs from an unvalidated path.
	if !addrEqual(input.from, s.getRemote()) {
		if s.server == nil {
			return
		} // server-side authenticated path validation
		s.onCandidatePacket(input.from, len(p), frames[0])
		return
	}
	s.lastRecv = time.Now()
	if s.tlsReady != nil {
		s.tlsReadyOnce.Do(func() { close(s.tlsReady) })
	}
	if h.Number > s.maxRecvPN {
		s.maxRecvPN = h.Number
	}
	// Distinct packet numbers within 1024-packet anti-replay window.
	if h.Number < 2 || s.replay.TooOld(h.Number) {
		return
	}
	if s.replay.Seen(h.Number) {
		if h.Flags&ackRequired != 0 {
			s.queueAck(h.Number)
		}
		return
	}
	accepted := true
	for _, f := range frames {
		if !s.onFrame(f) {
			accepted = false
		}
	}
	if accepted {
		s.replay.CheckAndMark(h.Number)
		if h.Flags&ackRequired != 0 {
			s.queueAck(h.Number)
		}
	}
	// Backpressured packets remain unmarked, so the sender's encrypted retry
	// can be processed once receiver buffers become available.
}

// onCandidatePacket accepts only a valid encrypted PATH_RESPONSE from the
// candidate address. A fresh challenge is bounded to <=3x authenticated bytes
// observed from that address; a single candidate and timeout bound memory.
func (s *Session) onCandidatePacket(addr *net.UDPAddr, n int, f wire.Frame) {
	now := time.Now()
	cand := s.path
	if cand != nil && now.Sub(cand.first) > 5*time.Second {
		cand = nil
		s.path = nil
	}
	if cand != nil && addrEqual(cand.addr, addr) {
		cand.received += n
		if f.Type == wire.FramePathResponse && len(f.Body) == 8 && string(f.Body) == string(cand.token[:]) {
			s.setRemote(addr)
			firstValidation := s.bootstrapTLS && !s.validated
			s.validated = true
			s.path = nil
			if firstValidation {
				// Only authenticated, return-path-validated clients are exposed to applications.
				select {
				case s.server.accepts <- s:
				default:
					s.stop()
					return
				}
			}
			s.migrations++
			// Reframe outstanding jumbo reliable messages before forgetting
			// the previous path's validated envelope. Otherwise new-path
			// 1200-byte links could stall on old 1400-byte ciphertext.
			if s.pathMTU > 1200 {
				s.recoverMTUBlackhole(now)
			}
			s.resetPathMTU()
			s.cwnd = min(s.cwnd, 10*wire.MaxDatagram)
			s.ssthresh = max(s.cwnd, 2*wire.MaxDatagram)
			s.nextPaced = time.Time{}
			// Retransmit unACKed packets to the validated new route on next tick.
			for _, p := range s.pending {
				p.sent = now.Add(-3 * time.Second)
			}
			s.lastRecv = now
			return
		}
		if now.Sub(cand.lastChallenge) >= 500*time.Millisecond {
			s.sendPathChallenge(cand)
		}
		return
	}
	if cand != nil && now.Sub(cand.first) < 500*time.Millisecond {
		return
	}
	cand = &candidatePath{addr: addr, first: now, received: n}
	if _, err := rand.Read(cand.token[:]); err != nil {
		return
	}
	s.path = cand
	s.sendPathChallenge(cand)
}
func (s *Session) sendPathChallenge(c *candidatePath) {
	b, err := wire.AppendFrame(nil, wire.Frame{Type: wire.FramePathChallenge, Body: c.token[:]})
	if err != nil {
		return
	}
	if s.nextPN >= (uint64(1)<<16)*keyEpochPackets {
		return
	}
	pn := s.nextPN
	protector, epoch, err := s.encryptor(pn)
	if err != nil {
		return
	}
	raw, err := sealedEpoch(protector, s.id, pn, wire.KindData, 0, epoch, b)
	if err != nil || c.sent+len(raw) > 3*c.received {
		return
	}
	s.nextPN++
	if _, err = s.writeUDP(raw, c.addr); err == nil {
		c.sent += len(raw)
		c.lastChallenge = time.Now()
	}
}

// admitApplicationBytes limits the post-authentication processing budget.
// Reliable senders retry dropped packets after backpressure clears; best-effort
// data may be discarded. This is per session; cross-session quotas belong in
// the authenticated identity admission layer.
func (s *Session) admitApplicationBytes(n int) bool {
	rate := s.opts.MaxInboundBytesPerSecond
	if rate == 0 {
		return true
	}
	now := time.Now()
	burst := float64(max(rate, int64(fragmentSize)))
	if s.inboundLast.IsZero() {
		s.inboundLast = now
		s.inboundTokens = burst
	}
	elapsed := now.Sub(s.inboundLast).Seconds()
	s.inboundLast = now
	s.inboundTokens += elapsed * float64(rate)
	if s.inboundTokens > burst {
		s.inboundTokens = burst
	}
	if float64(n) > s.inboundTokens {
		s.rateLimitedBytes += uint64(n)
		return false
	}
	s.inboundTokens -= float64(n)
	return true
}

func (s *Session) onFrame(f wire.Frame) bool {
	switch f.Type {
	case wire.FrameAck:
		pn, n := binary.Uvarint(f.Body)
		if n <= 0 || n != len(f.Body) {
			return false
		}
		s.onAck(pn)
		return true
	case wire.FrameAckRanges:
		ranges, err := decodeAckRanges(f.Body)
		if err != nil {
			return false
		}
		// Validate the entire authenticated ACK frame before mutating state.
		for _, r := range ranges {
			if r.hi >= s.nextPN || r.lo < 2 {
				return false
			}
		}
		highest := ranges[0].hi
		for _, r := range ranges {
			for pn := r.lo; pn <= r.hi; pn++ {
				s.onAck(pn)
			}
		}
		// Packet-threshold loss detection with a reordering time guard.
		// Retransmit the original ciphertext, never reseal a reused PN.
		now := time.Now()
		reordering := s.srtt / 8
		if reordering < 10*time.Millisecond {
			reordering = 10 * time.Millisecond
		}
		for pn, p := range s.pending {
			if highest > pn+3 && p.tries == 0 && now.Sub(p.sent) > reordering {
				p.sent = now.Add(-2 * time.Second)
				s.fastLossSignals++
			}
		}
		return true
	case wire.FrameOpen:
		return s.onOpen(f.Body)
	case wire.FrameReceipt:
		cid, n := binary.Uvarint(f.Body)
		if n <= 0 {
			return false
		}
		id, k := binary.Uvarint(f.Body[n:])
		if k <= 0 || n+k+1 != len(f.Body) || id == 0 {
			return false
		}
		level := protocol.ReceiptLevel(f.Body[n+k])
		if level != protocol.AckStored && level != protocol.AckProcessed {
			return false
		}
		if c := s.channels[cid]; c != nil {
			c.markReceipt(id, level)
		}
		return true
	case wire.FrameOpenAck:
		id, n := binary.Uvarint(f.Body)
		if n <= 0 || n != len(f.Body) {
			return false
		}
		c := s.channels[id]
		if c == nil {
			return true
		}
		if c.ready != nil {
			select {
			case c.ready <- nil:
			default:
			}
			c.ready = nil
		}
		return true
	case wire.FrameData:
		return s.onData(f.Body)
	case wire.FrameClose, wire.FrameReset:
		id, n := binary.Uvarint(f.Body)
		if n <= 0 || n != len(f.Body) {
			return false
		}
		c := s.channels[id]
		if c != nil {
			if c.ready != nil {
				select {
				case c.ready <- ErrClosed:
				default:
				}
				c.ready = nil
			}
			s.closeChannel(c)
		}
		return true
	case wire.FrameCredit:
		limit, n := binary.Uvarint(f.Body)
		if n <= 0 || n != len(f.Body) {
			return false
		}
		if limit > s.peerLimit {
			s.peerLimit = limit
		}
		return true
	case wire.FramePathChallenge:
		if len(f.Body) != 8 {
			return false
		}
		// Echo the validated server's challenge; control framing is encrypted.
		s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FramePathResponse, Body: append([]byte(nil), f.Body...)}, reliable: true})
		return true
	case wire.FramePathResponse:
		// Only a response arriving on an unvalidated candidate path is useful.
		return len(f.Body) == 8
	case wire.FrameGoAway:
		s.stop()
		return true
	case wire.FrameHalfClose:
		id, n := binary.Uvarint(f.Body)
		if n <= 0 {
			return false
		}
		last, k := binary.Uvarint(f.Body[n:])
		if k <= 0 || n+k != len(f.Body) || id == 0 {
			return false
		}
		if c := s.channels[id]; c != nil {
			if c.cfg.Delivery != protocol.ReliableOrdered {
				return false
			}
			c.remoteWriteClosed = true
			c.remoteFinalID = last
			c.maybeReadEOF()
		}
		return true
	case wire.FramePing, wire.FrameMTUProbe:
		return true
	default:
		return false
	}
}
func (s *Session) onAck(pn uint64) {
	p := s.pending[pn]
	if p == nil {
		return
	}
	delete(s.pending, pn)
	// Any authenticated acknowledgment is evidence that the connection still
	// makes forward progress. End a persistent-congestion episode only on ACK.
	s.lastAckTime = time.Now()
	s.persistentRecovery = false
	if p.frame.probeMTU > 0 {
		s.onProbeSuccess(p.frame.probeMTU)
		if p.frame.probeReply != nil {
			select {
			case p.frame.probeReply <- s.pathMTU:
			default:
			}
		}
	}
	s.inFlightBytes -= p.size
	if p.frame.rebindReply != nil {
		select {
		case p.frame.rebindReply <- nil:
		default:
		}
	}
	if p.size <= 1200 {
		s.lastSmallAck = time.Now()
	}
	if p.tries == 0 {
		rtt := time.Since(p.sent)
		if s.srtt == 0 {
			s.srtt = rtt
			s.rttvar = rtt / 2
		} else {
			diff := s.srtt - rtt
			if diff < 0 {
				diff = -diff
			}
			s.rttvar = (3*s.rttvar + diff) / 4
			s.srtt = (7*s.srtt + rtt) / 8
		}
	}
	if s.cwnd < s.ssthresh {
		s.cwnd += p.size
	} else {
		s.cwnd += wire.MaxDatagram * wire.MaxDatagram / s.cwnd
	}
	if s.cwnd > 2<<20 {
		s.cwnd = 2 << 20
	}
	if p.frame.c != nil && p.frame.reliable {
		p.frame.c.ackFragment(p.frame.msgID)
	}
}
func (s *Session) retry(now time.Time) {
	if s.bootstrapTLS && !s.validated {
		return
	}
	// Before the first RTT sample the path is unknown, and early packets can
	// legitimately wait more than one RTT (the peer cannot acknowledge before
	// its path validation completes), so be patient.
	timeout := initialRTO
	if s.srtt != 0 {
		// Leave margin for ACK delay and jitter: on a stable path rttvar tends
		// to zero, which would otherwise put the timer at the RTT itself and
		// retransmit packets whose ACK is merely a few milliseconds late.
		timeout = s.srtt + max(4*s.rttvar, s.srtt/4, minRTOMargin) + ackDelay
	}
	if timeout < 100*time.Millisecond {
		timeout = 100 * time.Millisecond
	}
	if timeout > time.Second {
		timeout = time.Second
	}
	for _, p := range s.pending {
		backoff := time.Duration(1) << min(p.tries, 2)
		if now.Sub(p.sent) < timeout*backoff {
			continue
		}
		if now.Before(s.nextPaced) {
			return
		}
		if p.frame.probeMTU > 0 && p.tries >= 2 {
			s.onProbeFailure()
			if p.frame.probeReply != nil {
				select {
				case p.frame.probeReply <- 0:
				default:
				}
			}
			// probes are optional; a dropped probe cannot tear down the session.
			for pn, check := range s.pending {
				if check == p {
					delete(s.pending, pn)
					s.inFlightBytes -= p.size
					break
				}
			}
			continue
		}
		if p.frame.probeMTU == 0 && p.frame.c != nil && p.size > 1200 && p.tries >= 3 {
			s.recoverMTUBlackhole(now)
			return
		}
		// Conservative persistent-congestion response: several PTOs without
		// authenticated ACK progress collapse the data congestion window to
		// its floor. This is a defensive heuristic, not RFC 9002 validation.
		s.detectPersistentCongestion(now, p, timeout)
		if p.tries >= s.opts.Retries {
			if p.frame.rebindReply != nil {
				select {
				case p.frame.rebindReply <- ErrTimeout:
				default:
				}
			}
			s.stop()
			return
		}
		// Only a lost application DATA packet is a congestion signal. Control
		// frames sent while the server is still validating the client's UDP
		// path (CREDIT, PATH_RESPONSE, OPEN) are discarded by design; counting
		// their retransmission as loss halved cwnd at startup, switched off
		// slow start for the whole session, and cost a 64 KiB message ~7 RTT.
		// A timeout before any RTT sample is not evidence of congestion either.
		if p.tries == 0 && s.srtt != 0 && p.frame.frame.Type == wire.FrameData && now.Sub(s.lastLoss) > max(s.srtt, 150*time.Millisecond) {
			s.ssthresh = max(2*wire.MaxDatagram, s.cwnd/2)
			s.cwnd = s.ssthresh
			s.lastLoss = now
		}
		// Same byte-for-byte authenticated datagram retransmitted. No second
		// encryption with a reused nonce occurs.
		_, err := s.writeUDP(p.raw, s.getRemote())
		if err != nil {
			s.stop()
			return
		}
		p.tries++
		s.retransmissions++
		p.sent = now
		s.lastSent = now
		s.nextPaced = now.Add(s.pacingDelay(p.size))
	}
}

// detectPersistentCongestion responds once per interval of sustained losses.
// The actor owns this state. Packet bytes, not retransmission counts, remain
// bounded by the congestion window and the pacer.
func (s *Session) detectPersistentCongestion(now time.Time, p *sentPacket, pto time.Duration) {
	if p == nil || p.tries < 2 || p.frame.probeMTU != 0 || pto <= 0 || s.persistentRecovery || now.Sub(p.first) < 3*pto {
		return
	}
	if !s.lastAckTime.IsZero() && now.Sub(s.lastAckTime) < 3*pto {
		return
	}
	s.persistentRecovery = true
	s.persistentCongestion++
	s.cwnd = 2 * wire.MaxDatagram
	s.ssthresh = s.cwnd
	s.nextPaced = now.Add(s.pacingDelay(p.size))
}

func (s *Session) expireAssemblies(now time.Time) {
	for _, c := range s.channels {
		for id, a := range c.assemblies {
			if now.Sub(a.last) > 30*time.Second {
				delete(c.assemblies, id)
				s.recvBytes.Add(-int64(len(a.payload)))
				s.consumedBytes.Add(int64(len(a.payload)))
			}
		}
	}
}
func (s *Session) drainCompleted() {
	for _, c := range s.channels {
		if c.cfg.Delivery == protocol.ReliableOrdered {
			for {
				m, ok := c.completed[c.nextDeliver]
				if !ok {
					break
				}
				select {
				case c.recv <- m:
					delete(c.completed, c.nextDeliver)
					c.nextDeliver++
				default:
					goto done
				}
			}
		}
		c.maybeReadEOF()
	done:
	}
}

func encodeCredit(v uint64) []byte { return binary.AppendUvarint(nil, v) }
func (s *Session) publishCredit() {
	limit := uint64(s.consumedBytes.Load()) + uint64(s.opts.MaxReceiveBytes)
	if limit <= s.lastCredit {
		return
	}
	// Coalesce small updates; still send a credit when entire session is stalled.
	if limit-s.lastCredit < 8*1024 && s.lastCredit != 0 {
		return
	}
	s.lastCredit = limit
	s.enqueueControl(txFrame{frame: wire.Frame{Type: wire.FrameCredit, Body: encodeCredit(limit)}, reliable: true})
}

// Encryption/decryption uses packet-number-bound epochs; old datagrams are
// retransmitted verbatim. Because packet numbers never reset, a key/nonce pair
// cannot be reused within the session even when packets are reordered.
func (s *Session) encryptor(pn uint64) (*cryptokit.Protector, uint16, error) {
	epoch := uint16(pn / keyEpochPackets)
	if epoch < s.txEpoch || epoch > s.txEpoch+1 {
		return nil, 0, wire.ErrInvalid
	}
	if epoch != s.txEpoch {
		next := advanceChain(s.txChain, epoch)
		p, err := s.schedule.fromChain(next, epoch, true)
		if err != nil {
			return nil, 0, err
		}
		clear(s.txChain[:])
		s.txChain = next
		s.tx = p
		s.txEpoch = epoch
	}
	return s.tx, epoch, nil
}
func (s *Session) decryptor(epoch uint16) (*cryptokit.Protector, error) {
	if epoch == s.rxEpoch {
		return s.rx, nil
	}
	if s.rxEpoch > 0 && epoch == s.rxEpoch-1 && s.rxPrevious != nil {
		return s.rxPrevious, nil
	}
	if epoch != s.rxEpoch+1 {
		return nil, wire.ErrInvalid
	}
	next := advanceChain(s.rxChain, epoch)
	p, err := s.schedule.fromChain(next, epoch, false)
	if err != nil {
		return nil, err
	}
	clear(s.rxChain[:])
	s.rxChain = next
	s.rxPrevious = s.rx
	s.rx = p
	s.rxEpoch = epoch
	return p, nil
}
