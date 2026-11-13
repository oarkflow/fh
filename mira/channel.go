package mira

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/internal/wire"
	"github.com/oarkflow/fh/protocol"
)

type assembly struct {
	chunkSize uint64
	payload   []byte
	got       []bool
	count     int
	last      time.Time
}

type receiptState struct {
	left          int
	bytes         int64
	payload       []byte // retained only until all transport fragments are ACKed
	done          chan struct{}
	err           error
	complete      bool
	stored        bool
	processed     bool
	storedDone    chan struct{}
	processedDone chan struct{}
}

// Channel is a bidirectional independently ordered/delivered logical stream.
type Channel struct {
	s                 *Session
	sendMu            sync.Mutex
	writeClosed       atomic.Bool
	readEOF           chan struct{}
	readEOFOnce       sync.Once
	remoteWriteClosed bool   // actor only
	remoteFinalID     uint64 // actor only
	id                uint64
	cfg               protocol.ChannelConfig
	recv              chan protocol.Message
	ready             chan error // only used for locally-opened channels
	closed            atomic.Bool
	closeOnce         sync.Once
	closedCh          chan struct{}
	expires           time.Time
	nextMsg           atomic.Uint64
	nextDeliver       uint64                      // owned by session event loop
	lastState         uint64                      // owned by session event loop
	assemblies        map[uint64]*assembly        // owned by loop
	completed         map[uint64]protocol.Message // owned by loop
	seen              map[uint64]struct{}         // owned by loop
	seenOrder         []uint64
	maxSeen           uint64
	mu                sync.Mutex
	receipts          map[uint64]*receiptState
}

func newChannel(s *Session, id uint64, cfg protocol.ChannelConfig, local bool) *Channel {
	n := 32
	if cfg.Delivery == protocol.LatestState {
		n = 1
	}
	c := &Channel{s: s, id: id, cfg: cfg, recv: make(chan protocol.Message, n), closedCh: make(chan struct{}), readEOF: make(chan struct{}), nextDeliver: 1,
		assemblies: make(map[uint64]*assembly), completed: make(map[uint64]protocol.Message), seen: make(map[uint64]struct{}), receipts: make(map[uint64]*receiptState)}
	if cfg.Lease > 0 {
		c.expires = time.Now().Add(cfg.Lease)
	}
	if local {
		c.ready = make(chan error, 1)
	}
	return c
}
func (c *Channel) ID() uint64 { return c.id }

// Config returns the negotiated channel configuration. Servers use it to route
// an accepted channel by purpose (for example LatestState telemetry versus
// ReliableOrdered commands).
func (c *Channel) Config() protocol.ChannelConfig { return c.cfg }
func (c *Channel) signalClose() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closedCh)
		c.mu.Lock()
		for _, r := range c.receipts {
			if !r.complete {
				r.payload = nil
				r.complete = true
				r.err = ErrClosed
				close(r.done)
				c.s.releaseQueue(r.bytes)
			}
		}
		c.mu.Unlock()
	})
}
func (c *Channel) expired() bool { return !c.expires.IsZero() && time.Now().After(c.expires) }
func (c *Channel) Send(ctx context.Context, payload []byte) (uint64, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.writeClosed.Load() {
		return 0, ErrClosed
	}
	if c.closed.Load() {
		return 0, ErrClosed
	}
	if c.expired() {
		return 0, ErrExpired
	}
	if len(payload) > int(c.cfg.MaxMessage) {
		return 0, protocol.ErrBackpressure
	}
	select {
	case <-c.s.done:
		return 0, ErrClosed
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if (c.cfg.Delivery == protocol.BestEffort || c.cfg.Delivery == protocol.LatestState) && len(payload) > fragmentSize {
		return 0, protocol.ErrBackpressure
	}
	if !c.s.reserveQueue(int64(len(payload) + 64)) {
		return 0, ErrBackpressure
	}
	id := c.nextMsg.Load() + 1
	reliable := c.cfg.Delivery != protocol.BestEffort && c.cfg.Delivery != protocol.LatestState
	if reliable {
		c.mu.Lock()
		if len(c.receipts) >= 4096 {
			for k, r := range c.receipts {
				if r.complete {
					delete(c.receipts, k)
				}
			}
		}
		if len(c.receipts) >= 8192 {
			c.mu.Unlock()
			c.s.releaseQueue(int64(len(payload) + 64))
			return 0, ErrBackpressure
		}
		c.receipts[id] = &receiptState{done: make(chan struct{}), storedDone: make(chan struct{}), processedDone: make(chan struct{}), bytes: int64(len(payload) + 64)}
		c.mu.Unlock()
	}
	req := sendRequest{c: c, id: id, data: append([]byte(nil), payload...), reserved: int64(len(payload) + 64)}
	select {
	case <-ctx.Done():
		c.dropReceipt(id)
		c.s.releaseQueue(req.reserved)
		return 0, ctx.Err()
	case <-c.s.done:
		c.dropReceipt(id)
		c.s.releaseQueue(req.reserved)
		return 0, ErrClosed
	case c.s.requests <- req:
		c.nextMsg.Store(id)
		return id, nil
	default:
		c.dropReceipt(id)
		c.s.releaseQueue(req.reserved)
		return 0, ErrBackpressure
	}
}
func (c *Channel) dropReceipt(id uint64) { c.mu.Lock(); delete(c.receipts, id); c.mu.Unlock() }
func (c *Channel) setLeft(id uint64, n int) {
	c.mu.Lock()
	if r := c.receipts[id]; r != nil {
		r.left = n
	}
	c.mu.Unlock()
}
func (c *Channel) ackFragment(id uint64) {
	c.mu.Lock()
	r := c.receipts[id]
	if r != nil && !r.complete {
		r.left--
		if r.left == 0 {
			r.payload = nil
			r.complete = true
			close(r.done)
			c.s.releaseQueue(r.bytes)
		}
	}
	c.mu.Unlock()
}

// AwaitReceipt distinguishes volatile transport reception from an explicit
// application assertion that an operation was fsynced or processed. Applications
// must call Acknowledge on the receiving channel after completing that step.
func (c *Channel) AwaitReceipt(ctx context.Context, id uint64, level protocol.ReceiptLevel) (protocol.Receipt, error) {
	if level < protocol.AckTransport || level > protocol.AckProcessed {
		return protocol.Receipt{}, ErrUnsupportedReceipt
	}
	c.mu.Lock()
	r := c.receipts[id]
	if r == nil {
		c.mu.Unlock()
		return protocol.Receipt{}, ErrUnknownReceipt
	}
	var notify <-chan struct{}
	switch level {
	case protocol.AckTransport:
		notify = r.done
	case protocol.AckStored:
		notify = r.storedDone
	case protocol.AckProcessed:
		notify = r.processedDone
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return protocol.Receipt{}, ctx.Err()
	case <-c.s.done:
		return protocol.Receipt{}, ErrClosed
	case <-c.closedCh:
		return protocol.Receipt{}, ErrClosed
	case <-notify:
	}
	c.mu.Lock()
	err := r.err
	c.mu.Unlock()
	if err != nil {
		return protocol.Receipt{}, err
	}
	return protocol.Receipt{MessageID: id, Level: level, At: time.Now()}, nil
}
func (c *Channel) markReceipt(id uint64, level protocol.ReceiptLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.receipts[id]
	if r == nil {
		return
	}
	if level >= protocol.AckStored && !r.stored {
		r.stored = true
		close(r.storedDone)
	}
	if level >= protocol.AckProcessed && !r.processed {
		r.processed = true
		close(r.processedDone)
	}
	// An application receipt also confirms that a complete message was accepted,
	// even if the transport ACK datagram was lost.
	if !r.complete {
		r.complete = true
		close(r.done)
		c.s.releaseQueue(r.bytes)
	}
}

// Acknowledge sends an authenticated application-level receipt. The caller
// alone is responsible for ensuring that AckStored follows a durable commit
// and AckProcessed follows completion of the business operation.
func (c *Channel) Acknowledge(ctx context.Context, id uint64, level protocol.ReceiptLevel) error {
	if level != protocol.AckStored && level != protocol.AckProcessed {
		return ErrUnsupportedReceipt
	}
	if c.closed.Load() {
		return ErrClosed
	}
	body := binary.AppendUvarint(nil, c.id)
	body = binary.AppendUvarint(body, id)
	body = append(body, byte(level))
	req := receiptRequest{frame: wire.Frame{Type: wire.FrameReceipt, Body: body}}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.s.done:
		return ErrClosed
	case c.s.requests <- req:
		return nil
	}
}

// CloseWrite gracefully ends the sending half of a ReliableOrdered channel.
// Previously accepted messages continue delivering; Receive remains available
// until the peer's own CloseWrite. Half-close is rejected for unordered modes.
func (c *Channel) CloseWrite(ctx context.Context) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	if c.cfg.Delivery != protocol.ReliableOrdered {
		return errors.New("mira: half-close requires ordered reliable channel")
	}
	if c.writeClosed.Load() {
		return nil
	}
	req := halfCloseRequest{id: c.id, final: c.nextMsg.Load()}
	select {
	case c.s.requests <- req:
		c.writeClosed.Store(true)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.s.done:
		return ErrClosed
	}
}

// maybeReadEOF is called only by the owner goroutine when all previous
// ordered messages have reached the receive queue.
func (c *Channel) maybeReadEOF() {
	if c.remoteWriteClosed && c.nextDeliver > c.remoteFinalID {
		c.readEOFOnce.Do(func() { close(c.readEOF) })
	}
}
func (c *Channel) Receive(ctx context.Context) (protocol.Message, error) {
	// Drain already-buffered messages first, including when the channel closed.
	select {
	case m := <-c.recv:
		c.s.recvBytes.Add(-int64(len(m.Payload)))
		c.s.consumedBytes.Add(int64(len(m.Payload)))
		select {
		case c.s.wake <- struct{}{}:
		default:
		}
		return m, nil
	default:
	}
	select {
	case m := <-c.recv:
		c.s.recvBytes.Add(-int64(len(m.Payload)))
		c.s.consumedBytes.Add(int64(len(m.Payload)))
		select {
		case c.s.wake <- struct{}{}:
		default:
		}
		return m, nil
	case <-c.readEOF:
		// Do not return EOF ahead of queued messages even if both cases ready.
		select {
		case m := <-c.recv:
			c.s.recvBytes.Add(-int64(len(m.Payload)))
			c.s.consumedBytes.Add(int64(len(m.Payload)))
			select {
			case c.s.wake <- struct{}{}:
			default:
			}
			return m, nil
		default:
		}
		return protocol.Message{}, io.EOF
	case <-c.closedCh:
		return protocol.Message{}, ErrClosed
	case <-c.s.done:
		return protocol.Message{}, ErrClosed
	case <-ctx.Done():
		return protocol.Message{}, ctx.Err()
	}
}
func (c *Channel) Close(ctx context.Context) error {
	if c.closed.Load() {
		return nil
	}
	c.signalClose()
	req := closeRequest{id: c.id}
	select {
	case c.s.requests <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.s.done:
		return ErrClosed
	}
}

func encodeOpen(id uint64, c protocol.ChannelConfig) []byte {
	b := binary.AppendUvarint(nil, id)
	b = append(b, byte(c.Delivery), c.Priority)
	b = binary.AppendUvarint(b, uint64(c.MaxMessage))
	lease := uint64(0)
	if c.Lease > 0 {
		lease = uint64(c.Lease / time.Millisecond)
	}
	b = binary.AppendUvarint(b, lease)
	return b
}
func decodeOpen(body []byte) (uint64, protocol.ChannelConfig, bool) {
	id, n := binary.Uvarint(body)
	if n <= 0 || n+2 > len(body) {
		return 0, protocol.ChannelConfig{}, false
	}
	body = body[n:]
	cfg := protocol.ChannelConfig{Delivery: protocol.Delivery(body[0]), Priority: body[1]}
	body = body[2:]
	max, n := binary.Uvarint(body)
	if n <= 0 {
		return 0, cfg, false
	}
	body = body[n:]
	lease, n := binary.Uvarint(body)
	if n <= 0 || n != len(body) {
		return 0, cfg, false
	}
	if cfg.Delivery < protocol.ReliableOrdered || cfg.Delivery > protocol.Bulk || max == 0 || max > (1<<20) || lease > 86_400_000 {
		return 0, cfg, false
	}
	cfg.MaxMessage = int64(max)
	cfg.Lease = time.Duration(lease) * time.Millisecond
	return id, cfg, true
}

// DATA v4 contains the sender's chosen fragment size. A message's fragments
// must all agree on this size; this permits PMTU discovery without corrupting
// in-flight reassembly when the path changes between messages.
func encodeDataSized(id, msg, total, offset, size uint64, data []byte) []byte {
	b := binary.AppendUvarint(nil, id)
	b = binary.AppendUvarint(b, msg)
	b = binary.AppendUvarint(b, total)
	b = binary.AppendUvarint(b, offset)
	b = binary.AppendUvarint(b, size)
	return append(b, data...)
}
func decodeData(b []byte) (cid, msg, total, offset uint64, data []byte, ok bool) {
	cid, msg, total, offset, _, data, ok = decodeDataSized(b)
	return
}
func decodeDataSized(b []byte) (cid, msg, total, offset, size uint64, data []byte, ok bool) {
	nums := []*uint64{&cid, &msg, &total, &offset, &size}
	for _, v := range nums {
		n, k := binary.Uvarint(b)
		if k <= 0 {
			return
		}
		var canonical [10]byte
		if binary.PutUvarint(canonical[:], n) != k {
			return
		}
		*v = n
		b = b[k:]
	}
	return cid, msg, total, offset, size, b, true
}

// payloadForRecovery is only called from the session actor. It returns the
// already-accounted reliable payload, not another copy.
func (c *Channel) payloadForRecovery(id uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.receipts[id]
	if r == nil || r.complete {
		return nil
	}
	return r.payload
}
func (c *Channel) resetRecoveryFragments(id uint64, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.receipts[id]; r != nil && !r.complete {
		r.left = n
	}
}
