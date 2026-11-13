// Package transfer moves a file or stream over one MIRA reliable channel with
// end-to-end integrity and an application-level commit receipt.
//
// Wire framing, one MIRA message each, first byte is the type:
//
//	'H' header  JSON {"name","size"}
//	'D' data    raw bytes (at most ChunkSize)
//	'E' end     32-byte SHA-256 of all data bytes
//
// The receiver verifies size and digest, runs the caller's commit hook, and
// only then sends AckProcessed for the 'E' message. Send returns nil only
// after that receipt, so success means "committed", not "packets arrived".
// The channel must be ReliableOrdered (chunk order matters) and carries one
// transfer at a time.
package transfer

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/oarkflow/fh/protocol"
)

// ChunkSize is the payload size of one data message.
const ChunkSize = 64 << 10

var (
	ErrTooLarge  = errors.New("transfer: size exceeds receiver limit")
	ErrIntegrity = errors.New("transfer: digest or size mismatch")
	ErrProtocol  = errors.New("transfer: protocol violation")
)

// Header describes an incoming transfer. The name is advisory: never use it as
// a filesystem path without sanitizing it.
type Header struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// Send streams r (exactly size bytes) and waits for the receiver's commit.
func Send(ctx context.Context, ch protocol.Channel, name string, r io.Reader, size int64) (err error) {
	// The receiver reports a rejection as a message on the same channel. Watch
	// for it so a refused transfer aborts at once instead of streaming the
	// whole file or waiting forever for a receipt that will never come.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rejected := make(chan string, 1)
	go func() {
		if m, rerr := ch.Receive(ctx); rerr == nil {
			reason := "transfer_rejected"
			if validRejectionCode(m.Payload) {
				reason = string(m.Payload)
			}
			rejected <- reason
			cancel()
		}
	}()
	defer func() {
		if err == nil {
			return
		}
		select {
		case reason := <-rejected:
			err = errors.New("transfer: receiver rejected: " + reason)
		default:
		}
	}()
	hdr, err := json.Marshal(Header{Name: name, Size: size})
	if err != nil {
		return err
	}
	if _, err := send(ctx, ch, append([]byte{'H'}, hdr...)); err != nil {
		return err
	}
	h := sha256.New()
	buf := make([]byte, 1+ChunkSize)
	buf[0] = 'D'
	var sent int64
	for sent < size {
		n, err := io.ReadFull(io.LimitReader(r, size-sent), buf[1:])
		if n > 0 {
			h.Write(buf[1 : 1+n])
			if _, serr := send(ctx, ch, buf[:1+n]); serr != nil {
				return serr
			}
			sent += int64(n)
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			break
		}
	}
	if sent != size {
		return fmt.Errorf("transfer: source ended after %d of %d bytes", sent, size)
	}
	id, err := send(ctx, ch, append([]byte{'E'}, h.Sum(nil)...))
	if err != nil {
		return err
	}
	_, err = ch.AwaitReceipt(ctx, id, protocol.AckProcessed)
	return err
}

// send retries on local backpressure: the session queue is bounded on purpose.
func send(ctx context.Context, ch protocol.Channel, b []byte) (uint64, error) {
	delay := 200 * time.Microsecond
	for {
		id, err := ch.Send(ctx, b)
		if err == nil || !isBackpressure(err) {
			return id, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(10*time.Millisecond, delay*2)
	}
}

func isBackpressure(err error) bool {
	return errors.Is(err, protocol.ErrBackpressure) || err.Error() == "mira: send queue full"
}

// Receive reads one transfer into dst. maxSize bounds the accepted size.
// commit, if non-nil, runs after verification and before AckProcessed; if it
// fails the sender is told and no receipt is sent. dst may hold partial data
// when an error is returned: callers should write to a temporary location and
// publish it only from commit.
func Receive(ctx context.Context, ch protocol.Channel, dst io.Writer, maxSize int64, commit func(Header) error) (Header, error) {
	reject := func(err error) (Header, error) {
		_, _ = ch.Send(ctx, []byte(rejectionCode(err)))
		return Header{}, err
	}
	m, err := ch.Receive(ctx)
	if err != nil {
		return Header{}, err
	}
	if len(m.Payload) < 2 || m.Payload[0] != 'H' {
		return reject(ErrProtocol)
	}
	var hdr Header
	if json.Unmarshal(m.Payload[1:], &hdr) != nil || hdr.Size < 0 {
		return reject(ErrProtocol)
	}
	if hdr.Size > maxSize {
		return reject(ErrTooLarge)
	}
	h := sha256.New()
	var got int64
	for {
		m, err = ch.Receive(ctx)
		if err != nil {
			return Header{}, err
		}
		if len(m.Payload) == 0 {
			return reject(ErrProtocol)
		}
		switch m.Payload[0] {
		case 'D':
			data := m.Payload[1:]
			got += int64(len(data))
			if got > hdr.Size {
				return reject(ErrIntegrity)
			}
			h.Write(data)
			if _, err := dst.Write(data); err != nil {
				return reject(fmt.Errorf("transfer: storage failed: %w", err))
			}
		case 'E':
			if got != hdr.Size || subtle.ConstantTimeCompare(m.Payload[1:], h.Sum(nil)) != 1 {
				return reject(ErrIntegrity)
			}
			if commit != nil {
				if err := commit(hdr); err != nil {
					return reject(fmt.Errorf("transfer: commit failed: %w", err))
				}
			}
			return hdr, ch.Acknowledge(ctx, m.MessageID, protocol.AckProcessed)
		default:
			return reject(ErrProtocol)
		}
	}
}

func rejectionCode(err error) string {
	switch {
	case errors.Is(err, ErrTooLarge):
		return "too_large"
	case errors.Is(err, ErrIntegrity):
		return "integrity_failed"
	case errors.Is(err, ErrProtocol):
		return "protocol_error"
	default:
		return "transfer_rejected"
	}
}

func validRejectionCode(code []byte) bool {
	if len(code) == 0 || len(code) > 64 {
		return false
	}
	for _, b := range code {
		if (b < 'a' || b > 'z') && (b < '0' || b > '9') && b != '_' {
			return false
		}
	}
	return true
}
