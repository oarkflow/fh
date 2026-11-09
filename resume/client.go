// Package resume restores durable application conversations over fresh MIRA
// sessions. It deliberately does NOT reuse old AEAD keys or packet numbers.
// The caller supplies a TLS-authenticated dialer for every new connection.
// Events may be delivered more than once if the client crashes after applying
// an effect but before acknowledging its cursor; effects must be idempotent.
package resume

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

type Dial func(context.Context) (*mira.Session, error)

type Client struct {
	Dial    Dial
	mu      sync.Mutex
	session *mira.Session
	ch      protocol.Channel
	closed  bool
}

func New(dial Dial) *Client { return &Client{Dial: dial} }
func (c *Client) connect(ctx context.Context) error {
	if c.closed {
		return mira.ErrClosed
	}
	if c.session != nil {
		return nil
	}
	if c.Dial == nil {
		return errors.New("resume: dialer required")
	}
	s, err := c.Dial(ctx)
	if err != nil {
		return err
	}
	ch, err := s.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		_ = s.Close(context.Background())
		return err
	}
	c.session = s
	c.ch = ch
	return nil
}
func (c *Client) drop() {
	s := c.session
	c.session = nil
	c.ch = nil
	if s != nil {
		_ = s.Close(context.Background())
	}
}
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.drop()
	return nil
}
func pause(ctx context.Context, n time.Duration) error {
	timer := time.NewTimer(n)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Publish uses a caller-generated stable operation ID. If a stored receipt or
// commit reply is lost, it retries with the SAME ID on a fresh session.
func (c *Client) Publish(ctx context.Context, id string, payload []byte) (uint64, error) {
	if len(id) < 8 || len(id) > 256 || len(payload) > 128<<10 {
		return 0, errors.New("resume: invalid operation ID or size")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		if err := c.connect(ctx); err == nil {
			mid, err := journal.SendRequest(ctx, c.ch, journal.Request{Operation: "publish", ID: id, Payload: payload})
			if err == nil {
				_, err = c.ch.AwaitReceipt(ctx, mid, protocol.AckStored)
			}
			var ev journal.Event
			if err == nil {
				ev, err = journal.ReadEvent(ctx, c.ch)
			}
			if err == nil {
				if ev.Type == "committed" && ev.ID == id && ev.Cursor > 0 {
					return ev.Cursor, nil
				}
				if ev.Type == "error" {
					return 0, errors.New("resume: backend refused publish: " + ev.Error)
				}
				return 0, errors.New("resume: unexpected publish response")
			}
		}
		if ctx.Err() != nil {
			break
		}
		c.drop()
		if err := pause(ctx, backoff); err != nil {
			return 0, err
		}
		backoff = min(2*time.Second, 2*backoff)
	}
	return 0, ctx.Err()
}

// ReplayAfter fetches all durable records strictly after cursor, invokes
// apply sequentially, then sends a server-side acknowledged cursor. The
// callback should commit its own idempotent side effect before returning nil.
// Replay restarts at the last acknowledged cursor on transport interruption.
func (c *Client) ReplayAfter(ctx context.Context, cursor uint64, apply func(context.Context, journal.Event) error) (uint64, error) {
	if apply == nil {
		return cursor, errors.New("resume: apply callback required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		if err := c.connect(ctx); err == nil {
			_, err := journal.SendRequest(ctx, c.ch, journal.Request{Operation: "replay", Cursor: cursor})
			if err == nil {
				lastApplied := cursor
				for {
					ev, e := journal.ReadEvent(ctx, c.ch)
					if e != nil {
						err = e
						break
					}
					switch ev.Type {
					case "message":
						if ev.Cursor <= lastApplied {
							continue
						}
						if err = apply(ctx, ev); err != nil {
							return cursor, err
						}
						lastApplied = ev.Cursor
					case "replay_complete":
						if lastApplied == cursor {
							return cursor, nil
						}
						_, err = journal.SendRequest(ctx, c.ch, journal.Request{Operation: "ack", Cursor: lastApplied})
						if err != nil {
							break
						}
						ack, e := journal.ReadEvent(ctx, c.ch)
						if e != nil {
							err = e
							break
						}
						if ack.Type != "acked" || ack.Cursor != lastApplied {
							return cursor, errors.New("resume: invalid cursor acknowledgement")
						}
						return lastApplied, nil
					case "error":
						return cursor, errors.New("resume: server replay error: " + ev.Error)
					default:
						return cursor, errors.New("resume: unexpected replay event")
					}
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		c.drop()
		if err := pause(ctx, backoff); err != nil {
			return cursor, err
		}
		backoff = min(2*time.Second, 2*backoff)
	}
	return cursor, ctx.Err()
}
