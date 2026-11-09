// Package journal implements an application-level durable messaging profile over
// native MIRA channels. It keeps transport ACKs distinct from fsynced records
// and provides idempotent publish, reconnect replay and explicit cursor ACK.
package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/protocol"
)

// Request is sent on a ReliableOrdered channel. Publish requires a stable,
// unpredictable per-command ID; reconnects may reuse it to avoid duplicates.
// Replay returns committed records after Cursor. Ack commits the processed
// cursor. An untrusted peer can only affect its own authenticated principal.
type Request struct {
	Operation string `json:"operation"`
	ID        string `json:"id,omitempty"`
	Payload   []byte `json:"payload,omitempty"`
	Cursor    uint64 `json:"cursor,omitempty"`
}
type Event struct {
	Type    string `json:"type"`
	Cursor  uint64 `json:"cursor,omitempty"`
	ID      string `json:"id,omitempty"`
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}
type Store interface {
	AppendIdempotent(context.Context, protocol.Principal, string, protocol.Message) (uint64, error)
	ReadRecordsAfter(context.Context, protocol.Principal, uint64, int) ([]durable.Entry, error)
	Acknowledge(context.Context, protocol.Principal, uint64) error
}

// ServeChannel authenticates the principal outside the application payload;
// never accept a client-supplied tenant or identity here. Only stored data
// receives AckStored. Errors are returned as application events, not fake ACKs.
func ServeChannel(ctx context.Context, ch protocol.Channel, principal protocol.Principal, store Store) error {
	if principal.Subject == "" {
		return errors.New("journal: authenticated principal required")
	}
	for {
		msg, err := ch.Receive(ctx)
		if err != nil {
			return err
		}
		if len(msg.Payload) > 200<<10 {
			if e := write(ctx, ch, Event{Type: "error", Error: "request too large"}); e != nil {
				return e
			}
			continue
		}
		var req Request
		if err = json.Unmarshal(msg.Payload, &req); err != nil {
			if e := write(ctx, ch, Event{Type: "error", Error: "invalid JSON"}); e != nil {
				return e
			}
			continue
		}
		switch req.Operation {
		case "publish":
			if len(req.ID) < 8 || len(req.ID) > 256 || len(req.Payload) > 128<<10 {
				if e := write(ctx, ch, Event{Type: "error", Error: "invalid id or payload"}); e != nil {
					return e
				}
				continue
			}
			// Stable idempotency key maps an application retry to the same cursor.
			m := protocol.Message{Payload: req.Payload}
			cursor, e := store.AppendIdempotent(ctx, principal, req.ID, m)
			if e != nil {
				if err := write(ctx, ch, Event{Type: "error", Error: e.Error()}); err != nil {
					return err
				}
				continue
			}
			if e = ch.Acknowledge(ctx, msg.MessageID, protocol.AckStored); e != nil {
				return e
			}
			if e = write(ctx, ch, Event{Type: "committed", ID: req.ID, Cursor: cursor}); e != nil {
				return e
			}
			// Processed here means the journal command was applied, not that a remote
			// consumer has received or executed an arbitrary business operation.
			if e = ch.Acknowledge(ctx, msg.MessageID, protocol.AckProcessed); e != nil {
				return e
			}
		case "replay":
			cursor := req.Cursor
			// Send batches until caught up, keeping memory proportional to 64 records.
			for {
				entries, e := store.ReadRecordsAfter(ctx, principal, cursor, 64)
				if e != nil {
					if err := write(ctx, ch, Event{Type: "error", Error: e.Error()}); err != nil {
						return err
					}
					break
				}
				for _, item := range entries {
					if e = write(ctx, ch, Event{Type: "message", Cursor: item.Cursor, Payload: item.Message.Payload}); e != nil {
						return e
					}
					cursor = item.Cursor
				}
				if len(entries) < 64 {
					if e = write(ctx, ch, Event{Type: "replay_complete", Cursor: cursor}); e != nil {
						return e
					}
					break
				}
			}
		case "ack":
			if e := store.Acknowledge(ctx, principal, req.Cursor); e != nil {
				if err := write(ctx, ch, Event{Type: "error", Error: e.Error()}); err != nil {
					return err
				}
				continue
			}
			if e := ch.Acknowledge(ctx, msg.MessageID, protocol.AckStored); e != nil {
				return e
			}
			if e := write(ctx, ch, Event{Type: "acked", Cursor: req.Cursor}); e != nil {
				return e
			}
		default:
			if e := write(ctx, ch, Event{Type: "error", Error: fmt.Sprintf("unknown operation %q", req.Operation)}); e != nil {
				return e
			}
		}
	}
}
func write(ctx context.Context, ch protocol.Channel, e Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = ch.Send(ctx, b)
	return err
}
func SendRequest(ctx context.Context, ch protocol.Channel, req Request) (uint64, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	return ch.Send(ctx, b)
}
func ReadEvent(ctx context.Context, ch protocol.Channel) (Event, error) {
	m, err := ch.Receive(ctx)
	if err != nil {
		return Event{}, err
	}
	var e Event
	if err = json.Unmarshal(m.Payload, &e); err != nil {
		return Event{}, err
	}
	return e, nil
}
