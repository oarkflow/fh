// Package fleet is a reusable device-fleet pattern on top of MIRA channels.
//
// Each device opens two channels on one session:
//
//   - a LatestState channel carrying telemetry snapshots (stale ones may be
//     dropped; only the newest matters), and
//   - a ReliableOrdered channel on which the server pushes commands and the
//     device confirms each with AckProcessed after applying it.
//
// Hub is the server side (registry, channel policy, dispatch); Agent is the
// device side (reconnecting runtime). Payloads are opaque bytes; applications
// choose the encoding.
package fleet

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/oarkflow/fh/protocol"
)

var (
	// ErrOffline means the device has no registered command channel.
	ErrOffline = errors.New("fleet: device offline")
	// ErrChannel means an accepted channel exposed no configuration.
	ErrChannel = errors.New("fleet: channel does not expose its configuration")
)

// Limits for the two channel kinds.
const (
	// MaxTelemetry keeps a snapshot within one datagram, as LatestState requires.
	MaxTelemetry = 900
	// MaxCommand bounds one command or result message.
	MaxCommand = 16 << 10
)

type configurer interface{ Config() protocol.ChannelConfig }

// Device is a point-in-time view of one connected device.
type Device struct {
	ID        string
	Telemetry []byte
	UpdatedAt time.Time
}

// Hub tracks connected devices. Install Authorize with
// SecureServer.SetChannelAuthorizer and run Handle for each accepted channel
// (for example through fh.App.ServeMIRA).
type Hub struct {
	// Allow decides which authenticated certificate identities are devices.
	// Required: a nil Allow refuses everyone.
	Allow func(identity string) bool
	// OnResult, if set, receives each result message a device sends back on
	// its command channel.
	OnResult func(device string, payload []byte)

	mu   sync.RWMutex
	cmd  map[string]protocol.Channel
	last map[string]Device
}

// NewHub returns an empty Hub with the given identity policy.
func NewHub(allow func(identity string) bool) *Hub {
	return &Hub{Allow: allow, cmd: map[string]protocol.Channel{}, last: map[string]Device{}}
}

// Authorize is the mira.ChannelAuthorizer for the fleet profile: allowed
// identities may open only the two documented channel kinds, within limits.
func (h *Hub) Authorize(identity string, cfg protocol.ChannelConfig) bool {
	if h.Allow == nil || !h.Allow(identity) {
		return false
	}
	switch cfg.Delivery {
	case protocol.LatestState:
		return cfg.MaxMessage <= MaxTelemetry
	case protocol.ReliableOrdered:
		return cfg.MaxMessage <= MaxCommand
	}
	return false
}

// Handle serves one accepted channel until it closes. The device identity is
// always the authenticated one passed in, never anything inside a payload.
func (h *Hub) Handle(ctx context.Context, identity string, ch protocol.Channel) error {
	defer ch.Close(context.Background())
	cc, ok := ch.(configurer)
	if !ok {
		return ErrChannel
	}
	switch cc.Config().Delivery {
	case protocol.LatestState:
		for {
			msg, err := ch.Receive(ctx)
			if err != nil {
				return nil
			}
			h.mu.Lock()
			h.last[identity] = Device{ID: identity, Telemetry: msg.Payload, UpdatedAt: time.Now()}
			h.mu.Unlock()
		}
	case protocol.ReliableOrdered:
		h.mu.Lock()
		h.cmd[identity] = ch
		h.mu.Unlock()
		defer func() {
			h.mu.Lock()
			if h.cmd[identity] == ch { // a reconnect may have replaced this channel
				delete(h.cmd, identity)
				delete(h.last, identity)
			}
			h.mu.Unlock()
		}()
		for {
			msg, err := ch.Receive(ctx)
			if err != nil {
				return nil
			}
			if h.OnResult != nil {
				h.OnResult(identity, msg.Payload)
			}
		}
	}
	return nil
}

// Dispatch sends a command and blocks until the device reports it processed,
// ctx ends, or the channel fails. Success means the device applied the
// command, not merely that the packet arrived.
func (h *Hub) Dispatch(ctx context.Context, device string, command []byte) (uint64, error) {
	h.mu.RLock()
	ch := h.cmd[device]
	h.mu.RUnlock()
	if ch == nil {
		return 0, ErrOffline
	}
	id, err := ch.Send(ctx, command)
	if err != nil {
		return 0, err
	}
	if _, err := ch.AwaitReceipt(ctx, id, protocol.AckProcessed); err != nil {
		return id, err
	}
	return id, nil
}

// Devices lists connected devices, sorted by ID, with their newest telemetry.
func (h *Hub) Devices() []Device {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]Device, 0, len(h.cmd))
	for id := range h.cmd {
		d := h.last[id]
		d.ID = id
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
