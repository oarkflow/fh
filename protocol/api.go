// Package protocol specifies the public API contract for MIRA-T/1 native
// implementations. It is an architecture interface, not a server runtime.
package protocol

import (
	"context"
	"errors"
	"time"
)

type Delivery uint8

const (
	ReliableOrdered Delivery = iota + 1
	ReliableUnordered
	BestEffort
	LatestState
	Bulk
)

type ReceiptLevel uint8

const (
	AckTransport ReceiptLevel = iota + 1
	AckStored
	AckProcessed
)

type ChannelConfig struct {
	Delivery    Delivery
	Priority    uint8
	MaxBuffered int64
	MaxMessage  int64
	Lease       time.Duration
}

type Message struct {
	ChannelID uint64
	MessageID uint64
	Payload   []byte
	ExpiresAt time.Time
}

type Receipt struct {
	MessageID uint64
	Level     ReceiptLevel
	At        time.Time
}

var ErrBackpressure = errors.New("session send queue full")
var ErrClosed = errors.New("channel or session closed")
var ErrLeaseExpired = errors.New("channel lease expired")

type Channel interface {
	ID() uint64
	Send(context.Context, []byte) (uint64, error)
	Receive(context.Context) (Message, error)
	AwaitReceipt(context.Context, uint64, ReceiptLevel) (Receipt, error)
	Acknowledge(context.Context, uint64, ReceiptLevel) error
	Close(context.Context) error
}

type Session interface {
	ID() uint64
	OpenChannel(context.Context, ChannelConfig) (Channel, error)
	AcceptChannel(context.Context) (Channel, error)
	Close(context.Context) error
}

type Authenticator interface {
	Authenticate(ctx context.Context, identity string, proof []byte) (Principal, error)
}

type Principal struct{ Subject, Tenant, Device string }

type Authorizer interface {
	AuthorizeOpen(ctx context.Context, principal Principal, cfg ChannelConfig) error
	AuthorizeSend(ctx context.Context, principal Principal, channelID uint64, payloadBytes int) error
}

type DurableStore interface {
	Append(ctx context.Context, principal Principal, msg Message) (cursor uint64, err error)
	ReadAfter(ctx context.Context, principal Principal, cursor uint64, max int) ([]Message, error)
	Acknowledge(ctx context.Context, principal Principal, cursor uint64) error
}
