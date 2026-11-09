// Package mira implements an encrypted, custom, bidirectional UDP packet transport.
// It deliberately does not use WebSocket, HTTP, SSE, or QUIC.
//
// WARNING: This is an executable experimental protocol, NOT an independently
// audited replacement for TLS/QUIC. See SECURITY.md before exposing to the Internet.
package mira

import (
	"errors"
	"time"
)

var (
	ErrClosed             = errors.New("mira: closed")
	ErrBackpressure       = errors.New("mira: send queue full")
	ErrUnsupportedReceipt = errors.New("mira: only transport receipt is supported")
	ErrUnknownReceipt     = errors.New("mira: receipt unknown or pruned")
	ErrInvalidSecret      = errors.New("mira: PSK must be at least 32 random bytes")
	ErrTimeout            = errors.New("mira: handshake timed out")
	ErrExpired            = errors.New("mira: channel lease expired")
)

const fragmentSize = 900
const ackRequired = 1

// Options controls hard resource limits per endpoint/session. Zero selects default.
type Options struct {
	MaxSessions               int
	MaxSessionsPerIdentity    int
	MaxInboundBytesPerSecond  int64
	MaxChannels               int
	MaxMessage                int
	MaxQueuedBytes            int64
	MaxReceiveBytes           int64
	MaxInflight               int
	IdleTimeout               time.Duration
	HandshakeTimeout          time.Duration
	Retries                   int
	MaxSessionLifetime        time.Duration
	DisableAutoMTU            bool // Manual-only probing when enabled.
	RequireChannelAuthorizer  bool // Fail closed when no channel policy callback is installed.
	AllowLegacyPSKOnPublicIPs bool // UNSAFE: enable experimental PSK over non-loopback.
}

func (o Options) normalized() Options {
	if o.MaxSessionsPerIdentity < 0 {
		o.MaxSessionsPerIdentity = 0
	}
	if o.MaxInboundBytesPerSecond < 0 {
		o.MaxInboundBytesPerSecond = 0
	}
	if o.MaxSessions <= 0 {
		o.MaxSessions = 1000
	}
	if o.MaxChannels <= 0 {
		o.MaxChannels = 32
	}
	if o.MaxMessage <= 0 {
		o.MaxMessage = 256 << 10
	}
	if o.MaxMessage > (1 << 20) {
		o.MaxMessage = 1 << 20
	}
	if o.MaxQueuedBytes <= 0 {
		o.MaxQueuedBytes = 4 << 20
	}
	if o.MaxReceiveBytes <= 0 {
		o.MaxReceiveBytes = 8 << 20
	}
	// At least one maximum-size reliable message must fit in the credit window.
	if o.MaxReceiveBytes < int64(o.MaxMessage) {
		o.MaxReceiveBytes = int64(o.MaxMessage)
	}
	if o.MaxInflight <= 0 {
		// A secondary memory bound behind the congestion window. 32 capped a
		// 134 ms path at ~0.3 MB/s; 256 allows ~2.7 MB/s there and still holds
		// at most ~380 KB of un-acknowledged ciphertext per session.
		o.MaxInflight = 256
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 90 * time.Second
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = 5 * time.Second
	}
	if o.Retries <= 0 {
		o.Retries = 12
	}
	if o.MaxSessionLifetime <= 0 {
		o.MaxSessionLifetime = time.Hour
	}
	return o
}
