package fleet

import (
	"context"
	"crypto/tls"
	"errors"
	"time"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

// Agent is the device-side runtime. It connects, opens the telemetry and
// command channels, and reconnects with exponential backoff until ctx ends.
type Agent struct {
	TLS     *tls.Config
	TCPAddr string // mTLS bootstrap
	UDPAddr string // encrypted MIRA UDP
	Options mira.Options

	// Telemetry returns the current snapshot (<= MaxTelemetry bytes). Called
	// every Interval. Required.
	Telemetry func() []byte
	Interval  time.Duration // default 1s
	// OnCommand applies a command. Returning nil produces AckProcessed and the
	// result "ok"; an error produces a generic "error" result and no
	// AckProcessed is sent. Required.
	OnCommand func(ctx context.Context, command []byte) error
	// OnCommandError observes local command failures. The error is never sent to
	// the remote hub; use this callback for local logging or diagnostics.
	OnCommandError func(error)
	// OnDisconnect, if set, observes each failed or ended session.
	OnDisconnect func(error)
}

// Run blocks until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if a.TLS == nil || a.Telemetry == nil || a.OnCommand == nil {
		return errors.New("fleet: Agent needs TLS, Telemetry and OnCommand")
	}
	if a.Interval <= 0 {
		a.Interval = time.Second
	}
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		started := time.Now()
		err := a.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if a.OnDisconnect != nil {
			a.OnDisconnect(err)
		}
		if time.Since(started) > 10*time.Second {
			backoff = 100 * time.Millisecond // it was healthy; start over
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(10*time.Second, backoff*2)
	}
	return ctx.Err()
}

func (a *Agent) session(ctx context.Context) error {
	s, err := mira.DialTLS(ctx, a.TCPAddr, a.UDPAddr, a.TLS, a.Options)
	if err != nil {
		return err
	}
	defer s.Close(context.Background())
	cmd, err := s.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered, MaxMessage: MaxCommand})
	if err != nil {
		return err
	}
	tel, err := s.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.LatestState, MaxMessage: MaxTelemetry})
	if err != nil {
		return err
	}

	in := make(chan protocol.Message)
	failed := make(chan error, 1)
	rctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		for {
			m, err := cmd.Receive(rctx)
			if err != nil {
				failed <- err
				return
			}
			select {
			case in <- m:
			case <-rctx.Done():
				return
			}
		}
	}()

	tick := time.NewTicker(a.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-failed:
			return err
		case m := <-in:
			result := "ok"
			if err := a.OnCommand(ctx, m.Payload); err != nil {
				result = "error"
				if a.OnCommandError != nil {
					a.OnCommandError(err)
				}
			} else if err := cmd.Acknowledge(ctx, m.MessageID, protocol.AckProcessed); err != nil {
				return err
			}
			if _, err := cmd.Send(ctx, []byte(result)); err != nil {
				return err
			}
		case <-tick.C:
			// A dropped snapshot is fine: the next one supersedes it.
			_, _ = tel.Send(ctx, a.Telemetry())
		}
	}
}
