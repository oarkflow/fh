package fh

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

// MIRAServer, MIRASession, MIRAOptions and MIRAChannel expose the native MIRA
// UDP transport through fh's root package. MIRA retains its own wire protocol
// and does not share HTTP listeners or browser securetransport framing.
type MIRAServer = mira.SecureServer
type MIRASession = mira.Session
type MIRAOptions = mira.Options
type MIRAChannel = protocol.Channel
type MIRAChannelConfig = protocol.ChannelConfig

// MIRAClient is fh's native MIRA transport client. It intentionally exposes
// sessions and channels instead of pretending MIRA is an HTTP RoundTripper.
type MIRAClient struct {
	tlsConfig *tls.Config
	opts      MIRAOptions
}

// NewMIRAClient selects the native MIRA transport with an mTLS 1.3 identity.
func NewMIRAClient(cfg *tls.Config, opts MIRAOptions) (*MIRAClient, error) {
	if cfg == nil {
		return nil, errors.New("fh: MIRA requires a TLS client config")
	}
	return &MIRAClient{tlsConfig: cfg.Clone(), opts: opts}, nil
}

// Connect establishes a new authenticated MIRA session to the server's TLS
// bootstrap and UDP endpoints.
func (c *MIRAClient) Connect(ctx context.Context, tcpAddr, udpAddr string) (*MIRASession, error) {
	if c == nil || c.tlsConfig == nil {
		return nil, errors.New("fh: unconfigured MIRA client")
	}
	return mira.DialTLS(ctx, tcpAddr, udpAddr, c.tlsConfig, c.opts)
}

// MIRAChannelHandler handles one accepted channel. The callback receives the
// authenticated transport identity and owns the channel until it returns.
type MIRAChannelHandler func(context.Context, string, MIRAChannel) error

const defaultMIRAHandlerLimit = 256

// ServeMIRA accepts sessions on a server created by App.ListenMIRATLS and
// dispatches accepted channels to handler. It blocks until ctx cancellation or
// the server closes. Each channel is handled concurrently.
func (a *App) ServeMIRA(ctx context.Context, server *MIRAServer, handler MIRAChannelHandler) error {
	return a.ServeMIRAWithLimit(ctx, server, defaultMIRAHandlerLimit, handler)
}

// ServeMIRAWithLimit is ServeMIRA with an explicit upper bound on concurrent
// application channel handlers. Channels arriving over the limit are closed
// immediately, keeping application work and goroutine use bounded.
func (a *App) ServeMIRAWithLimit(ctx context.Context, server *MIRAServer, maxHandlers int, handler MIRAChannelHandler) error {
	if a == nil || server == nil || handler == nil || ctx == nil {
		return errors.New("fh: MIRA serve requires app, server, handler and context")
	}
	if maxHandlers <= 0 {
		return errors.New("fh: MIRA handler limit must be positive")
	}
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-a.drainingCh:
			cancel()
		case <-serveCtx.Done():
		}
	}()
	defer server.Close()
	handlers := make(chan struct{}, maxHandlers)
	for {
		session, err := server.Accept(serveCtx)
		if err != nil {
			if serveCtx.Err() != nil {
				return serveCtx.Err()
			}
			return err
		}
		go func(session *MIRASession) {
			defer session.Close(context.Background())
			for {
				channel, acceptErr := session.AcceptChannel(serveCtx)
				if acceptErr != nil {
					return
				}
				select {
				case handlers <- struct{}{}:
				default:
					a.logger.Warn("MIRA channel rejected: handler capacity reached", "identity", session.Identity(), "channel_id", channel.ID())
					_ = channel.Close(context.Background())
					continue
				}
				go func(channel MIRAChannel) {
					defer func() { <-handlers }()
					if handleErr := handler(serveCtx, session.Identity(), channel); handleErr != nil {
						a.logger.Error("MIRA channel handler failed", "identity", session.Identity(), "channel_id", channel.ID(), "error", handleErr)
						_ = channel.Close(context.Background())
					}
				}(channel)
			}
		}(session)
	}
}

// ListenMIRATLS starts an authenticated native MIRA transport endpoint owned
// by the FH application lifecycle. It is a first-class transport endpoint;
// MIRA packets and channels are independent of HTTP request parsing. Its UDP
// and TLS bootstrap sockets are closed by App shutdown.
// The TLS config must require verified client certificates; see mira.ListenTLS.
// Channel opens fail closed until SetChannelAuthorizer installs a policy.
func (a *App) ListenMIRATLS(udpAddr, tcpAddr string, cfg *tls.Config, opts MIRAOptions) (*MIRAServer, error) {
	if a == nil {
		return nil, errors.New("fh: nil app")
	}
	if a.closed.Load() {
		return nil, ErrAppAlreadyStarted
	}
	server, err := mira.ListenTLS(udpAddr, tcpAddr, cfg, opts)
	if err != nil {
		return nil, err
	}
	a.miraMu.Lock()
	if a.closed.Load() {
		a.miraMu.Unlock()
		_ = server.Close()
		return nil, ErrAppAlreadyStarted
	}
	a.miraServers = append(a.miraServers, server)
	a.miraMu.Unlock()
	return server, nil
}

// DialMIRATLS opens a native encrypted MIRA session using mutual TLS 1.3.
// It is a native UDP client and is independent of fh.Client's HTTP API.
func DialMIRATLS(ctx context.Context, tcpAddr, udpAddr string, cfg *tls.Config, opts MIRAOptions) (*MIRASession, error) {
	return mira.DialTLS(ctx, tcpAddr, udpAddr, cfg, opts)
}
