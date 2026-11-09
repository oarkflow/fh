package journal

import (
	"context"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

// Acceptor is satisfied by *mira.SecureServer and *mira.Server.
type Acceptor interface {
	Accept(context.Context) (*mira.Session, error)
}

// ServeSessions runs the durable journal profile for every accepted session
// until ctx ends or the server closes. Each channel is served on its own
// goroutine as the session's authenticated principal: the journal partition is
// the certificate identity, never a value from the payload. Install a channel
// authorizer on the server first so only intended identities reach this point.
func ServeSessions(ctx context.Context, srv Acceptor, store Store) error {
	for {
		s, err := srv.Accept(ctx)
		if err != nil {
			return err
		}
		go func(s *mira.Session) {
			defer s.Close(context.Background())
			principal := protocol.Principal{Subject: s.Identity(), Device: s.Identity()}
			for {
				ch, err := s.AcceptChannel(ctx)
				if err != nil {
					return
				}
				go func() {
					defer ch.Close(context.Background())
					_ = ServeChannel(ctx, ch, principal, store)
				}()
			}
		}(s)
	}
}
