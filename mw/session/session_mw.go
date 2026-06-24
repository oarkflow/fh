package session

import (
	"github.com/oarkflow/fh"
)

// New returns a middleware that loads the session on request and saves it
// before the response is sent. The session is stored in Ctx locals under "session".
func New(manager *SessionManager) fh.HandlerFunc {
	if manager == nil {
		panic("session middleware requires a manager")
	}
	return func(ctx fh.Ctx) error {
		s, complete, err := manager.Begin(ctx)
		if err != nil {
			return err
		}
		ctx.Locals("session", s)
		ctx.OnBeforeResponse(complete)
		return ctx.Next()
	}
}

// Get retrieves the session from the context. Panics if the middleware is not
// registered.
func Get(ctx fh.Ctx) *Session {
	s, ok := ctx.Locals("session").(*Session)
	if !ok {
		panic("session middleware is not registered")
	}
	return s
}
