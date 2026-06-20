package session

import (
	fh "github.com/orgware/fasthttp"
)

// New returns a middleware that loads the session on request and saves it
// before the response is sent. The session is stored in Ctx locals under "session".
func New(manager *fh.SessionManager) fh.HandlerFunc {
	return func(ctx *fh.Ctx) error {
		s := manager.Get(ctx)
		ctx.Locals("session", s)
		defer func() {
			manager.Save(ctx, s)
		}()
		return ctx.Next()
	}
}

// Get retrieves the session from the context. Panics if the middleware is not
// registered.
func Get(ctx *fh.Ctx) *fh.Session {
	s, ok := ctx.Locals("session").(*fh.Session)
	if !ok {
		panic("session middleware is not registered")
	}
	return s
}
