package authorizer

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/tcpguard"
)

// New adapts a Guard to Fiber v3. Fiber remains an optional dependency
// and is not imported by the core tcpguard module.
func New(guard *tcpguard.Guard) fh.Handler {
	return func(c *fh.Ctx) error {
		req, err := http.NewRequestWithContext(c.Context(), c.Method(), c.OriginalURL(), bytes.NewReader(c.BodyRaw()))
		if err != nil {
			return err
		}
		req.Host = c.Hostname()
		req.RemoteAddr = c.IP()
		for key, values := range c.GetReqHeaders() {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}

		result, err := guard.EvaluateHTTPRequest(req)
		if err != nil {
			return err
		}
		c.Set("X-TCPGuard-Risk", fmt.Sprintf("%.0f", result.Decision.Risk.Score))
		if !result.Enforced {
			return c.Next()
		}
		for key, value := range result.Response.Headers {
			c.Set(key, value)
		}
		return c.Status(result.Response.Status).JSON(result.Response.Body)
	}
}
