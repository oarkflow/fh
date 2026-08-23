package proxy

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

// ConnectConfig controls a forward HTTP CONNECT tunnel.
type ConnectConfig struct {
	// AllowTarget authorizes the requested host:port. A nil function rejects all
	// targets so applications must opt in to outbound tunneling explicitly.
	AllowTarget func(string) bool
	Timeout     time.Duration
}

// Connect returns a handler for HTTP/1.1 CONNECT requests. It establishes a
// raw TCP tunnel only after AllowTarget approves the authority.
func Connect(cfg ConnectConfig) fh.HandlerFunc {
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	return func(c fh.Ctx) error {
		if !strings.EqualFold(c.Method(), "CONNECT") {
			return c.Status(fh.StatusMethodNotAllowed).SendString("CONNECT required")
		}
		target := c.OriginalURL()
		if target == "" || !validAuthority(target) || cfg.AllowTarget == nil || !cfg.AllowTarget(target) {
			return c.Status(fh.StatusForbidden).SendString("CONNECT target denied")
		}
		upstream, err := dialer.DialContext(c.Context(), "tcp", target)
		if err != nil {
			return fh.DependencyFailure(fmt.Sprintf("connect: %v", err)).WithCause(err)
		}
		return c.Hijack(func(client *fh.ResponseConn) error {
			client.WriteHeader(fh.StatusOK)
			defer upstream.Close()
			defer client.Close()
			errors := make(chan error, 2)
			go func() {
				_, copyErr := io.Copy(upstream, client)
				errors <- copyErr
			}()
			go func() {
				_, copyErr := io.Copy(client, upstream)
				errors <- copyErr
			}()
			return <-errors
		})
	}
}

func validAuthority(target string) bool {
	host, port, err := net.SplitHostPort(target)
	if err != nil || host == "" || port == "" || strings.ContainsAny(target, "\x00\r\n") {
		return false
	}
	return true
}
