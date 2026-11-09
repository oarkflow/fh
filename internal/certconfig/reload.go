package certconfig

// Reloadable TLS server credentials. Existing TLS sessions remain valid;
// every NEW handshake uses a coherent immutable snapshot of certificate,
// private key and trusted client roots. A failed reload preserves the prior
// snapshot. This does not revoke already-open MIRA UDP sessions: use the
// Server.RevokeIdentity API to disconnect affected identities immediately.

import (
	"crypto/tls"
	"sync/atomic"
)

const miraALPN = "mira-t/1-udp-bootstrap"

type ServerReloader struct {
	cert, key, ca string
	current       atomic.Pointer[tls.Config]
}

func NewServerReloader(cert, key, ca string) (*ServerReloader, error) {
	r := &ServerReloader{cert: cert, key: key, ca: ca}
	if err := r.Reload(); err != nil {
		return nil, err
	}
	return r, nil
}
func (r *ServerReloader) Reload() error {
	cfg, err := Server(r.cert, r.key, r.ca)
	if err != nil {
		return err
	}
	cfg.MinVersion = tls.VersionTLS13
	cfg.MaxVersion = tls.VersionTLS13
	cfg.NextProtos = []string{miraALPN}
	r.current.Store(cfg)
	return nil
}
func (r *ServerReloader) Config() *tls.Config {
	initial := r.current.Load().Clone()
	initial.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return r.current.Load(), nil
	}
	return initial
}
