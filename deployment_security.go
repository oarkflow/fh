package fh

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
)

// DeploymentMode describes how public traffic reaches fh.
type DeploymentMode string

const (
	DeploymentDirectOrigin DeploymentMode = "direct-origin"
	DeploymentTrustedEdge  DeploymentMode = "trusted-edge"
)

// DeploymentSecurityConfig enables an explicit, conservative startup gate.
// Shared* fields are operator assertions: fh cannot inspect whether an
// arbitrary middleware store is actually shared by all replicas.
type DeploymentSecurityConfig struct {
	Mode              DeploymentMode
	ExpectedReplicas  int
	TrustedProxyCIDRs []string
	UsesReplay        bool
	UsesSessions      bool
	SharedQueue       bool
	SharedIdempotency bool
	SharedReplay      bool
	SharedSessions    bool
}

type secureTLSListener struct{ net.Listener }

func (a *App) validateDeployment(_ net.Listener, tlsEnabled bool) error {
	g := a.cfg.DeploymentSecurityGate
	if g == nil {
		return nil
	}
	var findings []string
	add := func(ok bool, issue string) {
		if !ok {
			findings = append(findings, issue)
		}
	}
	c := a.cfg
	var edgeNets []*net.IPNet
	add(c.SecureByDefault, "enable SecureByDefault")
	add(len(c.AllowedHosts) > 0, "configure AllowedHosts")
	add(c.ReadHeaderTimeout > 0 && c.RequestBodyTimeout > 0 && c.WriteTimeout > 0 && c.IdleTimeout > 0 && c.TLSHandshakeTimeout > 0, "configure positive header, body, write, idle, and TLS handshake timeouts")
	add(c.MaxConnections > 0 && c.MaxInFlightRequests > 0, "bound connections and in-flight requests")
	add(c.MaxRequestBodySize > 0 && c.MaxHeaderListSize > 0 && c.MaxHeaderCount > 0 && c.MaxRequestLineSize > 0, "bound request body, headers, and request line")
	add(c.MaxBufferedRequestBytes > 0, "configure MaxBufferedRequestBytes")
	add(c.DisableHTTP2 || c.MaxConcurrentStreams > 0, "bound HTTP/2 concurrent streams")
	add(c.DisableHTTP2 || c.HTTP2IdleTimeout > 0, "configure HTTP/2 idle timeout")
	add(c.DisableH2C || c.DisableHTTP2, "disable h2c")
	add(!c.DisablePanicRecovery, "enable panic recovery")
	switch g.Mode {
	case DeploymentDirectOrigin:
		add(tlsEnabled, "direct origin requires an fh TLS serving entrypoint")
	case DeploymentTrustedEdge:
		add(len(g.TrustedProxyCIDRs) > 0, "declare trusted proxy CIDRs")
		for _, raw := range g.TrustedProxyCIDRs {
			_, network, err := net.ParseCIDR(raw)
			if err != nil || network == nil || network.String() == "0.0.0.0/0" || network.String() == "::/0" {
				findings = append(findings, fmt.Sprintf("invalid or unrestricted trusted proxy CIDR %q", raw))
			} else {
				edgeNets = append(edgeNets, network)
			}
		}
	default:
		findings = append(findings, "choose direct-origin or trusted-edge deployment mode")
	}
	add(g.ExpectedReplicas >= 1, "set ExpectedReplicas to at least one")
	if g.ExpectedReplicas > 1 {
		defaultReliability := c.Reliability.Enabled && !c.Reliability.JournalEnabled && !c.Reliability.IdempotencyEnabled && !c.Reliability.QueueEnabled
		if c.Reliability.Enabled && (c.Reliability.QueueEnabled || defaultReliability) {
			add(g.SharedQueue, "confirm shared queue storage across replicas")
			add(c.Reliability.QueueStorage != nil, "replace the process-local file queue with shared QueueStorage")
			capable, ok := c.Reliability.QueueStorage.(MultiReplicaQueueStorage)
			add(ok && capable.SupportsMultiReplica(), "shared queue must support atomic claims, fenced leases, and safe recovery")
		}
		if c.Reliability.Enabled && (c.Reliability.IdempotencyEnabled || defaultReliability) {
			add(g.SharedIdempotency, "confirm shared idempotency storage across replicas")
			add(c.Reliability.IdempotencyRepository != nil, "replace the process-local file idempotency repository with a shared implementation")
		}
		if g.UsesReplay {
			add(g.SharedReplay, "confirm shared replay storage across replicas")
		}
		if g.UsesSessions {
			add(g.SharedSessions, "confirm shared session storage across replicas")
		}
	}
	if len(findings) == 0 {
		a.trustedEdgeNets = edgeNets
		return nil
	}
	return errors.New("fh: deployment security gate failed: " + strings.Join(findings, "; "))
}

func (a *App) allowedEdgePeer(conn net.Conn) bool {
	if a.cfg.DeploymentSecurityGate == nil || a.cfg.DeploymentSecurityGate.Mode != DeploymentTrustedEdge {
		return true
	}
	if tc, ok := conn.(*tls.Conn); ok {
		conn = tc.NetConn()
	}
	addr := conn.RemoteAddr()
	if raw, ok := conn.(interface{ RawRemoteAddr() net.Addr }); ok {
		addr = raw.RawRemoteAddr()
	}
	ip := net.ParseIP(peerIPKey(addr))
	if ip == nil {
		return false
	}
	for _, network := range a.trustedEdgeNets {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
