package main

import (
	"context"
	"crypto/ed25519"
	"flag"
	"github.com/oarkflow/fh/delegation"
	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/gateway"
	"github.com/oarkflow/fh/internal/access"
	"github.com/oarkflow/fh/internal/atrest"
	"github.com/oarkflow/fh/internal/certconfig"
	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	udp := flag.String("udp", ":7447", "MIRA UDP data address")
	tcp := flag.String("tcp", ":7448", "mTLS bootstrap TCP address")
	cert := flag.String("cert", "", "TLS server certificate PEM")
	key := flag.String("key", "", "TLS server private key PEM")
	ca := flag.String("ca", "", "trusted client CA PEM")
	walPath := flag.String("wal", "", "optional encrypted WAL; enables journal profile")
	keyFile := flag.String("wal-keys", "", "owner-only WAL keyring JSON, required with -wal")
	allow := flag.String("allow", "", "comma-separated authorized certificate identities")
	allowFile := flag.String("allow-file", "", "reloadable newline-separated certificate allowlist; SIGHUP reloads and revokes")
	gatewayIdentity := flag.String("gateway-identity", "", "explicitly authorized mTLS service identity for browser delegation")
	gatewayPublicKey := flag.String("gateway-delegation-key", "", "legacy trusted Ed25519 public key (PEM)")
	gatewayKeyring := flag.String("gateway-delegation-ring", "", "JSON public keyring for SIGHUP reloadable gateway trust")
	gatewayAudience := flag.String("gateway-audience", "mira-upstream", "expected signed assertion audience")
	maxSessions := flag.Int("max-sessions", 16, "maximum concurrent sessions; raise only after sizing per-session memory against the process limit")
	maxServerReceive := flag.Int64("max-server-receive-bytes", 128<<20, "aggregate buffered inbound application payload across sessions (default 128 MiB)")
	maxPerIdentity := flag.Int("max-sessions-per-identity", 4, "maximum concurrent sessions per certificate identity")
	maxTLSPerIP := flag.Int("max-tls-handshakes-per-ip", 32, "maximum concurrent TLS bootstrap handshakes from one source IP (1..1024)")
	maxHandshakeRate := flag.Int("max-handshake-attempts-per-second", 2000, "endpoint-wide UDP HELLO and TLS bootstrap rate limit (1..1000000)")
	maxChannels := flag.Int("max-channels", 16, "maximum channels per session")
	maxInflight := flag.Int("max-inflight", 0, "maximum un-acknowledged packets per session (0 = library default 256); lower for memory-constrained deployments or raise for bulk senders")
	udpReadBuffer := flag.Int("udp-read-buffer", 0, "kernel UDP receive queue bytes (0 = operating-system default; maximum 64 MiB)")
	udpWriteBuffer := flag.Int("udp-write-buffer", 0, "kernel UDP send queue bytes (0 = operating-system default; maximum 64 MiB)")
	idleTimeout := flag.Duration("idle-timeout", 90*time.Second, "close sessions idle for this long")
	maxLifetime := flag.Duration("max-session-lifetime", 30*time.Minute, "force re-authentication after this long")
	metricsAddr := flag.String("metrics", "127.0.0.1:9109", "loopback-only Prometheus listener; set empty to disable")
	flag.Parse()
	if *maxSessions <= 0 || *maxPerIdentity <= 0 || *maxChannels <= 0 || *maxInflight < 0 || *maxServerReceive < 8<<20 || *maxServerReceive > 64<<30 {
		log.Fatal("-max-sessions, -max-sessions-per-identity, and -max-channels must be positive; -max-inflight cannot be negative; -max-server-receive-bytes must be 8 MiB..64 GiB")
	}
	if *maxTLSPerIP <= 0 || *maxTLSPerIP > 1024 {
		log.Fatal("-max-tls-handshakes-per-ip must be 1..1024")
	}
	if *maxHandshakeRate <= 0 || *maxHandshakeRate > 1_000_000 {
		log.Fatal("-max-handshake-attempts-per-second must be 1..1000000")
	}
	if *udpReadBuffer < 0 || *udpReadBuffer > 64<<20 || *udpWriteBuffer < 0 || *udpWriteBuffer > 64<<20 {
		log.Fatal("-udp-read-buffer and -udp-write-buffer must be 0..67108864 bytes")
	}
	if *idleTimeout <= 0 || *maxLifetime <= 0 {
		log.Fatal("-idle-timeout and -max-session-lifetime must be positive")
	}
	var delegated *delegation.Verifier
	if *gatewayPublicKey != "" && *gatewayKeyring != "" {
		log.Fatal("choose gateway-delegation-key or gateway-delegation-ring")
	}
	if (*gatewayIdentity == "") == (*gatewayPublicKey != "" || *gatewayKeyring != "") {
		log.Fatal("gateway identity and delegation public key must be configured together")
	}
	if *gatewayKeyring != "" {
		delegated = &delegation.Verifier{Audience: *gatewayAudience}
		if err := delegated.ReloadKeys(*gatewayKeyring); err != nil {
			log.Fatal(err)
		}
	}
	if *gatewayPublicKey != "" {
		raw, e := os.ReadFile(*gatewayPublicKey)
		if e != nil {
			log.Fatal(e)
		}
		pub, e := gateway.ParseJWTPublicKeyPEM(raw, "EdDSA")
		if e != nil {
			log.Fatal(e)
		}
		ed, ok := pub.(ed25519.PublicKey)
		if !ok {
			log.Fatal("gateway signing key must be Ed25519")
		}
		delegated = &delegation.Verifier{Audience: *gatewayAudience, PublicKey: ed}
	}
	initialAllowed, err := access.Load(*allowFile, *allow)
	if err != nil {
		log.Fatalf("certificate ACL: %v", err)
	}
	var allowed atomic.Value
	allowed.Store(initialAllowed)
	var lastStorageLog atomic.Int64
	reloader, err := certconfig.NewServerReloader(*cert, *key, *ca)
	if err != nil {
		log.Fatal(err)
	}
	server, err := mira.ListenTLSWithIdentityAuthorizer(*udp, *tcp, reloader.Config(), func(identity string) bool {
		_, ok := allowed.Load().(map[string]struct{})[identity]
		return ok
	}, mira.Options{MaxSessions: *maxSessions, MaxServerReceiveBytes: *maxServerReceive, MaxSessionsPerIdentity: *maxPerIdentity, MaxTLSHandshakesPerIP: *maxTLSPerIP, MaxHandshakeAttemptsPerSecond: *maxHandshakeRate, MaxChannels: *maxChannels, MaxInflight: *maxInflight, UDPReadBufferBytes: *udpReadBuffer, UDPWriteBufferBytes: *udpWriteBuffer, IdleTimeout: *idleTimeout, MaxSessionLifetime: *maxLifetime})
	if err != nil {
		log.Fatal(err)
	}
	var base *durable.Store
	var storage journal.Store
	var handlerWG sync.WaitGroup
	defer func() {
		if err := server.Close(); err != nil {
			log.Printf("MIRA shutdown: %v", err)
		}
		handlersDone := make(chan struct{})
		go func() {
			handlerWG.Wait()
			close(handlersDone)
		}()
		select {
		case <-handlersDone:
		case <-time.After(10 * time.Second):
			log.Printf("shutdown timed out waiting for application handlers; leaving WAL close to process exit")
			return
		}
		if closer, ok := storage.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				log.Printf("encrypted WAL keyring close: %v", err)
			}
		}
		if base != nil {
			if err := base.Close(); err != nil {
				log.Printf("WAL close: %v", err)
			}
		}
	}()
	if *walPath != "" {
		if *keyFile == "" {
			log.Fatal("-wal requires -wal-keys")
		}
		base, err = durable.Open(*walPath, 512<<20)
		if err != nil {
			log.Fatal(err)
		}
		active, keys, err := atrest.ReadKeyring(*keyFile)
		if err != nil {
			log.Fatal(err)
		}
		storage, err = durable.NewEncryptedStore(base, active, keys)
		for _, key := range keys {
			clear(key)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	server.SetChannelAuthorizer(func(id string, c protocol.ChannelConfig) bool {
		_, ok := allowed.Load().(map[string]struct{})[id]
		return ok && c.MaxMessage <= 1<<20
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *metricsAddr != "" {
		host, _, e := net.SplitHostPort(*metricsAddr)
		if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			log.Fatal("metrics must bind an explicit loopback IP or be disabled")
		}
		metricsListener, e := net.Listen("tcp", *metricsAddr)
		if e != nil {
			log.Fatalf("metrics listener: %v", e)
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			if err := server.WritePrometheus(w); err != nil {
				log.Printf("metrics write: %v", err)
			}
		})
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		metricsServer := &http.Server{Handler: mux, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 20 * time.Second}
		go func() {
			if e := metricsServer.Serve(metricsListener); e != nil && e != http.ErrServerClosed {
				log.Printf("metrics listener: %v", e)
				stop()
			}
		}()
		defer metricsServer.Close()
	}
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	go func() {
		for {
			select {
			case <-reload:
				if *gatewayKeyring != "" {
					if err := delegated.ReloadKeys(*gatewayKeyring); err != nil {
						log.Printf("gateway delegation trust reload refused: %v", err)
					} else {
						log.Printf("gateway delegation trust reloaded")
					}
				}
				if *allowFile != "" {
					updated, e := access.Load(*allowFile, "")
					if e != nil {
						log.Printf("ACL reload refused: %v", e)
					} else {
						old := allowed.Load().(map[string]struct{})
						removed := make(map[string]struct{})
						for id := range old {
							if _, ok := updated[id]; !ok {
								removed[id] = struct{}{}
							}
						}
						if err := server.ReplaceRevokedIdentities(removed, updated); err != nil {
							log.Printf("ACL reload refused: %v", err)
							continue
						}
						allowed.Store(updated)
						log.Printf("ACL reloaded with %d authorized identities", len(updated))
					}
				}
				if err := reloader.Reload(); err != nil {
					log.Printf("TLS certificate/CA reload refused: %v", err)
				} else {
					log.Printf("TLS certificate and client trust roots reloaded for new sessions")
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	log.Printf("MIRA TLS bootstrap %s; encrypted UDP %s", server.TLSAddr(), server.Addr())
	for {
		s, err := server.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("accept: %v", err)
			continue
		}
		if _, ok := allowed.Load().(map[string]struct{})[s.Identity()]; !ok {
			s.Close(context.Background())
			continue
		}
		handlerWG.Add(1)
		go func(ss *mira.Session) {
			defer handlerWG.Done()
			defer ss.Close(context.Background())
			for {
				ch, err := ss.AcceptChannel(ctx)
				if err != nil {
					return
				}
				handlerWG.Add(1)
				go func(c protocol.Channel) {
					defer handlerWG.Done()
					defer c.Close(context.Background())
					subject := ss.Identity()
					if *gatewayIdentity != "" && subject == *gatewayIdentity {
						authCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						bound, e := delegated.Accept(authCtx, c, ss.ID())
						cancel()
						if e != nil {
							return
						}
						subject = bound
					}
					if storage != nil {
						_ = journal.ServeChannelWithErrorHandler(ctx, c, protocol.Principal{Subject: subject, Device: ss.Identity()}, storage, func(err error) {
							now := time.Now().Unix()
							previous := lastStorageLog.Load()
							if now-previous >= 10 && lastStorageLog.CompareAndSwap(previous, now) {
								log.Printf("journal storage operation failed: %v", err)
							}
						})
						return
					}
					for {
						msg, e := c.Receive(ctx)
						if e != nil {
							return
						}
						if _, e = c.Send(ctx, msg.Payload); e != nil {
							return
						}
					}
				}(ch)
			}
		}(s)
	}
}
