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
	maxSessions := flag.Int("max-sessions", 1000, "maximum concurrent sessions")
	maxPerIdentity := flag.Int("max-sessions-per-identity", 4, "maximum concurrent sessions per certificate identity")
	maxChannels := flag.Int("max-channels", 16, "maximum channels per session")
	maxInflight := flag.Int("max-inflight", 0, "maximum un-acknowledged packets per session (0 = library default 32); raise for bulk senders")
	idleTimeout := flag.Duration("idle-timeout", 90*time.Second, "close sessions idle for this long")
	maxLifetime := flag.Duration("max-session-lifetime", 30*time.Minute, "force re-authentication after this long")
	metricsAddr := flag.String("metrics", "127.0.0.1:9109", "loopback-only Prometheus listener; set empty to disable")
	flag.Parse()
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
	reloader, err := certconfig.NewServerReloader(*cert, *key, *ca)
	if err != nil {
		log.Fatal(err)
	}
	server, err := mira.ListenTLS(*udp, *tcp, reloader.Config(), mira.Options{MaxSessions: *maxSessions, MaxSessionsPerIdentity: *maxPerIdentity, MaxChannels: *maxChannels, MaxInflight: *maxInflight, IdleTimeout: *idleTimeout, MaxSessionLifetime: *maxLifetime})
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()
	var base *durable.Store
	var storage journal.Store
	if *walPath != "" {
		if *keyFile == "" {
			log.Fatal("-wal requires -wal-keys")
		}
		base, err = durable.Open(*walPath, 512<<20)
		if err != nil {
			log.Fatal(err)
		}
		defer base.Close()
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
	if *metricsAddr != "" {
		host, _, e := net.SplitHostPort(*metricsAddr)
		if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			log.Fatal("metrics must bind an explicit loopback IP or be disabled")
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			if err := server.WritePrometheus(w); err != nil {
				log.Printf("metrics write: %v", err)
			}
		})
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
		metricsServer := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 20 * time.Second}
		go func() {
			if e := metricsServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				log.Printf("metrics listener: %v", e)
			}
		}()
		defer metricsServer.Close()
	}
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
						allowed.Store(updated)
						for id := range old {
							if _, ok := updated[id]; !ok {
								server.RevokeIdentity(id)
							}
						}
						for id := range updated {
							server.AllowIdentity(id)
						}
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
	defer stop()
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
		go func(ss *mira.Session) {
			defer ss.Close(context.Background())
			for {
				ch, err := ss.AcceptChannel(ctx)
				if err != nil {
					return
				}
				go func(c protocol.Channel) {
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
						_ = journal.ServeChannel(ctx, c, protocol.Principal{Subject: subject, Device: ss.Identity()}, storage)
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
