// Command server is a small mTLS-authenticated MIRA UDP echo server.
// Generate test certificates with: go run ./cmd/mira-dev-certs -out ./dev-certs
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/protocol"
)

func main() {
	udpAddr := flag.String("udp", "127.0.0.1:7447", "MIRA encrypted UDP address")
	tcpAddr := flag.String("tcp", "127.0.0.1:7448", "TLS bootstrap address")
	certPath := flag.String("cert", "dev-certs/server.pem", "server certificate PEM")
	keyPath := flag.String("key", "dev-certs/server.key", "server private key PEM")
	caPath := flag.String("ca", "dev-certs/root.pem", "client CA certificate PEM")
	allow := flag.String("allow", "device-1", "comma-separated allowed certificate identities")
	flag.Parse()

	serverCert, err := tls.LoadX509KeyPair(*certPath, *keyPath)
	if err != nil {
		log.Fatal(err)
	}
	caPEM, err := os.ReadFile(*caPath)
	if err != nil {
		log.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		log.Fatal("client CA file contains no certificates")
	}
	tlsConfig, err := fh.NewServerTLSConfig(fh.ServerTLSOptions{
		Certificates:             []tls.Certificate{serverCert},
		ClientCAs:                clientCAs,
		RequireClientCertificate: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	app := fh.New()
	server, err := app.ListenMIRATLS(*udpAddr, *tcpAddr, tlsConfig, fh.MIRAOptions{
		MaxSessions:            100,
		MaxSessionsPerIdentity: 4,
		MaxChannels:            16,
		MaxMessage:             1 << 20,
		IdleTimeout:            90 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown()
	allowed := make(map[string]struct{})
	for _, identity := range strings.Split(*allow, ",") {
		identity = strings.TrimSpace(identity)
		if identity != "" {
			allowed[identity] = struct{}{}
		}
	}
	server.SetChannelAuthorizer(func(identity string, cfg protocol.ChannelConfig) bool {
		_, ok := allowed[identity]
		return ok && cfg.MaxMessage <= 1<<20
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("FH MIRA transport mTLS bootstrap on %s; encrypted UDP on %s", server.TLSAddr(), server.Addr())
	err = app.ServeMIRA(ctx, server, func(ctx context.Context, identity string, channel fh.MIRAChannel) error {
		log.Printf("accepted MIRA channel identity=%q id=%d", identity, channel.ID())
		serveChannel(ctx, channel)
		return nil
	})
	if ctx.Err() == nil && err != nil {
		log.Printf("MIRA transport stopped: %v", err)
	}
}

func serveChannel(ctx context.Context, channel protocol.Channel) {
	for {
		message, err := channel.Receive(ctx)
		if err != nil {
			return
		}
		reply := append([]byte("echo: "), message.Payload...)
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = channel.Send(sendCtx, reply)
		cancel()
		if err != nil {
			log.Printf("echo send failed: %v", err)
			return
		}
	}
}
