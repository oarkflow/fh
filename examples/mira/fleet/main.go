// Command fleet is a device-fleet control plane built on fh + MIRA.
//
// Devices connect over MIRA (mTLS bootstrap, encrypted UDP). They publish
// LatestState telemetry and receive ReliableOrdered commands that they confirm
// with AckProcessed. Operators use a plain HTTP API served by the same fh App.
//
// Demo mode (default; no certificates needed) also starts simulated devices:
//
//	go run ./examples/mira/fleet -devices 3
//	curl localhost:8080/fleet
//	curl -X POST localhost:8080/fleet/device-1/commands -d '{"op":"set_setpoint","value":25}'
//
// Production mode serves real devices that hold certificates from your CA:
//
//	go run ./examples/mira/fleet -cert server.pem -key server.key -ca root.pem \
//	    -udp :7447 -tcp :7448 -http 127.0.0.1:8080
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/mira/fleet"
	"github.com/oarkflow/fh/mira/mtls"
)

func main() {
	udp := flag.String("udp", "127.0.0.1:7447", "MIRA encrypted UDP address")
	tcp := flag.String("tcp", "127.0.0.1:7448", "mTLS bootstrap TCP address")
	httpAddr := flag.String("http", "127.0.0.1:8080", "operator HTTP API address")
	cert := flag.String("cert", "", "server certificate PEM (empty = demo mode)")
	key := flag.String("key", "", "server private key PEM")
	ca := flag.String("ca", "", "client CA PEM")
	devices := flag.Int("devices", 3, "simulated devices in demo mode")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var serverTLS *tls.Config
	var devCA *devpki.CA
	var err error
	if *cert == "" {
		if devCA, err = devpki.New(); err == nil {
			serverTLS, err = devCA.ServerTLS()
		}
	} else {
		serverTLS, err = mtls.LoadServer(*cert, *key, *ca)
	}
	if err != nil {
		log.Fatal(err)
	}

	app := fh.New()
	hub := NewHub()
	Routes(app, hub)
	server, err := app.ListenMIRATLS(*udp, *tcp, serverTLS, fh.MIRAOptions{
		MaxSessions: 5000, MaxSessionsPerIdentity: 2, MaxChannels: 4, MaxMessage: fleet.MaxCommand,
		IdleTimeout: 60 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	server.SetChannelAuthorizer(hub.Authorize)

	if devCA != nil {
		for i := 1; i <= *devices; i++ {
			cfg, err := devCA.ClientTLS(fmt.Sprintf("device-%d", i))
			if err != nil {
				log.Fatal(err)
			}
			dev := NewThermostat()
			agent := &fleet.Agent{TLS: cfg, TCPAddr: server.TLSAddr().String(), UDPAddr: server.Addr().String(),
				Interval: 100 * time.Millisecond, Telemetry: dev.Snapshot, OnCommand: dev.Apply,
				OnDisconnect: func(err error) { log.Printf("device session ended: %v", err) }}
			go agent.Run(ctx)
		}
	}

	go func() {
		if err := app.ServeMIRA(ctx, server, hub.Handle); err != nil && ctx.Err() == nil {
			log.Printf("MIRA stopped: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = app.ShutdownWithTimeout(5 * time.Second)
	}()
	log.Printf("MIRA udp=%s bootstrap=%s; HTTP http://%s", server.Addr(), server.TLSAddr(), *httpAddr)
	if err := app.Listen(*httpAddr); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
