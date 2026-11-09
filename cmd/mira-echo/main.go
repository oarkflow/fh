package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/fh/mira"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:7447", "UDP listen address")
	flag.Parse()
	raw := os.Getenv("MIRA_PSK")
	psk, err := hex.DecodeString(raw)
	if err != nil || len(psk) != 32 {
		log.Fatal("set MIRA_PSK to 64 random hexadecimal characters")
	}
	srv, err := mira.Listen(*addr, psk, mira.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()
	log.Printf("MIRA-T/1 server ready at %s", srv.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for {
		session, err := srv.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			defer session.Close(context.Background())
			log.Printf("session %x connected", session.ID())
			for {
				ch, err := session.AcceptChannel(ctx)
				if err != nil {
					return
				}
				go func() {
					for {
						recvCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
						msg, err := ch.Receive(recvCtx)
						cancel()
						if err != nil {
							return
						}
						log.Printf("received cid=%d mid=%d bytes=%d", ch.ID(), msg.MessageID, len(msg.Payload))
						sendCtx, done := context.WithTimeout(ctx, 5*time.Second)
						_, err = ch.Send(sendCtx, []byte(fmt.Sprintf("echo: %s", msg.Payload)))
						done()
						if err != nil {
							log.Printf("send failed: %v", err)
							return
						}
					}
				}()
			}
		}()
	}
}
