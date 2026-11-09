package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/oarkflow/fh/internal/certconfig"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"log"
	"time"
)

func main() {
	tcp := flag.String("tcp", "127.0.0.1:7448", "TLS bootstrap address")
	udp := flag.String("udp", "127.0.0.1:7447", "MIRA data address")
	cert := flag.String("cert", "", "client PEM certificate")
	key := flag.String("key", "", "client PEM private key")
	ca := flag.String("ca", "", "trusted server CA PEM")
	name := flag.String("name", "server.local", "server certificate DNS name")
	message := flag.String("message", "hello MIRA-T/1 v4", "echo payload")
	probe := flag.Int("probe-mtu", 0, "optional MTU probe e.g. 1400")
	flag.Parse()
	conf, err := certconfig.Client(*cert, *key, *ca, *name)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	session, err := mira.DialTLS(ctx, *tcp, *udp, conf, mira.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close(context.Background())
	if *probe != 0 {
		mt, err := session.ProbeMTU(ctx, *probe)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("authenticated MTU probe: %d bytes\n", mt)
	}
	ch, err := session.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		log.Fatal(err)
	}
	id, err := ch.Send(ctx, []byte(*message))
	if err != nil {
		log.Fatal(err)
	}
	msg, err := ch.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	_, err = ch.AwaitReceipt(ctx, id, protocol.AckTransport)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("MIRA echo: %s\n", msg.Payload)
}
