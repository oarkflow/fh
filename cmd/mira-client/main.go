package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7447", "remote UDP endpoint")
	msg := flag.String("message", "hello from MIRA-T/1", "text to send")
	identity := flag.String("identity", "", "credential identity for device authentication")
	flag.Parse()
	psk, err := hex.DecodeString(os.Getenv("MIRA_PSK"))
	if err != nil || len(psk) != 32 {
		log.Fatal("set MIRA_PSK to 64 random hexadecimal characters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var sess *mira.Session
	if *identity == "" {
		sess, err = mira.Dial(ctx, *addr, psk, mira.Options{})
	} else {
		sess, err = mira.DialWithIdentity(ctx, *addr, *identity, psk, mira.Options{})
	}
	if err != nil {
		log.Fatal(err)
	}
	defer sess.Close(context.Background())
	ch, err := sess.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		log.Fatal(err)
	}
	id, err := ch.Send(ctx, []byte(*msg))
	if err != nil {
		log.Fatal(err)
	}
	receipt, err := ch.AwaitReceipt(ctx, id, protocol.AckTransport)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("transport acknowledged message %d at %s\n", id, receipt.At.Format(time.RFC3339Nano))
	response, err := ch.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("server: %s\n", response.Payload)
}
