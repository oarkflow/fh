package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"log"
	"os"
	"time"
)

func main() {
	address := flag.String("addr", "127.0.0.1:7447", "UDP server address")
	device := flag.String("identity", "", "device credential ID")
	op := flag.String("op", "publish", "publish, replay or ack")
	id := flag.String("id", "", "stable request id (required for publish)")
	payload := flag.String("message", "", "publish payload")
	cursor := flag.Uint64("cursor", 0, "replay starting cursor / ack cursor")
	flag.Parse()
	secret, e := hex.DecodeString(os.Getenv("MIRA_PSK"))
	if e != nil || len(secret) != 32 || *device == "" {
		log.Fatal("provide -identity and 64-hex-byte MIRA_PSK")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session, e := mira.DialWithIdentity(ctx, *address, *device, secret, mira.Options{})
	if e != nil {
		log.Fatal(e)
	}
	defer session.Close(context.Background())
	ch, e := session.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if e != nil {
		log.Fatal(e)
	}
	msgID, e := journal.SendRequest(ctx, ch, journal.Request{Operation: *op, ID: *id, Payload: []byte(*payload), Cursor: *cursor})
	if e != nil {
		log.Fatal(e)
	}
	if *op == "publish" {
		if _, e = ch.AwaitReceipt(ctx, msgID, protocol.AckStored); e != nil {
			log.Fatal("commit receipt: ", e)
		}
	}
	for {
		ev, e := journal.ReadEvent(ctx, ch)
		if e != nil {
			log.Fatal(e)
		}
		fmt.Printf("event=%s cursor=%d id=%s message=%s error=%s\n", ev.Type, ev.Cursor, ev.ID, string(ev.Payload), ev.Error)
		if ev.Type == "replay_complete" || *op != "replay" {
			break
		}
	}
}
