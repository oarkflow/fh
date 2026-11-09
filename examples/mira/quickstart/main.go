// Command quickstart is the smallest complete MIRA program: an mTLS-secured
// server and a client in one process, one reliable channel, a transport
// receipt and the session statistics. It needs no certificates or flags.
//
//	go run ./examples/mira/quickstart
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/protocol"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := run(ctx, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, out io.Writer) error {
	pki, err := devpki.New()
	if err != nil {
		return err
	}
	serverTLS, err := pki.ServerTLS()
	if err != nil {
		return err
	}

	// 1. Server: an fh App owns the MIRA listener, so app.Shutdown() closes it.
	app := fh.New()
	defer app.Shutdown()
	server, err := app.ListenMIRATLS("127.0.0.1:0", "127.0.0.1:0", serverTLS, fh.MIRAOptions{})
	if err != nil {
		return err
	}
	// Channel opens fail closed until a policy is installed.
	server.SetChannelAuthorizer(func(identity string, cfg protocol.ChannelConfig) bool {
		return identity == "device-1"
	})
	go app.ServeMIRA(ctx, server, func(ctx context.Context, identity string, ch fh.MIRAChannel) error {
		for {
			msg, err := ch.Receive(ctx)
			if err != nil {
				return nil
			}
			if _, err := ch.Send(ctx, append([]byte("echo: "), msg.Payload...)); err != nil {
				return err
			}
		}
	})

	// 2. Client: mTLS bootstrap over TCP, then encrypted UDP.
	clientTLS, err := pki.ClientTLS("device-1")
	if err != nil {
		return err
	}
	client, err := fh.NewMIRAClient(clientTLS, fh.MIRAOptions{})
	if err != nil {
		return err
	}
	session, err := client.Connect(ctx, server.TLSAddr().String(), server.Addr().String())
	if err != nil {
		return err
	}
	defer session.Close(context.Background())

	ch, err := session.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		return err
	}
	id, err := ch.Send(ctx, []byte("hello over MIRA"))
	if err != nil {
		return err
	}
	reply, err := ch.Receive(ctx)
	if err != nil {
		return err
	}
	if _, err := ch.AwaitReceipt(ctx, id, protocol.AckTransport); err != nil {
		return err
	}
	fmt.Fprintf(out, "server identity seen as: %s\n", session.Identity())
	fmt.Fprintf(out, "reply: %s\n", reply.Payload)

	stats, err := session.Stats(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "path MTU %d, rtt %s, tx key epoch %d\n", stats.PathMTU, stats.RTT.Round(time.Microsecond), stats.TXKeyEpoch)
	return nil
}
