// Command client connects to the example server, sends one message, and prints
// the echo. Generate test certificates with: go run ./cmd/mira-dev-certs -out ./dev-certs
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/protocol"
)

func main() {
	tcpAddr := flag.String("tcp", "127.0.0.1:7448", "TLS bootstrap address")
	udpAddr := flag.String("udp", "127.0.0.1:7447", "MIRA UDP address")
	serverName := flag.String("name", "server.local", "server certificate DNS name")
	certPath := flag.String("cert", "dev-certs/client.pem", "client certificate PEM")
	keyPath := flag.String("key", "dev-certs/client.key", "client private key PEM")
	caPath := flag.String("ca", "dev-certs/root.pem", "trusted server CA PEM")
	message := flag.String("message", "hello from fh", "message to echo")
	flag.Parse()

	clientCert, err := tls.LoadX509KeyPair(*certPath, *keyPath)
	if err != nil {
		log.Fatal(err)
	}
	caPEM, err := os.ReadFile(*caPath)
	if err != nil {
		log.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		log.Fatal("CA file contains no certificates")
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      roots,
		ServerName:   *serverName,
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := fh.NewMIRAClient(tlsConfig, fh.MIRAOptions{})
	if err != nil {
		log.Fatal(err)
	}
	session, err := client.Connect(ctx, *tcpAddr, *udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close(context.Background())
	channel, err := session.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		log.Fatal(err)
	}
	messageID, err := channel.Send(ctx, []byte(*message))
	if err != nil {
		log.Fatal(err)
	}
	reply, err := channel.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := channel.AwaitReceipt(ctx, messageID, protocol.AckTransport); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("server replied: %s\n", reply.Payload)
}
