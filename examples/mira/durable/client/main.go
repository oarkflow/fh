// Command client publishes one idempotent event and replays committed events
// after its durable local cursor using a fresh mTLS MIRA session.
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

	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/resume"
)

func main() {
	tcpAddr := flag.String("tcp", "127.0.0.1:7448", "TLS bootstrap address")
	udpAddr := flag.String("udp", "127.0.0.1:7447", "MIRA UDP address")
	serverName := flag.String("name", "server.local", "server certificate DNS name")
	certPath := flag.String("cert", "dev-certs/client.pem", "client certificate PEM")
	keyPath := flag.String("key", "dev-certs/client.key", "client private key PEM")
	caPath := flag.String("ca", "dev-certs/root.pem", "trusted server CA PEM")
	checkpointPath := flag.String("checkpoint", "/tmp/fh-mira-cursor", "absolute local replay cursor file")
	publishID := flag.String("id", "", "stable operation ID; required when publishing")
	message := flag.String("message", "", "optional message to publish")
	flag.Parse()

	if *message != "" && *publishID == "" {
		log.Fatal("-id is required with -message and must be reused for retries")
	}
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

	checkpoint, err := resume.OpenCursorFile(*checkpointPath)
	if err != nil {
		log.Fatal(err)
	}
	client := resume.New(func(ctx context.Context) (*mira.Session, error) {
		return mira.DialTLS(ctx, *tcpAddr, *udpAddr, tlsConfig, mira.Options{})
	})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if *message != "" {
		cursor, err := client.Publish(ctx, *publishID, []byte(*message))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("committed operation=%s cursor=%d\n", *publishID, cursor)
	}

	cursor, err := client.ReplayCheckpointed(ctx, checkpoint, func(_ context.Context, event journal.Event) error {
		fmt.Printf("event cursor=%d payload=%s\n", event.Cursor, event.Payload)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("replay acknowledged through cursor=%d\n", cursor)
}
