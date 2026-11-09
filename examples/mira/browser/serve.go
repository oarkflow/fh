// Command serve hosts the browser gateway sample over HTTPS for local testing.
// Use a trusted development certificate; browsers correctly reject untrusted TLS.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:9444", "HTTPS listen address")
	cert := flag.String("cert", "dev-certs/server.pem", "TLS certificate PEM")
	key := flag.String("key", "dev-certs/server.key", "TLS private key PEM")
	dir := flag.String("dir", "examples/mira/browser", "directory containing the demo page and compiled JavaScript")
	flag.Parse()
	server := &http.Server{
		Addr:              *addr,
		Handler:           http.FileServer(http.Dir(*dir)),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	log.Printf("serving MIRA browser sample at https://%s", *addr)
	log.Fatal(server.ListenAndServeTLS(*cert, *key))
}
