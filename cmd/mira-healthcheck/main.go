// mira-healthcheck is a dependency-free Docker HEALTHCHECK probe for
// mira-tls-server (a scratch image has no shell or curl). It requests the
// server's loopback /healthz and exits 0 only on HTTP 204.
package main

import (
	"flag"
	"net/http"
	"os"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:9109/healthz", "health endpoint")
	timeout := flag.Duration("timeout", 3*time.Second, "request timeout")
	flag.Parse()
	client := http.Client{Timeout: *timeout}
	resp, err := client.Get(*url)
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		os.Exit(1)
	}
}
