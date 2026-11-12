// Command deviceauth demonstrates registration, device-key login, recovery,
// device change, and protected API access over loopback.
package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: go run ./examples/deviceauth {server|client|admin} [args]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		serve(os.Args[2:])
	case "client":
		client(os.Args[2:])
	case "admin":
		admin(os.Args[2:])
	default:
		log.Fatalf("unknown command %q; choose server, client, or admin", os.Args[1])
	}
}
