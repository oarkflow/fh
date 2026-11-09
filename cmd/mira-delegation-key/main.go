// mira-delegation-key provisions an independent Ed25519 gateway assertion key.
// This key is NOT a TLS private key or a JWT signing key for public clients.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"log"
	"os"
	"path/filepath"
)

func writeNew(path string, mode os.FileMode, block *pem.Block) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := pem.Encode(f, block); err != nil {
		return err
	}
	return f.Sync()
}
func main() {
	dir := flag.String("out", "", "existing private output directory; key files must not already exist")
	flag.Parse()
	if *dir == "" {
		log.Fatal("-out directory required")
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		log.Fatal(err)
	}
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		log.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		log.Fatal(err)
	}
	if err := writeNew(filepath.Join(*dir, "gateway-delegation.key"), 0600, &pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}); err != nil {
		log.Fatal(err)
	}
	if err := writeNew(filepath.Join(*dir, "gateway-delegation.pub"), 0644, &pem.Block{Type: "PUBLIC KEY", Bytes: public}); err != nil {
		log.Fatal(err)
	}
	log.Print("created gateway-delegation.key (private) and gateway-delegation.pub (server trust anchor)")
}
