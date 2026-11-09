// mira-dev-certs generates short-lived private TEST credentials for localhost.
// NEVER deploy this generated CA as a production trust root.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	dir := flag.String("out", "./dev-certs", "directory to create; must not already exist")
	name := flag.String("name", "server.local", "server DNS name")
	clientName := flag.String("client", "device-1", "client certificate identity")
	flag.Parse()
	if err := os.Mkdir(*dir, 0700); err != nil {
		log.Fatal("create fresh directory (never overwrite keys): ", err)
	}
	issueKey := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			log.Fatal(err)
		}
		return k
	}
	serial := func() *big.Int {
		v, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if err != nil {
			log.Fatal(err)
		}
		return v
	}
	now := time.Now()
	caK := issueKey()
	ca := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "MIRA DEVELOPMENT ROOT - DO NOT TRUST IN PRODUCTION"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(7 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, MaxPathLenZero: true}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caK.Public(), caK)
	if err != nil {
		log.Fatal(err)
	}
	write := func(filename string, data []byte, mode os.FileMode) {
		file := filepath.Join(*dir, filename)
		if err := os.WriteFile(file, data, mode); err != nil {
			log.Fatal(err)
		}
	}
	write("root.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0644)
	write("root.key", privatePEM(caK), 0600)
	mint := func(id string, server bool) {
		k := issueKey()
		usage := x509.ExtKeyUsageClientAuth
		if server {
			usage = x509.ExtKeyUsageServerAuth
		}
		cert := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: id}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		if server {
			cert.DNSNames = []string{id}
			cert.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP("::1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, k.Public(), caK)
		if err != nil {
			log.Fatal(err)
		}
		filename := "client"
		if server {
			filename = "server"
		}
		write(filename+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644)
		write(filename+".key", privatePEM(k), 0600)
	}
	mint(*name, true)
	mint(*clientName, false)
	log.Printf("development-only test credentials written to %s", *dir)
}
func privatePEM(k *ecdsa.PrivateKey) []byte {
	b, e := x509.MarshalPKCS8PrivateKey(k)
	if e != nil {
		log.Fatal(e)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: b})
}
