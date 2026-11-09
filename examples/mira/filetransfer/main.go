// Command filetransfer uploads a file over MIRA with end-to-end SHA-256
// verification and a commit receipt: the client only sees success after the
// server has fsynced and atomically renamed the file into place.
//
//	go run ./examples/mira/filetransfer -size 32          # 32 MiB self-test
//	go run ./examples/mira/filetransfer -file ./big.iso   # upload a real file
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/mira/transfer"
	"github.com/oarkflow/fh/protocol"
)

const maxUpload = 1 << 30

// bulkInflight raises the sender's packets-in-flight cap from the default 32.
// Throughput is bounded by roughly inflight x MTU / RTT; measured on loopback,
// 32 gives ~9 MiB/s and 128 gives ~20 MiB/s. Raise it only for bulk senders:
// every in-flight packet is memory held until acknowledged.
const bulkInflight = 128

func main() {
	file := flag.String("file", "", "file to upload (default: generate random data)")
	sizeMiB := flag.Int("size", 16, "MiB of random data when -file is empty")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	dest, err := os.MkdirTemp("", "uploads-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dest)
	src := *file
	if src == "" {
		f, err := os.CreateTemp("", "payload-*.bin")
		if err != nil {
			log.Fatal(err)
		}
		defer os.Remove(f.Name())
		if _, err := io.CopyN(f, rand.Reader, int64(*sizeMiB)<<20); err != nil {
			log.Fatal(err)
		}
		f.Close()
		src = f.Name()
	}
	if err := run(ctx, os.Stdout, src, dest); err != nil {
		log.Fatal(err)
	}
}

// storeUpload receives one transfer into dest. The file is written to a temp
// name, fsynced, and renamed only from the commit hook, so a crash or a failed
// verification can never leave a partial file under its final name.
func storeUpload(ctx context.Context, ch protocol.Channel, dest string) error {
	tmp, err := os.CreateTemp(dest, ".incoming-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	defer tmp.Close()
	_, err = transfer.Receive(ctx, ch, tmp, maxUpload, func(h transfer.Header) error {
		if err := tmp.Sync(); err != nil {
			return err
		}
		// The client chose the name: keep only its last element.
		return os.Rename(tmp.Name(), filepath.Join(dest, filepath.Base(h.Name)))
	})
	return err
}

func run(ctx context.Context, out io.Writer, src, dest string) error {
	ca, err := devpki.New()
	if err != nil {
		return err
	}
	serverTLS, err := ca.ServerTLS()
	if err != nil {
		return err
	}
	srv, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", serverTLS, mira.Options{MaxChannels: 4})
	if err != nil {
		return err
	}
	defer srv.Close()
	srv.SetChannelAuthorizer(func(id string, c protocol.ChannelConfig) bool {
		return id == "uploader" && c.Delivery == protocol.ReliableOrdered
	})
	go func() {
		for {
			s, err := srv.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				defer s.Close(context.Background())
				for {
					ch, err := s.AcceptChannel(ctx)
					if err != nil {
						return
					}
					go func() {
						defer ch.Close(context.Background())
						if err := storeUpload(ctx, ch, dest); err != nil {
							log.Printf("upload from %s failed: %v", s.Identity(), err)
						}
					}()
				}
			}()
		}
	}()

	clientTLS, err := ca.ClientTLS("uploader")
	if err != nil {
		return err
	}
	session, err := mira.DialTLS(ctx, srv.TLSAddr().String(), srv.Addr().String(), clientTLS, mira.Options{MaxInflight: bulkInflight})
	if err != nil {
		return err
	}
	defer session.Close(context.Background())
	ch, err := session.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		return err
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	start := time.Now()
	if err := transfer.Send(ctx, ch, filepath.Base(src), f, st.Size()); err != nil {
		return err
	}
	elapsed := time.Since(start)

	want, err := digest(src)
	if err != nil {
		return err
	}
	got, err := digest(filepath.Join(dest, filepath.Base(src)))
	if err != nil {
		return err
	}
	if want != got {
		return fmt.Errorf("stored file differs: %s != %s", got, want)
	}
	fmt.Fprintf(out, "uploaded %d bytes in %s (%.1f MiB/s); server sha256 %s verified\n",
		st.Size(), elapsed.Round(time.Millisecond), float64(st.Size())/(1<<20)/elapsed.Seconds(), got[:16])
	return nil
}

func digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
