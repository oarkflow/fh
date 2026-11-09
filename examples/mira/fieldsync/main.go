// Command fieldsync shows offline-tolerant, exactly-once-effect data capture:
// a field client publishes readings to a durable, encrypted journal over MIRA,
// survives a server crash mid-stream, retries safely with stable IDs, and then
// replays everything with a crash-safe checkpoint.
//
//	go run ./examples/mira/fieldsync
//
// Why it works: MIRA delivers bytes; the journal profile adds (1) fsync-before-
// AckStored, (2) idempotent publish keyed by a client-chosen ID, and (3) a
// cursor the client advances only after applying each event. A transport
// receipt alone could never give you these guarantees.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/protocol"
	"github.com/oarkflow/fh/resume"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("", "fieldsync-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := run(ctx, os.Stdout, dir); err != nil {
		log.Fatal(err)
	}
}

// backend is a journal server that can be crashed and restarted on the same
// encrypted WAL, as a real service would be.
type backend struct {
	dir   string
	ca    *devpki.CA
	key   []byte
	addrs atomic.Pointer[[2]string] // current TLS bootstrap and UDP addresses
	stop  func()
}

func (b *backend) start(ctx context.Context) error {
	tlsCfg, err := b.ca.ServerTLS()
	if err != nil {
		return err
	}
	srv, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", tlsCfg, mira.Options{MaxChannels: 4})
	if err != nil {
		return err
	}
	srv.SetChannelAuthorizer(func(id string, _ protocol.ChannelConfig) bool { return id == "field-unit-7" })
	base, err := durable.Open(filepath.Join(b.dir, "journal.wal"), 64<<20)
	if err != nil {
		srv.Close()
		return err
	}
	store, err := durable.NewEncryptedStore(base, 1, map[uint16][]byte{1: b.key})
	if err != nil {
		srv.Close()
		base.Close()
		return err
	}
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = journal.ServeSessions(sctx, srv, store) }()
	b.addrs.Store(&[2]string{srv.TLSAddr().String(), srv.Addr().String()})
	b.stop = func() { cancel(); srv.Close(); <-done; base.Close() }
	return nil
}

func run(ctx context.Context, out io.Writer, dir string) error {
	ca, err := devpki.New()
	if err != nil {
		return err
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	be := &backend{dir: dir, ca: ca, key: key}
	if err := be.start(ctx); err != nil {
		return err
	}
	defer func() { be.stop() }()

	clientTLS, err := ca.ClientTLS("field-unit-7")
	if err != nil {
		return err
	}
	// resume.Client redials through this function after any failure.
	client := resume.New(func(ctx context.Context) (*mira.Session, error) {
		a := be.addrs.Load()
		return mira.DialTLS(ctx, a[0], a[1], clientTLS, mira.Options{})
	})
	defer client.Close()

	cursors := map[string]uint64{}
	publish := func(id, text string) error {
		pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		c, err := client.Publish(pctx, id, []byte(text))
		if err != nil {
			return err
		}
		cursors[id] = c
		fmt.Fprintf(out, "published %-12s -> cursor %d\n", id, c)
		return nil
	}

	for i := 1; i <= 3; i++ {
		if err := publish(fmt.Sprintf("reading-%04d", i), fmt.Sprintf("temp=%d.5C", 20+i)); err != nil {
			return err
		}
	}

	fmt.Fprintln(out, "-- server crashes; the client keeps publishing --")
	be.stop()
	result := make(chan error, 1)
	go func() { result <- publish("reading-0004", "temp=24.5C") }()
	time.Sleep(700 * time.Millisecond) // the client is retrying with backoff
	fmt.Fprintln(out, "-- server restarts on the same encrypted WAL --")
	if err := be.start(ctx); err != nil {
		return err
	}
	if err := <-result; err != nil {
		return err
	}

	// A retry of an already-committed ID (a lost reply, say) must not duplicate.
	before := cursors["reading-0002"]
	if err := publish("reading-0002", "temp=22.5C"); err != nil {
		return err
	}
	if cursors["reading-0002"] != before {
		return fmt.Errorf("duplicate publish moved cursor %d -> %d", before, cursors["reading-0002"])
	}
	fmt.Fprintln(out, "retry of reading-0002 returned the same cursor: no duplicate stored")

	cp, err := resume.OpenCursorFile(filepath.Join(dir, "cursor"))
	if err != nil {
		return err
	}
	var applied []string
	apply := func(_ context.Context, ev journal.Event) error {
		applied = append(applied, string(ev.Payload)) // do the real work idempotently here
		return nil
	}
	last, err := client.ReplayCheckpointed(ctx, cp, apply)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "replayed %d events in order, checkpoint now %d: %v\n", len(applied), last, applied)
	applied = nil
	if _, err := client.ReplayCheckpointed(ctx, cp, apply); err != nil {
		return err
	}
	fmt.Fprintf(out, "second replay delivered %d events (nothing new)\n", len(applied))
	if len(applied) != 0 {
		return fmt.Errorf("checkpointed replay redelivered %d events", len(applied))
	}
	return nil
}
