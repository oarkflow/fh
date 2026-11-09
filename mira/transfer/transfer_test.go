package transfer_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/mira/transfer"
	"github.com/oarkflow/fh/protocol"
)

// pair returns a connected client channel and the server's accepted channel.
func pair(t *testing.T) (client, server protocol.Channel) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	ca, _ := devpki.New()
	sc, _ := ca.ServerTLS()
	srv, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", sc, mira.Options{MaxChannels: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetChannelAuthorizer(func(string, protocol.ChannelConfig) bool { return true })
	got := make(chan protocol.Channel, 1)
	go func() {
		s, err := srv.Accept(ctx)
		if err != nil {
			return
		}
		ch, err := s.AcceptChannel(ctx)
		if err == nil {
			got <- ch
		}
	}()
	cc, _ := ca.ClientTLS("uploader")
	s, err := mira.DialTLS(ctx, srv.TLSAddr().String(), srv.Addr().String(), cc, mira.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(context.Background()) })
	client, err = s.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
	if err != nil {
		t.Fatal(err)
	}
	return client, <-got
}

func TestRoundTripLargeFile(t *testing.T) {
	cli, srv := pair(t)
	payload := make([]byte, 6<<20+123) // not a multiple of the chunk size
	rand.Read(payload)
	var dst bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := transfer.Receive(context.Background(), srv, &dst, 16<<20, nil)
		done <- err
	}()
	start := time.Now()
	if err := transfer.Send(context.Background(), cli, "big.bin", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dst.Bytes(), payload) {
		t.Fatal("payload corrupted")
	}
	rate := float64(len(payload)) / (1 << 20) / time.Since(start).Seconds()
	t.Logf("%d bytes in %s (%.1f MiB/s)", len(payload), time.Since(start).Round(time.Millisecond), rate)
	// Regression guard for the pacer wake-up bug: with no timer for paced sends
	// the session managed ~0.2 MiB/s on loopback. The floor is deliberately far
	// below what healthy runs achieve (5-20 MiB/s) so slow CI does not flake.
	if rate < 1.0 {
		t.Fatalf("throughput %.2f MiB/s is below the 1 MiB/s floor: pacing has regressed", rate)
	}
}

func TestEmptyFile(t *testing.T) {
	cli, srv := pair(t)
	go transfer.Receive(context.Background(), srv, &bytes.Buffer{}, 1<<20, nil)
	if err := transfer.Send(context.Background(), cli, "empty", bytes.NewReader(nil), 0); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverLimitRejectsBeforeData(t *testing.T) {
	cli, srv := pair(t)
	go transfer.Receive(context.Background(), srv, &bytes.Buffer{}, 1000, nil)
	err := transfer.Send(context.Background(), cli, "huge", bytes.NewReader(make([]byte, 5000)), 5000)
	if err == nil {
		t.Fatal("expected rejection")
	}
}

func TestCommitFailureIsNotAcknowledged(t *testing.T) {
	cli, srv := pair(t)
	go transfer.Receive(context.Background(), srv, &bytes.Buffer{}, 1<<20, func(transfer.Header) error { return errors.New("disk full") })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := transfer.Send(ctx, cli, "f", bytes.NewReader([]byte("data")), 4)
	if err == nil {
		t.Fatal("sender must not report success when commit failed")
	}
	t.Log(err)
}

func TestTamperedDigestDetected(t *testing.T) {
	cli, srv := pair(t)
	errc := make(chan error, 1)
	go func() {
		_, err := transfer.Receive(context.Background(), srv, &bytes.Buffer{}, 1<<20, nil)
		errc <- err
	}()
	ctx := context.Background()
	cli.Send(ctx, []byte(`H{"name":"x","size":4}`))
	cli.Send(ctx, append([]byte{'D'}, "abcd"...))
	cli.Send(ctx, append([]byte{'E'}, make([]byte, 32)...)) // wrong digest
	if err := <-errc; !errors.Is(err, transfer.ErrIntegrity) {
		t.Fatalf("got %v, want ErrIntegrity", err)
	}
}
