package resume

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestReconnectPublishAndDurableReplay(t *testing.T) {
	secret := bytes.Repeat([]byte{98}, 32)
	st, err := durable.Open(filepath.Join(t.TempDir(), "events.wal"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server, err := mira.Listen("127.0.0.1:0", secret, mira.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var peers sync.Mutex
	var active []*mira.Session
	go func() {
		for {
			session, err := server.Accept(ctx)
			if err != nil {
				return
			}
			peers.Lock()
			active = append(active, session)
			peers.Unlock()
			go func(s *mira.Session) {
				defer s.Close(context.Background())
				channel, err := s.AcceptChannel(ctx)
				if err != nil {
					return
				}
				_ = journal.ServeChannel(ctx, channel, protocol.Principal{Subject: "device-1"}, st)
			}(session)
		}
	}()
	client := New(func(ctx context.Context) (*mira.Session, error) {
		return mira.Dial(ctx, server.Addr().String(), secret, mira.Options{})
	})
	defer client.Close()
	cursor, err := client.Publish(ctx, "operation-100", []byte("first-event"))
	if err != nil || cursor == 0 {
		t.Fatal(cursor, err)
	}
	client.mu.Lock()
	client.drop()
	client.mu.Unlock()
	next, err := client.Publish(ctx, "operation-100", []byte("first-event"))
	if err != nil || next != cursor {
		t.Fatalf("retry dedupe: %d %v", next, err)
	}
	processed := 0
	last, err := client.ReplayAfter(ctx, 0, func(_ context.Context, e journal.Event) error {
		processed++
		if !bytes.Equal(e.Payload, []byte("first-event")) {
			t.Fatalf("replay data: %q", e.Payload)
		}
		return nil
	})
	if err != nil || last != cursor || processed != 1 {
		t.Fatalf("replay %+v %d %v", last, processed, err)
	}
	peers.Lock()
	for _, s := range active {
		_ = s.Close(context.Background())
	}
	peers.Unlock()
}
