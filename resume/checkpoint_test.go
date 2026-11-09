package resume

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCursorCheckpointAtomicMonotonicAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	cp, err := OpenCursorFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cp.Cursor() != 0 {
		t.Fatal("new state must begin at cursor zero")
	}
	if err := cp.Store(17); err != nil {
		t.Fatal(err)
	}
	if err := cp.Store(4); err == nil {
		t.Fatal("cursor rollback accepted")
	}
	reopened, err := OpenCursorFile(path)
	if err != nil || reopened.Cursor() != 17 {
		t.Fatalf("reopen %d, %v", reopened.Cursor(), err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("permissions %o", info.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[11] ^= 0xff
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCursorFile(path); err == nil {
		t.Fatal("corrupt state accepted")
	}
}
func TestCursorConcurrentWrites(t *testing.T) {
	cp, err := OpenCursorFile(filepath.Join(t.TempDir(), "cursor"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 1; i <= 30; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _ = cp.Store(uint64(i)) }(i)
	}
	wg.Wait()
	if cp.Cursor() != 30 {
		t.Fatalf("cursor %d", cp.Cursor())
	}
}
func TestReplayCheckpointNil(t *testing.T) {
	c := New(nil)
	if _, err := c.ReplayCheckpointed(context.Background(), nil, nil); err == nil {
		t.Fatal("nil checkpoint accepted")
	}
}

func TestCheckpointedReplayOverRealMIRA(t *testing.T) {
	key := bytes.Repeat([]byte{77}, 32)
	st, err := durable.Open(filepath.Join(t.TempDir(), "events.wal"), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server, err := mira.Listen("127.0.0.1:0", key, mira.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	go func() {
		for {
			ss, e := server.Accept(ctx)
			if e != nil {
				return
			}
			go func(s *mira.Session) {
				defer s.Close(context.Background())
				ch, e := s.AcceptChannel(ctx)
				if e == nil {
					_ = journal.ServeChannel(ctx, ch, protocol.Principal{Subject: "checkpoint-device"}, st)
				}
			}(ss)
		}
	}()
	dial := func(ctx context.Context) (*mira.Session, error) {
		return mira.Dial(ctx, server.Addr().String(), key, mira.Options{})
	}
	client := New(dial)
	defer client.Close()
	if _, err := client.Publish(ctx, "checkpoint-op-1", []byte("first")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cursor")
	checkpoint, err := OpenCursorFile(path)
	if err != nil {
		t.Fatal(err)
	}
	applied := 0
	cursor, err := client.ReplayCheckpointed(ctx, checkpoint, func(_ context.Context, e journal.Event) error { applied++; return nil })
	if err != nil || cursor == 0 || applied != 1 || checkpoint.Cursor() != cursor {
		t.Fatalf("initial replay: %d %d %v", cursor, applied, err)
	}
	client.Close()
	reopened, err := OpenCursorFile(path)
	if err != nil {
		t.Fatal(err)
	}
	client2 := New(dial)
	defer client2.Close()
	next, err := client2.ReplayCheckpointed(ctx, reopened, func(_ context.Context, e journal.Event) error { applied++; return nil })
	if err != nil || next != cursor || applied != 1 {
		t.Fatalf("duplicate replay after restart: %d %d %v", next, applied, err)
	}
	if _, err := client2.Publish(ctx, "checkpoint-op-2", []byte("second")); err != nil {
		t.Fatal(err)
	}
	next, err = client2.ReplayCheckpointed(ctx, reopened, func(_ context.Context, e journal.Event) error { applied++; return nil })
	if err != nil || next <= cursor || applied != 2 {
		t.Fatalf("next replay: %d %d %v", next, applied, err)
	}
}
