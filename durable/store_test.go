package durable

import (
	"bytes"
	"context"
	"fmt"
	"github.com/oarkflow/fh/protocol"
	"os"
	"path/filepath"
	"testing"
)

func TestCrashRecoveryAndIdempotency(t *testing.T) {
	p := protocol.Principal{Subject: "alice", Tenant: "tenant1", Device: "phone"}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "log")
	s, e := Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	m := protocol.Message{ChannelID: 1, MessageID: 4, Payload: []byte("secret")}
	c, e := s.AppendIdempotent(ctx, p, "operation-42", m)
	if e != nil || c != 1 {
		t.Fatal(c, e)
	}
	m.Payload[0] = 'X'
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	m.Payload[0] = 's'
	c2, e := s.AppendIdempotent(ctx, p, "operation-42", m)
	if e != nil || c2 != c {
		t.Fatalf("idempotency %d %v", c2, e)
	}
	m.Payload = []byte("different")
	if _, e = s.AppendIdempotent(ctx, p, "operation-42", m); e != ErrConflict {
		t.Fatalf("expected conflict: %v", e)
	}
	rows, e := s.ReadRecordsAfter(ctx, p, 0, 10)
	if e != nil || len(rows) != 1 || rows[0].Cursor != 1 || !bytes.Equal(rows[0].Message.Payload, []byte("secret")) {
		t.Fatalf("records: %#v %v", rows, e)
	}
	if e = s.Acknowledge(ctx, p, 1); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	n, e := s.ReplayCursor(p)
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	rows, e = s.ReadRecordsAfter(ctx, p, 0, 10)
	if e != nil || len(rows) != 0 {
		t.Fatal(rows, e)
	}
}
func TestTornTailAndCorruption(t *testing.T) {
	ctx := context.Background()
	p := protocol.Principal{Subject: "a"}
	path := filepath.Join(t.TempDir(), "a.wal")
	s, _ := Open(path, 1<<20)
	_, e := s.Append(ctx, p, protocol.Message{Payload: []byte("test")})
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.Write([]byte{0, 0, 0, 7})
	f.Close()
	s, e = Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	f, _ = os.OpenFile(path, os.O_RDWR, 0600)
	f.WriteAt([]byte{255}, 10)
	f.Close()
	_, e = Open(path, 1<<20)
	if e == nil {
		t.Fatal("expected checksum failure")
	}
}
func TestCompactionAndExclusiveWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "store.wal")
	p := protocol.Principal{Subject: "subject"}
	s, e := Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if other, e := Open(path, 1<<20); e == nil {
		other.Close()
		t.Fatal("multiple writers were accepted")
	}
	for i := 0; i < 20; i++ {
		_, e = s.AppendIdempotent(ctx, p, fmt.Sprintf("key-%d", i), protocol.Message{Payload: bytes.Repeat([]byte(fmt.Sprintf("msg-%d", i)), 2048)})
		if e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Acknowledge(ctx, p, 10); e != nil {
		t.Fatal(e)
	}
	old, _ := os.Stat(path)
	if e = s.Compact(ctx); e != nil {
		t.Fatal(e)
	}
	newSize, _ := os.Stat(path)
	if newSize.Size() >= old.Size() {
		t.Fatalf("compaction did not reclaim space: before=%d after=%d", old.Size(), newSize.Size())
	}
	s.Close()
	s, e = Open(path, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	rows, e := s.ReadRecordsAfter(ctx, p, 0, 100)
	if e != nil || len(rows) != 10 || rows[0].Cursor != 11 {
		t.Fatalf("recovered %d rows (%v)", len(rows), e)
	}
	cursor, e := s.AppendIdempotent(ctx, p, "key-0", protocol.Message{Payload: bytes.Repeat([]byte("msg-0"), 2048)})
	if e != nil || cursor != 1 {
		t.Fatalf("idempotent after compaction %d %v", cursor, e)
	}
	if _, e = s.Append(ctx, p, protocol.Message{Payload: []byte("after-compact")}); e != nil {
		t.Fatal("append after compaction", e)
	}
}
