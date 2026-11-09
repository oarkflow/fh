package durable

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/protocol"
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptedWALIdempotencyAndKeyRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret.wal")
	base, err := Open(path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	keyring := map[uint16][]byte{1: bytes.Repeat([]byte{20}, 32), 2: bytes.Repeat([]byte{21}, 32)}
	s, err := NewEncryptedStore(base, 1, keyring)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p := protocol.Principal{Subject: "alice", Tenant: "t1"}
	msg := protocol.Message{Payload: []byte("private content"), MessageID: 4}
	cursor, err := s.AppendIdempotent(ctx, p, "operation-1", msg)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.AppendIdempotent(ctx, p, "operation-1", msg)
	if err != nil || cursor != again {
		t.Fatalf("idempotency %d %d %v", cursor, again, err)
	}
	conflict := msg
	conflict.Payload = []byte("malicious different content")
	if _, err := s.AppendIdempotent(ctx, p, "operation-1", conflict); err != ErrConflict {
		t.Fatal("conflict detection", err)
	}
	if err := s.Rotate(2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendIdempotent(ctx, p, "operation-2", msg); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("private content")) {
		t.Fatal("plaintext leaked to WAL")
	}
	base, err = Open(path, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	s, err = NewEncryptedStore(base, 2, keyring)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.ReadRecordsAfter(ctx, p, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("records %d", len(entries))
	}
	for _, entry := range entries {
		if string(entry.Message.Payload) != "private content" {
			t.Fatal("decrypt mismatch")
		}
	}
	if _, err := NewEncryptedStore(base, 2, map[uint16][]byte{2: keyring[2]}); err != nil {
		t.Fatal(err)
	}
	onlyCurrent, _ := NewEncryptedStore(base, 2, map[uint16][]byte{2: keyring[2]})
	if _, err := onlyCurrent.ReadRecordsAfter(ctx, p, 0, 8); err != ErrEncryption {
		t.Fatal("missing historical key must fail closed", err)
	}
}
