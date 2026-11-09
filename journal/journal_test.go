package journal

import (
	"bytes"
	"context"
	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
	"path/filepath"
	"testing"
	"time"
)

func TestDurableJournalResumeOverTwoSessions(t *testing.T) {
	key := bytes.Repeat([]byte{32}, 32)
	st, e := durable.Open(filepath.Join(t.TempDir(), "journal.wal"), 8<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer st.Close()
	srv, e := mira.Listen("127.0.0.1:0", key, mira.Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	principal := protocol.Principal{Subject: "device-123"}
	connect := func() (protocol.Channel, func()) {
		t.Helper()
		c, e := mira.Dial(ctx, srv.Addr().String(), key, mira.Options{})
		if e != nil {
			t.Fatal(e)
		}
		s, e := srv.Accept(ctx)
		if e != nil {
			t.Fatal(e)
		}
		go func() {
			ch, e := s.AcceptChannel(ctx)
			if e == nil {
				_ = ServeChannel(ctx, ch, principal, st)
			}
		}()
		ch, e := c.OpenChannel(ctx, protocol.ChannelConfig{Delivery: protocol.ReliableOrdered})
		if e != nil {
			t.Fatal(e)
		}
		return ch, func() { c.Close(context.Background()); s.Close(context.Background()) }
	}
	ch, close1 := connect()
	msgID, e := SendRequest(ctx, ch, Request{Operation: "publish", ID: "operation-001", Payload: []byte("durable-data")})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ch.AwaitReceipt(ctx, msgID, protocol.AckStored); e != nil {
		t.Fatal(e)
	}
	ev, e := ReadEvent(ctx, ch)
	if e != nil || ev.Type != "committed" || ev.Cursor != 1 {
		t.Fatal(ev, e)
	}
	close1()
	ch, close2 := connect()
	defer close2()
	// Reconnecting with the same command ID does not append a second record.
	_, e = SendRequest(ctx, ch, Request{Operation: "publish", ID: "operation-001", Payload: []byte("durable-data")})
	if e != nil {
		t.Fatal(e)
	}
	ev, e = ReadEvent(ctx, ch)
	if e != nil || ev.Cursor != 1 {
		t.Fatal(ev, e)
	}
	_, e = SendRequest(ctx, ch, Request{Operation: "replay", Cursor: 0})
	if e != nil {
		t.Fatal(e)
	}
	ev, e = ReadEvent(ctx, ch)
	if e != nil || ev.Type != "message" || string(ev.Payload) != "durable-data" {
		t.Fatal(ev, e)
	}
	ev, e = ReadEvent(ctx, ch)
	if e != nil || ev.Type != "replay_complete" {
		t.Fatal(ev, e)
	}
	_, e = SendRequest(ctx, ch, Request{Operation: "ack", Cursor: 1})
	if e != nil {
		t.Fatal(e)
	}
	ev, e = ReadEvent(ctx, ch)
	if e != nil || ev.Type != "acked" {
		t.Fatal(ev, e)
	}
}
