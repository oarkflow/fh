package mira

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestRebindAndWaitConfirmsNewPath(t *testing.T) {
	key := bytes.Repeat([]byte{21}, 32)
	server, err := Listen("127.0.0.1:0", key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	client, err := Dial(ctx, server.Addr().String(), key, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	peer, err := server.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close(context.Background())
	if err = client.RebindAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	stats, err := peer.Stats(ctx)
	if err != nil || stats.ValidatedMigrations == 0 {
		t.Fatalf("rebind not validated: %+v %v", stats, err)
	}
}
