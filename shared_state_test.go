package fh_test

import (
	"context"
	"errors"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

func TestAppSharedState(t *testing.T) {
	provider := kv.NewMemoryProvider()
	app := fh.New(fh.WithSharedState(provider))

	sessions, err := app.StateStore(context.Background(), "sessions/default")
	if err != nil {
		t.Fatal(err)
	}
	rateLimits := app.MustStateStore("ratelimit/public")
	if err := sessions.Set("client", []byte("session"), 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := rateLimits.Get("client"); err != nil || ok {
		t.Fatalf("state leaked between namespaces: ok=%v err=%v", ok, err)
	}
	app.OnShutdown(func() error {
		if _, ok, err := sessions.Get("client"); err != nil || !ok {
			t.Fatalf("shared state closed before shutdown hooks: ok=%v err=%v", ok, err)
		}
		return nil
	})

	if err := app.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Store(context.Background(), "new/namespace"); !errors.Is(err, kv.ErrProviderClosed) {
		t.Fatalf("provider after app shutdown = %v, want ErrProviderClosed", err)
	}
}

func TestAppSharedStateUnavailable(t *testing.T) {
	app := fh.New()
	if _, err := app.StateStore(context.Background(), "sessions/default"); !errors.Is(err, fh.ErrSharedStateUnavailable) {
		t.Fatalf("StateStore error = %v", err)
	}
}
