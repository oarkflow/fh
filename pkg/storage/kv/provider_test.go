package kv

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func testProviderContract(t *testing.T, provider Provider) {
	t.Helper()
	ctx := context.Background()

	sessions, err := provider.Store(ctx, "sessions/default")
	if err != nil {
		t.Fatal(err)
	}
	again, err := provider.Store(ctx, "sessions/default")
	if err != nil {
		t.Fatal(err)
	}
	if sessions != again {
		t.Fatal("same namespace returned different Store instances")
	}
	replay, err := provider.Store(ctx, "replay/webhooks")
	if err != nil {
		t.Fatal(err)
	}
	if err := sessions.Set("same-key", []byte("session"), 0); err != nil {
		t.Fatal(err)
	}
	if err := replay.Set("same-key", []byte("replay"), 0); err != nil {
		t.Fatal(err)
	}
	value, ok, err := sessions.Get("same-key")
	if err != nil || !ok || string(value) != "session" {
		t.Fatalf("session namespace = %q, %v, %v", value, ok, err)
	}
	value, ok, err = replay.Get("same-key")
	if err != nil || !ok || string(value) != "replay" {
		t.Fatalf("replay namespace = %q, %v, %v", value, ok, err)
	}
	if n, err := sessions.Len(); err != nil || n != 1 {
		t.Fatalf("session Len = %d, %v", n, err)
	}
	if n, err := replay.Len(); err != nil || n != 1 {
		t.Fatalf("replay Len = %d, %v", n, err)
	}
}

func TestMemoryProviderContract(t *testing.T) {
	provider := NewMemoryProvider()
	t.Cleanup(func() { _ = provider.Close() })
	testProviderContract(t, provider)
}

func TestMemoryProviderConcurrentOpen(t *testing.T) {
	provider := NewMemoryProvider()
	t.Cleanup(func() { _ = provider.Close() })

	const workers = 32
	stores := make([]Store, workers)
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			stores[i] = MustStore(provider, "ratelimit/public")
		}(i)
	}
	wg.Wait()
	for i := 1; i < len(stores); i++ {
		if stores[i] != stores[0] {
			t.Fatal("concurrent Store calls created multiple namespace stores")
		}
	}
}

func TestFileProviderContractAndPersistence(t *testing.T) {
	root := t.TempDir()
	provider, err := NewFileProvider(root)
	if err != nil {
		t.Fatal(err)
	}
	testProviderContract(t, provider)
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileProvider(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	store, err := reopened.Store(context.Background(), "sessions/default")
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := store.Get("same-key")
	if err != nil || !ok || string(value) != "session" {
		t.Fatalf("reopened value = %q, %v, %v", value, ok, err)
	}
}

func TestProviderValidationAndClose(t *testing.T) {
	provider := NewMemoryProvider()
	for _, namespace := range []string{"", " leading", "trailing ", "bad\nname"} {
		if _, err := provider.Store(context.Background(), namespace); !errors.Is(err, ErrInvalidNamespace) {
			t.Fatalf("Store(%q) error = %v, want ErrInvalidNamespace", namespace, err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Store(canceled, "sessions/default"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Store error = %v", err)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if _, err := provider.Store(context.Background(), "sessions/default"); !errors.Is(err, ErrProviderClosed) {
		t.Fatalf("Store after Close error = %v", err)
	}
}
