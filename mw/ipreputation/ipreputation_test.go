package ipreputation

import "testing"

func TestMemoryStoreReturnsIsolatedScores(t *testing.T) {
	store := NewMemoryStore(10)
	store.Set("192.0.2.1", &Score{Value: 50, Reasons: []string{"initial"}})
	got, ok := store.Get("192.0.2.1")
	if !ok {
		t.Fatal("score missing")
	}
	got.Value = 0
	got.Reasons[0] = "mutated"
	again, _ := store.Get("192.0.2.1")
	if again.Value != 50 || again.Reasons[0] != "initial" {
		t.Fatalf("caller mutated stored score: %#v", again)
	}
}

func TestDefaultSuspiciousHandlerDoesNotAdvanceChain(t *testing.T) {
	cfg := normalize(Config{})
	if err := cfg.OnSuspicious(nil, &Score{Value: 60}); err != nil {
		t.Fatal(err)
	}
}
