package fact_test

import (
	"sync"
	"testing"

	"github.com/oarkflow/fh/ref/fact"
)

type UserFact struct {
	ID   string
	Role string
}

func TestFactKeys(t *testing.T) {
	key1 := fact.NewKey[UserFact]("user")
	key2 := fact.NewKey[string]("tenant")

	if key1.DefinitionID() == key2.DefinitionID() {
		t.Fatalf("expected distinct DefinitionIDs, got %d and %d", key1.DefinitionID(), key2.DefinitionID())
	}

	any1 := key1.Any()
	if any1.Name != "user" || any1.DefID != key1.DefinitionID() {
		t.Errorf("expected AnyKey to match typed Key")
	}
}

func TestFactStore(t *testing.T) {
	store := fact.NewStore(3)

	if store.SlotCount() != 3 {
		t.Fatalf("expected 3 slots, got %d", store.SlotCount())
	}

	if fact.Has(store, 0) {
		t.Errorf("slot 0 should not be populated")
	}

	u := UserFact{ID: "usr-42", Role: "admin"}
	fact.Put(store, 0, u)

	if !fact.Has(store, 0) {
		t.Errorf("slot 0 should be populated")
	}
	if store.Count() != 1 {
		t.Errorf("expected count 1, got %d", store.Count())
	}

	gotU, ok := fact.Get[UserFact](store, 0)
	if !ok || gotU.ID != "usr-42" || gotU.Role != "admin" {
		t.Errorf("expected to retrieve UserFact, got %+v", gotU)
	}

	// Type mismatch
	_, ok = fact.Get[string](store, 0)
	if ok {
		t.Errorf("expected type mismatch for string at slot 0")
	}

	// Overwrite slot 0
	u2 := UserFact{ID: "usr-99", Role: "guest"}
	fact.Put(store, 0, u2)
	gotU2, ok := fact.Get[UserFact](store, 0)
	if !ok || gotU2.ID != "usr-99" {
		t.Errorf("expected updated UserFact")
	}
	// Count should still be 1
	if store.Count() != 1 {
		t.Errorf("expected count 1 after overwrite, got %d", store.Count())
	}

	// Out of bounds
	if fact.Has(store, 99) {
		t.Errorf("out of bounds slot should return false")
	}
	fact.Put(store, 99, "invalid")
	_, ok = fact.Get[string](store, 99)
	if ok {
		t.Errorf("out of bounds slot get should return false")
	}
}

func TestFactStoreConcurrency(t *testing.T) {
	store := fact.NewStore(100)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(slot fact.PlanSlot) {
			defer wg.Done()
			fact.Put(store, slot, int(slot)*10)
		}(fact.PlanSlot(i))
	}
	wg.Wait()

	if store.Count() != 100 {
		t.Fatalf("expected count 100, got %d", store.Count())
	}

	for i := 0; i < 100; i++ {
		val, ok := fact.Get[int](store, fact.PlanSlot(i))
		if !ok || val != i*10 {
			t.Errorf("slot %d: expected %d, got %d", i, i*10, val)
		}
	}
}
