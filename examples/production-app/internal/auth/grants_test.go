package auth

import "testing"

func TestGrantIsOneShotAndSessionBound(t *testing.T) {
	store := NewGrantStore()
	token, err := store.Issue("user-1", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if store.Consume(token, "user-1", "session-b") {
		t.Fatal("grant accepted for another session")
	}
	if store.Consume(token, "user-1", "session-a") {
		t.Fatal("failed binding attempt did not burn grant")
	}
	token, err = store.Issue("user-1", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if !store.Consume(token, "user-1", "session-a") {
		t.Fatal("valid grant rejected")
	}
	if store.Consume(token, "user-1", "session-a") {
		t.Fatal("grant replay accepted")
	}
}
