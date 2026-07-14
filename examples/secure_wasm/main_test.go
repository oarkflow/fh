package main

import (
	"encoding/base64"
	"testing"
)

func TestRegistrationGrantIsOneShotAndSessionBound(t *testing.T) {
	store := newGrantStore()
	token, err := store.issue("user-123", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if store.consume(token, "user-123", "session-b") {
		t.Fatal("grant accepted for a different web session")
	}
	if store.consume(token, "user-123", "session-a") {
		t.Fatal("failed binding attempt did not burn the one-time grant")
	}

	token, err = store.issue("user-123", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if !store.consume(token, "user-123", "session-a") {
		t.Fatal("valid registration grant was rejected")
	}
	if store.consume(token, "user-123", "session-a") {
		t.Fatal("registration grant was accepted more than once")
	}
}

func TestRegistrationGrantRejectsMalformedToken(t *testing.T) {
	store := newGrantStore()
	for _, token := range []string{"", "not-base64!", base64.RawURLEncoding.EncodeToString(make([]byte, 31))} {
		if store.consume(token, "user-123", "session-a") {
			t.Fatalf("malformed registration token %q was accepted", token)
		}
	}
}
